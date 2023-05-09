use async_channel::{Receiver, Sender};
use bincode::{
    config::{
        FixintEncoding, RejectTrailing, WithOtherIntEncoding, WithOtherTrailing,
    },
    DefaultOptions, Options,
};
use caches::Cache;
use dbeel::{
    args::{get_args, Args},
    cached_file_reader::FileId,
    error::{Error, Result},
    lsm_tree::{LSMTree, TOMBSTONE},
    page_cache::{PageCache, PartitionPageCache, PAGE_SIZE},
    read_exactly::read_exactly,
};
use futures_lite::{AsyncRead, AsyncWrite, AsyncWriteExt, Future};
use glommio::{
    enclose,
    net::{TcpListener, TcpStream},
    spawn_local,
    timer::sleep,
    CpuSet, LocalExecutorBuilder, Placement,
};
use murmur3::murmur3_32;
use pretty_env_logger::formatted_timed_builder;
use rmpv::encode::write_value;
use rmpv::{decode::read_value_ref, encode::write_value_ref, Value, ValueRef};
use serde::{Deserialize, Serialize};
use std::{any::Any, cell::Cell, path::PathBuf};
use std::{cell::RefCell, time::Duration};
use std::{collections::HashMap, rc::Rc};

extern crate pretty_env_logger;
#[macro_use]
extern crate log;

#[cfg(debug_assertions)]
const DEFAULT_LOG_LEVEL: &str = "dbeel=trace";
#[cfg(not(debug_assertions))]
const DEFAULT_LOG_LEVEL: &str = "dbeel=info";

#[derive(Clone, Serialize, Deserialize)]
enum ShardEvent {
    CreateCollection(String),
    DropCollection(String),
}

#[derive(Clone, Serialize, Deserialize)]
enum ShardRequest {
    GetShards,
}

#[derive(Clone, Serialize, Deserialize)]
enum ShardResponse {
    GetShards(Vec<(String, String)>),
}

#[derive(Clone, Serialize, Deserialize)]
enum ShardMessage {
    Event(ShardEvent),
    Request((usize, ShardRequest)),
    Response((usize, ShardResponse)),
}

// A packet that is sent between shards, holds a cpu id and a message.
type ShardPacket = (usize, ShardMessage);

#[derive(Debug, Clone)]
struct LocalShardConnection {
    cpu: usize,
    sender: Sender<ShardPacket>,
    receiver: Receiver<ShardPacket>,
}

impl LocalShardConnection {
    fn new(cpu: usize) -> Self {
        let (sender, receiver) = async_channel::unbounded();
        Self {
            cpu,
            sender,
            receiver,
        }
    }
}

#[derive(Debug)]
struct RemoteShardConnection {
    // The address to use to connect to the remote shard.
    address: String,
}

impl RemoteShardConnection {
    fn new(address: String) -> Self {
        Self { address }
    }
}

#[derive(Debug)]
enum ShardConnection {
    Local(LocalShardConnection),
    Remote(RemoteShardConnection),
}

// A struct to hold other shards to communicate with.
#[derive(Debug)]
struct OtherShard {
    // The unique shard name.
    name: String,

    // The hash of the unique name, used for consistent hashing.
    hash: u32,

    // Communicate with a shard by abstracting away if it's remote or local.
    connection: ShardConnection,
}

fn hash_string(s: &String) -> std::io::Result<u32> {
    murmur3_32(&mut std::io::Cursor::new(s), 0)
}

impl OtherShard {
    fn new(name: String, connection: ShardConnection) -> Self {
        let hash = hash_string(&name).unwrap();
        Self {
            name,
            hash,
            connection,
        }
    }
}

// Holder of state to the shard running on the current thread.
// To share the state between all coroutines, async methods get Rc<Self>.
struct MyShard {
    // Input config from the user.
    args: Args,

    // Current shard's cpu id.
    id: usize,

    // The consistent hash ring (shards sorted by hash).
    shards: RefCell<Vec<OtherShard>>,

    // Collections to the lsm tree on disk.
    trees: RefCell<HashMap<String, Rc<LSMTree>>>,

    // The shard's page cache.
    cache: Rc<RefCell<PageCache<FileId>>>,

    // Requests currently waiting for responses.
    waiting_requests: RefCell<HashMap<usize, Sender<ShardResponse>>>,

    // The request id available for sending a request.
    available_request_id: Cell<usize>,
}

impl MyShard {
    fn new(
        args: Args,
        id: usize,
        shards: Vec<OtherShard>,
        cache: PageCache<FileId>,
    ) -> Self {
        Self {
            args,
            id,
            shards: RefCell::new(shards),
            trees: RefCell::new(HashMap::new()),
            cache: Rc::new(RefCell::new(cache)),
            waiting_requests: RefCell::new(HashMap::new()),
            available_request_id: Cell::new(0),
        }
    }

    fn get_request_id(&self) -> usize {
        let id = self.available_request_id.get();
        if id == std::usize::MAX {
            self.available_request_id.set(0);
        } else {
            self.available_request_id.set(id + 1);
        }
        id
    }

    fn get_collection(&self, name: &String) -> Result<Rc<LSMTree>> {
        self.trees
            .borrow()
            .get(name)
            .ok_or(Error::CollectionNotFound(name.to_string()))
            .map(|t| t.clone())
    }

    async fn create_collection(self: Rc<Self>, name: String) -> Result<()> {
        if self.trees.borrow().contains_key(&name) {
            return Err(Error::CollectionAlreadyExists(name));
        }

        let mut dir = PathBuf::from(self.args.dir.clone());
        dir.push(format!("{}-{}", name, self.id));
        let cache = self.cache.clone();
        let tree = Rc::new(
            LSMTree::open_or_create(
                dir,
                PartitionPageCache::new(name.clone(), cache),
            )
            .await?,
        );
        self.trees.borrow_mut().insert(name, tree);
        Ok(())
    }

    fn drop_collection(self: Rc<Self>, name: String) -> Result<()> {
        {
            let tree = self
                .trees
                .borrow_mut()
                .remove(&name)
                .ok_or(Error::CollectionNotFound(name))?;
            tree.purge()?;
        }

        // TODO: add set items to cache, then there will be no reason to ever delete
        // items from the cache.
        self.cache.borrow_mut().purge();

        Ok(())
    }

    async fn broadcast_message_to_remote_shards(
        self: Rc<Self>,
        message: &ShardMessage,
    ) -> Result<()> {
        let addresses = self
            .shards
            .borrow()
            .iter()
            .flat_map(|p| match &p.connection {
                ShardConnection::Remote(c) => Some(c.address.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();

        let timeout =
            Duration::from_millis(self.args.remote_shard_connect_timeout);

        for address in addresses {
            let mut stream =
                TcpStream::connect_timeout(&address, timeout).await?;
            if let Err(e) = send_message_to_stream(&mut stream, &message).await
            {
                error!(
                    "Error sending message to remote shard ({}): {}",
                    address, e
                );
            }
        }

        Ok(())
    }

    async fn broadcast_message_to_local_shards(
        self: Rc<Self>,
        message: &ShardMessage,
    ) -> Result<()> {
        let senders = self
            .shards
            .borrow()
            .iter()
            .flat_map(|p| match &p.connection {
                ShardConnection::Local(c) => Some(c.sender.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();

        // TODO: remove unwrap.
        for sender in senders {
            sender.send((self.id, message.clone())).await.unwrap();
        }

        Ok(())
    }

    async fn broadcast_message(
        self: Rc<Self>,
        message: &ShardMessage,
    ) -> Result<()> {
        self.clone()
            .broadcast_message_to_local_shards(message)
            .await?;
        self.broadcast_message_to_remote_shards(message).await?;
        Ok(())
    }
}

fn remote_shard_bincode_options() -> WithOtherIntEncoding<
    WithOtherTrailing<DefaultOptions, RejectTrailing>,
    FixintEncoding,
> {
    return DefaultOptions::new()
        .reject_trailing_bytes()
        .with_fixint_encoding();
}

async fn handle_shard_event(
    my_shard: Rc<MyShard>,
    event: ShardEvent,
) -> Result<()> {
    // All events should be idempotent.
    match event {
        ShardEvent::CreateCollection(name) => {
            if let Err(e) = my_shard.create_collection(name).await {
                if e.type_id() != Error::CollectionAlreadyExists.type_id() {
                    return Err(e);
                }
            }
        }
        ShardEvent::DropCollection(name) => {
            if let Err(e) = my_shard.drop_collection(name) {
                if e.type_id() != Error::CollectionNotFound.type_id() {
                    return Err(e);
                }
            }
        }
    };

    Ok(())
}

async fn handle_shard_request(
    my_shard: Rc<MyShard>,
    request: ShardRequest,
) -> Result<ShardResponse> {
    let response = match request {
        ShardRequest::GetShards => ShardResponse::GetShards(
            my_shard
                .shards
                .borrow()
                .iter()
                .flat_map(|shard| match &shard.connection {
                    ShardConnection::Remote(c) => {
                        Some((shard.name.clone(), c.address.clone()))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ),
    };

    Ok(response)
}

async fn get_message_from_stream(
    stream: &mut (impl AsyncRead + std::marker::Unpin),
) -> Result<ShardMessage> {
    let size_buf = read_exactly(stream, 2).await?;
    let size = u16::from_le_bytes(size_buf.as_slice().try_into().unwrap());
    let request_buf = read_exactly(stream, size.into()).await?;

    let mut cursor = std::io::Cursor::new(&request_buf[..]);

    Ok(remote_shard_bincode_options()
        .deserialize_from::<_, ShardMessage>(&mut cursor)?)
}

async fn handle_remote_shard_client(
    my_shard: Rc<MyShard>,
    mut client: TcpStream,
) -> Result<()> {
    let msg = get_message_from_stream(&mut client).await?;
    if let Some(response_msg) = handle_shard_message(my_shard, msg).await? {
        send_message_to_stream(&mut client, &response_msg).await?;
    }
    client.close().await?;

    Ok(())
}

async fn run_remote_shard_messages_server(
    my_shard: Rc<MyShard>,
    server: TcpListener,
) {
    loop {
        match server.accept().await {
            Ok(client) => {
                spawn_local(enclose!((my_shard.clone() => my_shard) async move {
                    let result =
                        handle_remote_shard_client(my_shard, client).await;
                    if let Err(e) = result {
                        error!("Failed to handle distributed client: {}", e);
                    }
                }))
                .detach();
            }
            Err(e) => {
                error!("Failed to accept client: {}", e);
            }
        }
    }
}

async fn handle_shard_message(
    my_shard: Rc<MyShard>,
    message: ShardMessage,
) -> Result<Option<ShardMessage>> {
    Ok(match message {
        ShardMessage::Event(event) => {
            handle_shard_event(my_shard, event).await?;
            None
        }
        ShardMessage::Request((id, request)) => {
            let response = handle_shard_request(my_shard, request).await?;
            Some(ShardMessage::Response((id, response)))
        }
        ShardMessage::Response((id, response)) => {
            let sender = my_shard
                .waiting_requests
                .borrow_mut()
                .remove(&id)
                .ok_or(Error::RequestIdNotFoundInWaitingList)?;
            // TODO: remove unwrap.
            sender.send(response).await.unwrap();
            None
        }
    })
}

async fn run_shard_messages_receiver(
    my_shard: Rc<MyShard>,
    sender: Sender<ShardPacket>,
    receiver: Receiver<ShardPacket>,
) -> Result<()> {
    let shard_id = my_shard.id;

    loop {
        let (id, msg) = receiver.recv().await?;
        if id == shard_id {
            continue;
        }

        match handle_shard_message(my_shard.clone(), msg).await {
            Ok(response) => {
                if let Some(response_msg) = response {
                    if let Err(e) = sender.send((shard_id, response_msg)).await
                    {
                        error!(
                            "Failed to reply to local shard ({}): {}",
                            shard_id, e
                        );
                    }
                }
            }
            Err(e) => error!("Failed to handle shard message: {}", e),
        }
    }
}

async fn run_compaction_loop(my_shard: Rc<MyShard>) {
    let compaction_factor = my_shard.args.compaction_factor;

    loop {
        let trees = my_shard
            .trees
            .borrow()
            .values()
            .map(|t| t.clone())
            .collect::<Vec<_>>();

        for tree in trees {
            'current_tree_compaction: loop {
                let (even, mut odd): (Vec<usize>, Vec<usize>) =
                    tree.sstable_indices().iter().partition(|i| *i % 2 == 0);

                if even.len() >= compaction_factor {
                    let new_index = even[even.len() - 1] + 1;
                    if let Err(e) =
                        tree.compact(even, new_index, odd.is_empty()).await
                    {
                        error!("Failed to compact files: {}", e);
                    }
                    continue 'current_tree_compaction;
                }

                if odd.len() >= compaction_factor && even.len() >= 1 {
                    debug_assert!(even[0] > odd[odd.len() - 1]);

                    odd.push(even[0]);

                    let new_index = even[0] + 1;
                    if let Err(e) = tree.compact(odd, new_index, true).await {
                        error!("Failed to compact files: {}", e);
                    }
                    continue 'current_tree_compaction;
                }

                break 'current_tree_compaction;
            }
        }

        sleep(Duration::from_millis(1)).await;
    }
}

fn extract_field<'a>(map: &'a Value, field_name: &str) -> Result<&'a Value> {
    let field = &map[field_name];

    if field.is_nil() {
        return Err(Error::MissingField(field_name.to_string()));
    }

    Ok(field)
}

fn extract_field_as_str(map: &Value, field_name: &str) -> Result<String> {
    Ok(extract_field(map, field_name)?
        .as_str()
        .ok_or(Error::MissingField(field_name.to_string()))?
        .to_string())
}

fn extract_field_encoded(map: &Value, field_name: &str) -> Result<Vec<u8>> {
    let field = extract_field(map, field_name)?;

    let mut field_encoded: Vec<u8> = Vec::new();
    write_value(&mut field_encoded, field)?;

    Ok(field_encoded)
}

async fn send_message_to_stream(
    stream: &mut (impl AsyncWrite + std::marker::Unpin),
    message: &ShardMessage,
) -> Result<()> {
    let msg_buf = remote_shard_bincode_options().serialize(message)?;
    let size_buf = (msg_buf.len() as u16).to_le_bytes();

    stream.write_all(&size_buf).await?;
    stream.write_all(&msg_buf).await?;

    Ok(())
}

async fn request_from_stream(
    stream: &mut (impl AsyncRead + AsyncWrite + std::marker::Unpin),
    request: ShardRequest,
    request_id: usize,
) -> Result<ShardResponse> {
    send_message_to_stream(
        stream,
        &ShardMessage::Request((request_id, request)),
    )
    .await?;
    let msg = get_message_from_stream(stream).await?;
    match msg {
        ShardMessage::Response((response_id, response)) => {
            assert_eq!(request_id, response_id);
            Ok(response)
        }
        _ => Err(Error::ResponseWrongType),
    }
}

fn broadcast_message_in_background(
    my_shard: Rc<MyShard>,
    message: ShardMessage,
) {
    spawn_local(async move {
        if let Err(e) = my_shard.broadcast_message(&message).await {
            error!("Failed to broadcast message: {}", e);
        }
    })
    .detach();
}

async fn with_write<F>(tree: Rc<LSMTree>, write_fn: F) -> Result<()>
where
    F: Future<Output = Result<Option<Vec<u8>>>> + 'static,
{
    // Wait for flush.
    while tree.memtable_full() {
        futures_lite::future::yield_now().await;
    }

    write_fn.await?;

    if tree.memtable_full() {
        // Capacity is full, flush memtable to disk.
        spawn_local(async move {
            if let Err(e) = tree.flush().await {
                error!("Failed to flush memtable: {}", e);
            }
        })
        .detach();
    }

    Ok(())
}

async fn handle_request(
    my_shard: Rc<MyShard>,
    buffer: Vec<u8>,
) -> Result<Option<Vec<u8>>> {
    let msgpack_request = read_value_ref(&mut &buffer[..])?.to_owned();
    if let Some(map_vec) = msgpack_request.as_map() {
        let map = Value::Map(map_vec.to_vec());
        match map["type"].as_str() {
            Some("create_collection") => {
                let name = extract_field_as_str(&map, "name")?;

                if my_shard.trees.borrow().contains_key(&name) {
                    return Err(Error::CollectionAlreadyExists(name));
                }

                my_shard.clone().create_collection(name.clone()).await?;

                broadcast_message_in_background(
                    my_shard,
                    ShardMessage::Event(ShardEvent::CreateCollection(name)),
                );
            }
            Some("drop_collection") => {
                let name = extract_field_as_str(&map, "name")?;
                my_shard.clone().drop_collection(name.clone())?;

                broadcast_message_in_background(
                    my_shard,
                    ShardMessage::Event(ShardEvent::DropCollection(name)),
                );
            }
            Some("set") => {
                let collection = extract_field_as_str(&map, "collection")?;
                let key = extract_field_encoded(&map, "key")?;
                let value = extract_field_encoded(&map, "value")?;

                let tree = my_shard.get_collection(&collection)?;

                with_write(
                    tree.clone(),
                    async move { tree.set(key, value).await },
                )
                .await?;
            }
            Some("delete") => {
                let collection = extract_field_as_str(&map, "collection")?;
                let key = extract_field_encoded(&map, "key")?;

                let tree = my_shard.get_collection(&collection)?;

                with_write(tree.clone(), async move { tree.delete(key).await })
                    .await?;
            }
            Some("get") => {
                let collection = extract_field_as_str(&map, "collection")?;
                let key = extract_field_encoded(&map, "key")?;

                let tree = my_shard.get_collection(&collection)?;

                return match tree.get(&key).await? {
                    Some(value) if value != TOMBSTONE => Ok(Some(value)),
                    _ => Err(Error::KeyNotFound),
                };
            }
            Some(name) => {
                return Err(Error::UnsupportedField(name.to_string()));
            }
            _ => {
                return Err(Error::BadFieldType("type".to_string()));
            }
        }
    } else {
        return Err(Error::BadFieldType("document".to_string()));
    }

    Ok(None)
}

async fn handle_client(
    my_shard: Rc<MyShard>,
    client: &mut (impl AsyncRead + AsyncWrite + std::marker::Unpin),
) -> Result<()> {
    let size_buf = read_exactly(client, 2).await?;
    let size = u16::from_le_bytes(size_buf.as_slice().try_into().unwrap());
    let request_buf = read_exactly(client, size.into()).await?;

    match handle_request(my_shard, request_buf).await {
        Ok(None) => {
            let mut buf: Vec<u8> = Vec::new();
            write_value_ref(&mut buf, &ValueRef::String("OK".into()))?;
            client.write_all(&buf).await?;
        }
        Ok(Some(buf)) => {
            client.write_all(&buf).await?;
        }
        Err(e) => {
            let error_string = format!("Error while handling request: {}", e);
            error!("{}", error_string);

            let mut buf: Vec<u8> = Vec::new();
            write_value_ref(
                &mut buf,
                &ValueRef::String(error_string.as_str().into()),
            )?;
            client.write_all(&buf).await?;
        }
    }

    client.close().await?;

    Ok(())
}

async fn get_remote_shards(
    seed_nodes: &[String],
    timeout: Duration,
) -> Result<Option<Vec<(String, String)>>> {
    for (i, address) in seed_nodes.into_iter().enumerate() {
        let mut stream = TcpStream::connect_timeout(address, timeout).await?;
        let response =
            request_from_stream(&mut stream, ShardRequest::GetShards, i)
                .await?;
        match response {
            ShardResponse::GetShards(shards) => return Ok(Some(shards)),
        }
    }

    Ok(None)
}

async fn run_shard(
    args: Args,
    id: usize,
    local_connections: Vec<LocalShardConnection>,
) -> Result<()> {
    info!("Starting shard of id: {}", id);

    let (sender, receiver) = local_connections
        .iter()
        .filter(|c| c.cpu == id)
        .map(|c| (c.sender.clone(), c.receiver.clone()))
        .next()
        .unwrap();

    let shard_name = format!("{}-{}", args.name, id);

    let port = args.port + id as u16;
    let mut shards = local_connections
        .into_iter()
        .map(|c| OtherShard::new(shard_name.clone(), ShardConnection::Local(c)))
        .collect::<Vec<_>>();

    let cache_len = args.page_cache_size / PAGE_SIZE / shards.len();
    let cache = PageCache::new(cache_len, cache_len / 16)
        .map_err(|e| Error::CacheCreationError(e.to_string()))?;

    let address = format!("{}:{}", args.ip, port);
    let server = TcpListener::bind(address.as_str())?;
    trace!("Listening for clients on: {}", address);

    if !args.seed_nodes.is_empty() {
        if let Some(remote_shards) = get_remote_shards(
            &args.seed_nodes,
            Duration::from_millis(args.remote_shard_connect_timeout),
        )
        .await?
        {
            shards.extend(
                remote_shards
                    .into_iter()
                    .filter(|(name, _)| name != &shard_name)
                    .map(|(name, address)| {
                        trace!(
                            "Discovered remote shard: ({}, {})",
                            name,
                            address
                        );
                        OtherShard::new(
                            name,
                            ShardConnection::Remote(
                                RemoteShardConnection::new(address),
                            ),
                        )
                    }),
            );
        } else {
            warn!("No remote shards in seed nodes");
        }
    }

    // Sort by hash to make it a consistant hashing ring.
    shards.sort_unstable_by_key(|x| x.hash);

    let my_shard = Rc::new(MyShard::new(args, id, shards, cache));

    spawn_local(enclose!((my_shard.clone() => my_shard) async move {
        if let Err(e) = run_shard_messages_receiver(my_shard, sender, receiver).await {
            error!("Error running shard messages receiver: {}", e);
        }
    }))
    .detach();

    spawn_local(enclose!((my_shard.clone() => my_shard) async move {
        run_compaction_loop(my_shard).await;
    }))
    .detach();

    spawn_local(enclose!((my_shard.clone() => my_shard) async move {
        let address = format!(
            "{}:{}",
            my_shard.args.ip,
            my_shard.args.remote_shard_port + my_shard.id as u16
        );
        let server = match TcpListener::bind(address.as_str()) {
            Ok(server) => server,
            Err(e) => {
                error!("Error starting remote messages server: {}", e);
                return;
            }
        };
        trace!("Listening for distributed messages on: {}", address);

        run_remote_shard_messages_server(my_shard, server).await;
    }))
    .detach();

    loop {
        match server.accept().await {
            Ok(mut client) => {
                spawn_local(enclose!((my_shard.clone() => my_shard) async move {
                    if let Err(e) = handle_client(my_shard, &mut client).await {
                        error!("Failed to handle client: {}", e);
                    }
                }))
                .detach();
            }
            Err(e) => {
                error!("Failed to accept client: {}", e);
            }
        }
    }
}

fn main() -> Result<()> {
    let mut log_builder = formatted_timed_builder();
    log_builder.parse_filters(
        &std::env::var("RUST_LOG")
            .or::<String>(Ok(DEFAULT_LOG_LEVEL.to_string()))
            .unwrap(),
    );
    log_builder.try_init().unwrap();

    let args = get_args();

    let cpu_set = CpuSet::online()?;
    assert!(!cpu_set.is_empty());

    let local_connections = cpu_set
        .iter()
        .map(|x| x.cpu)
        .map(|cpu| LocalShardConnection::new(cpu))
        .collect::<Vec<_>>();

    let handles = cpu_set
        .into_iter()
        .map(|x| x.cpu)
        .map(|cpu| {
            LocalExecutorBuilder::new(Placement::Fixed(cpu))
                .name(format!("executor({})", cpu).as_str())
                .spawn(enclose!(
                (local_connections.clone() => connections,
                 args.clone() => args)
                move || async move {
                    run_shard(args, cpu, connections).await
                }))
                .map_err(|e| Error::GlommioError(e))
        })
        .collect::<Result<Vec<_>>>()?;

    handles
        .into_iter()
        .map(|h| h.join().map_err(|e| Error::GlommioError(e)))
        .collect::<Result<Vec<_>>>()?;

    Ok(())
}
