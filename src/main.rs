use async_channel::{Receiver, Sender};
use bincode::{
    config::{
        FixintEncoding, RejectTrailing, WithOtherIntEncoding, WithOtherTrailing,
    },
    DefaultOptions, Options,
};
use caches::Cache;
use clap::Parser;
use dbeel::{
    cached_file_reader::FileId,
    error::{Error, Result},
    lsm_tree::{LSMTree, TOMBSTONE},
    page_cache::{PageCache, PartitionPageCache, PAGE_SIZE},
};
use futures_lite::{AsyncReadExt, AsyncWriteExt, Future};
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
use std::{any::Any, env::temp_dir};
use std::{cell::RefCell, time::Duration};
use std::{collections::HashMap, rc::Rc};

extern crate pretty_env_logger;
#[macro_use]
extern crate log;

#[cfg(debug_assertions)]
const DEFAULT_LOG_LEVEL: &str = "dbeel=trace";
#[cfg(not(debug_assertions))]
const DEFAULT_LOG_LEVEL: &str = "dbeel=info";

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
/// A stupid database, by Tony Solomonik.
struct Args {
    #[clap(
        short,
        long,
        help = "Unique node name, used to differentiate between nodes for \
                distribution of load.",
        default_value = "dbeel"
    )]
    name: String,

    #[clap(
        short,
        long,
        help = "Listen hostname / ip.",
        default_value = "127.0.0.1"
    )]
    ip: String,

    #[clap(
        short,
        long,
        help = "Server port base.
                Each shard has a different port calculated by <port_base> + \
                <cpu_id>.
                This port is for listening on client requests.",
        default_value = "10000"
    )]
    port: u16,

    #[clap(
        long,
        help = "Remote shard port base.
                This port is for listening for distributed messages from \
                remote shards.",
        default_value = "20000"
    )]
    remote_shard_port: u16,

    #[clap(
        long,
        help = "Remote shard connect timeout in milliseconds.",
        default_value = "5000"
    )]
    remote_shard_connect_timeout: u64,

    #[clap(
        short,
        long,
        help = "How much files to compact each time.",
        default_value = "2"
    )]
    compaction_factor: usize,

    #[clap(
        long,
        help = "Page cache size in bytes.",
        default_value = "1073741824"
    )]
    page_cache_size: usize,
}

#[derive(Clone, Serialize, Deserialize)]
enum ShardMessage {
    CreateCollection(String),
    DropCollection(String),
}

fn bincode_options() -> WithOtherIntEncoding<
    WithOtherTrailing<DefaultOptions, RejectTrailing>,
    FixintEncoding,
> {
    return DefaultOptions::new()
        .reject_trailing_bytes()
        .with_fixint_encoding();
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

#[derive(Debug)]
struct Shard {
    // The hash of the unique address, used for consistent hashing.
    hash: u32,

    // Communicate with a shard by abstracting away if it's remote or local.
    connection: ShardConnection,
}

fn hash_string(s: &String) -> std::io::Result<u32> {
    murmur3_32(&mut std::io::Cursor::new(s), 0)
}

impl Shard {
    fn new(hash: u32, connection: ShardConnection) -> Self {
        Self { hash, connection }
    }
}

// State shared between all coroutines that run on the same shard.
struct PerShardState {
    // Input config from the user.
    args: Args,

    // Current shard's cpu id.
    id: usize,

    // The consistent hash ring (shards sorted by hash).
    shards: Vec<Shard>,

    // Collections to the lsm tree on disk.
    trees: HashMap<String, Rc<LSMTree>>,

    // The shard's page cache.
    cache: Rc<RefCell<PageCache<FileId>>>,
}

impl PerShardState {
    fn new(
        args: Args,
        id: usize,
        shards: Vec<Shard>,
        cache: PageCache<FileId>,
    ) -> Self {
        Self {
            args,
            id,
            shards,
            trees: HashMap::new(),
            cache: Rc::new(RefCell::new(cache)),
        }
    }
}

type SharedPerShardState = Rc<RefCell<PerShardState>>;

async fn handle_shard_message(
    state: SharedPerShardState,
    msg: ShardMessage,
) -> Result<()> {
    // All messages should be idempotent.
    match msg {
        ShardMessage::CreateCollection(name) => {
            if let Err(e) = create_collection_for_shard(state, name).await {
                if e.type_id() != Error::CollectionAlreadyExists.type_id() {
                    return Err(e);
                }
            }
        }
        ShardMessage::DropCollection(name) => {
            if let Err(e) = drop_collection_for_shard(state, name) {
                if e.type_id() != Error::CollectionNotFound.type_id() {
                    return Err(e);
                }
            }
        }
    };

    Ok(())
}

async fn handle_distributed_client(
    state: SharedPerShardState,
    mut client: TcpStream,
) -> Result<()> {
    let size_buf = read_exactly(&mut client, 2).await?;
    let size = u16::from_le_bytes(size_buf.as_slice().try_into().unwrap());
    let request_buf = read_exactly(&mut client, size.into()).await?;

    let mut cursor = std::io::Cursor::new(&request_buf[..]);
    let msg =
        bincode_options().deserialize_from::<_, ShardMessage>(&mut cursor)?;

    handle_shard_message(state.clone(), msg).await
}

async fn run_distributed_messages_server(
    state: SharedPerShardState,
    server: TcpListener,
) -> Result<()> {
    loop {
        match server.accept().await {
            Ok(client) => {
                spawn_local(enclose!((state.clone() => state) async move {
                    let result = handle_distributed_client(state, client).await;
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

async fn run_shard_messages_receiver(
    state: SharedPerShardState,
    receiver: Receiver<ShardPacket>,
) -> Result<()> {
    loop {
        let (id, msg) = receiver.recv().await?;
        if id == state.borrow().id {
            continue;
        }

        handle_shard_message(state.clone(), msg).await?;
    }
}

async fn run_compaction_loop(state: SharedPerShardState) {
    let compaction_factor = state.borrow().args.compaction_factor;

    loop {
        let trees: Vec<Rc<LSMTree>> =
            state.borrow().trees.values().map(|t| t.clone()).collect();
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

async fn read_exactly(
    stream: &mut TcpStream,
    n: usize,
) -> std::io::Result<Vec<u8>> {
    let mut buf = vec![0; n];
    let mut bytes_read = 0;

    while bytes_read < n {
        match stream.read(&mut buf[bytes_read..]).await {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "unexpected end of file",
                ))
            }
            Ok(n) => bytes_read += n,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }

    Ok(buf)
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

fn get_collection(state: &PerShardState, name: &String) -> Result<Rc<LSMTree>> {
    state
        .trees
        .get(name)
        .ok_or(Error::CollectionNotFound(name.to_string()))
        .map(|t| t.clone())
}

async fn create_collection_for_shard(
    state: SharedPerShardState,
    name: String,
) -> Result<()> {
    if state.borrow().trees.contains_key(&name) {
        return Err(Error::CollectionAlreadyExists(name));
    }

    let mut dir = temp_dir();
    dir.push(format!("{}-{}", name, state.borrow().id));
    let cache = state.borrow().cache.clone();
    let tree = Rc::new(
        LSMTree::open_or_create(
            dir,
            PartitionPageCache::new(name.clone(), cache),
        )
        .await?,
    );
    state.borrow_mut().trees.insert(name, tree);
    Ok(())
}

fn drop_collection_for_shard(
    state: SharedPerShardState,
    name: String,
) -> Result<()> {
    {
        let tree = state
            .borrow_mut()
            .trees
            .remove(&name)
            .ok_or(Error::CollectionNotFound(name))?;
        tree.purge()?;
    }

    // TODO: add set items to cache, then there will be no reason to ever delete
    // items from the cache.
    state.borrow_mut().cache.borrow_mut().purge();

    Ok(())
}

async fn broadcast_message_to_remote_shards(
    state: SharedPerShardState,
    message: &ShardMessage,
) -> Result<()> {
    let addresses = state
        .borrow()
        .shards
        .iter()
        .flat_map(|p| match &p.connection {
            ShardConnection::Remote(c) => Some(c.address.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();

    let msg_buf = bincode_options().serialize(message)?;
    let size_buf = (msg_buf.len() as u16).to_le_bytes();
    let timeout =
        Duration::from_millis(state.borrow().args.remote_shard_connect_timeout);

    for address in addresses {
        let mut stream =
            match TcpStream::connect_timeout(&address, timeout).await {
                Ok(s) => s,
                Err(e) => {
                    error!(
                        "Error connecting to distributed client ({}): {}",
                        address, e
                    );
                    continue;
                }
            };

        if let Err(e) = stream.write_all(&size_buf).await {
            eprintln!("Failed to send size to ({}): {}", address, e);
            continue;
        }

        if let Err(e) = stream.write_all(&msg_buf).await {
            eprintln!("Failed to send data to ({}): {}", address, e);
            continue;
        }
    }

    Ok(())
}

async fn broadcast_message_to_local_shards(
    state: SharedPerShardState,
    message: ShardMessage,
) -> Result<()> {
    let senders = state
        .borrow()
        .shards
        .iter()
        .flat_map(|p| match &p.connection {
            ShardConnection::Local(c) => Some(c.sender.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();

    let id = state.borrow().id;

    // TODO: remove unwrap.
    for sender in senders {
        sender.send((id, message.clone())).await.unwrap();
    }

    Ok(())
}

async fn broadcast_message(
    state: SharedPerShardState,
    message: ShardMessage,
) -> Result<()> {
    broadcast_message_to_local_shards(state.clone(), message.clone()).await?;
    broadcast_message_to_remote_shards(state, &message).await?;
    Ok(())
}

fn broadcast_message_in_background(
    state: SharedPerShardState,
    message: ShardMessage,
) {
    spawn_local(enclose!((state.clone() => state) async move {
        if let Err(e) = broadcast_message(state, message).await {
            error!("Failed to broadcast message: {}", e);
        }
    }))
    .detach();
}

async fn handle_request(
    state: SharedPerShardState,
    buffer: Vec<u8>,
) -> Result<Option<Vec<u8>>> {
    let msgpack_request = read_value_ref(&mut &buffer[..])?.to_owned();
    if let Some(map_vec) = msgpack_request.as_map() {
        let map = Value::Map(map_vec.to_vec());
        match map["type"].as_str() {
            Some("create_collection") => {
                let name = extract_field_as_str(&map, "name")?;

                if state.borrow().trees.contains_key(&name) {
                    return Err(Error::CollectionAlreadyExists(name));
                }

                create_collection_for_shard(state.clone(), name.clone())
                    .await?;

                broadcast_message_in_background(
                    state,
                    ShardMessage::CreateCollection(name),
                );
            }
            Some("drop_collection") => {
                let name = extract_field_as_str(&map, "name")?;
                drop_collection_for_shard(state.clone(), name.clone())?;

                broadcast_message_in_background(
                    state,
                    ShardMessage::DropCollection(name),
                );
            }
            Some("set") => {
                let collection = extract_field_as_str(&map, "collection")?;
                let key = extract_field_encoded(&map, "key")?;
                let value = extract_field_encoded(&map, "value")?;

                let tree = get_collection(&state.borrow(), &collection)?;

                with_write(
                    tree.clone(),
                    async move { tree.set(key, value).await },
                )
                .await?;
            }
            Some("delete") => {
                let collection = extract_field_as_str(&map, "collection")?;
                let key = extract_field_encoded(&map, "key")?;

                let tree = get_collection(&state.borrow(), &collection)?;

                with_write(tree.clone(), async move { tree.delete(key).await })
                    .await?;
            }
            Some("get") => {
                let collection = extract_field_as_str(&map, "collection")?;
                let key = extract_field_encoded(&map, "key")?;

                let tree = get_collection(&state.borrow(), &collection)?;

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
    state: SharedPerShardState,
    client: &mut TcpStream,
) -> Result<()> {
    let size_buf = read_exactly(client, 2).await?;
    let size = u16::from_le_bytes(size_buf.as_slice().try_into().unwrap());
    let request_buf = read_exactly(client, size.into()).await?;

    match handle_request(state, request_buf).await {
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

    Ok(())
}

async fn run_shard(
    args: Args,
    id: usize,
    local_connections: Vec<LocalShardConnection>,
) -> Result<()> {
    info!("Starting shard of id: {}", id);

    let receiver = local_connections
        .iter()
        .filter(|c| c.cpu == id)
        .map(|c| c.receiver.clone())
        .next()
        .unwrap();

    let shard_name = format!("{}-{}", args.name, id);

    let port = args.port + id as u16;
    let mut shards = local_connections
        .into_iter()
        .map(|c| {
            Shard::new(
                hash_string(&shard_name).unwrap(),
                ShardConnection::Local(c),
            )
        })
        .collect::<Vec<_>>();

    // TODO: Add remote shards.
    // for i in 12345..12345 + 12 {
    //     let address = format!("localhost:{}", i);
    //     shards.push(Shard::new(
    //         hash_string(&address).unwrap(),
    //         ShardConnection::Remote(RemoteShardConnection::new(address)),
    //     ));
    // }

    // Sort by hash to make it a consistant hashing ring.
    shards.sort_unstable_by_key(|x| x.hash);

    let cache_len = args.page_cache_size / PAGE_SIZE / shards.len();
    let cache = PageCache::new(cache_len, cache_len / 16)
        .map_err(|e| Error::CacheCreationError(e.to_string()))?;

    let address = format!("{}:{}", args.ip, port);
    let server = TcpListener::bind(address.as_str())?;
    trace!("Listening for clients on: {}", address);

    let state =
        Rc::new(RefCell::new(PerShardState::new(args, id, shards, cache)));

    spawn_local(enclose!((state.clone() => state) async move {
        if let Err(e) = run_shard_messages_receiver(state, receiver).await {
            error!("Error running shard messages receiver: {}", e);
        }
    }))
    .detach();

    spawn_local(enclose!((state.clone() => state) async move {
        run_compaction_loop(state).await;
    }))
    .detach();

    spawn_local(enclose!((state.clone() => state) async move {
        let address = format!(
            "{}:{}",
            state.borrow().args.ip,
            state.borrow().args.remote_shard_port + state.borrow().id as u16
        );
        let server = match TcpListener::bind(address.as_str()) {
            Ok(server) => server,
            Err(e) => {
                error!("Error starting remote messages server: {}", e);
                return;
            }
        };
        trace!("Listening for distributed messages on: {}", address);

        if let Err(e) = run_distributed_messages_server(state, server).await {
            error!("Error running remote messages server: {}", e);
        }
    }))
    .detach();

    loop {
        match server.accept().await {
            Ok(mut client) => {
                spawn_local(enclose!((state.clone() => state) async move {
                    if let Err(e) = handle_client(state, &mut client).await {
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

    let args = Args::parse();

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
