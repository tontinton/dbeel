use async_channel::{Receiver, Sender};
use caches::Cache;
use dbil::{
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
use std::env::temp_dir;
use std::{cell::RefCell, time::Duration};
use std::{collections::HashMap, rc::Rc};

extern crate pretty_env_logger;
#[macro_use]
extern crate log;

// Each shard has a different port calculated by <port_base> + <cpu_id>
const LISTEN_PORT_BASE: usize = 10000;
const LISTEN_IP: &str = "127.0.0.1";

#[cfg(debug_assertions)]
const DEFAULT_LOG_LEVEL: &str = "dbil=trace";
#[cfg(not(debug_assertions))]
const DEFAULT_LOG_LEVEL: &str = "dbil=info";

// How much files to compact.
const COMPACTION_FACTOR: usize = 2;

const PAGE_CACHE_SIZE: usize = 1024 * 1024 * 1024 / PAGE_SIZE; // 1 GB
const PAGE_CACHE_SAMPLES: usize = PAGE_CACHE_SIZE / 16;

#[derive(Clone)]
enum ShardMessage {
    CreateCollection(String),
    DropCollection(String),
}

// A packet that is sent between shards, holds a cpu id and a message.
type ShardPacket = (usize, ShardMessage);

#[derive(Debug, Clone)]
enum PartitionConnection {
    Local {
        cpu: usize,
        sender: Sender<ShardPacket>,
        receiver: Receiver<ShardPacket>,
    },
    Remote(String, u16), // ip:port
}

impl PartitionConnection {
    fn name(&self) -> String {
        match self {
            Self::Local {
                cpu,
                sender: _,
                receiver: _,
            } => format!("local-{}", cpu),
            Self::Remote(ip, port) => format!("{}:{}", ip, port),
        }
    }
}

#[derive(Debug, Clone)]
struct Partition {
    hash: u32,
    connection: PartitionConnection,
}

// State shared between all coroutines that run on the same shard.
struct PerShardState {
    id: usize,
    partitions: Vec<Partition>,
    trees: HashMap<String, Rc<LSMTree>>,
    cache: Rc<RefCell<PageCache<FileId>>>,
}

impl PerShardState {
    fn new(
        id: usize,
        partitions: Vec<Partition>,
        cache: PageCache<FileId>,
    ) -> Self {
        Self {
            id,
            partitions,
            trees: HashMap::new(),
            cache: Rc::new(RefCell::new(cache)),
        }
    }
}

type SharedPerShardState = Rc<RefCell<PerShardState>>;

fn hash_string(s: &String) -> std::io::Result<u32> {
    murmur3_32(&mut std::io::Cursor::new(s), 0)
}

async fn run_shard_messages_receiver(state: SharedPerShardState) -> Result<()> {
    let receivers = state
        .borrow()
        .partitions
        .iter()
        .flat_map(|p| match &p.connection {
            PartitionConnection::Local {
                cpu,
                sender: _,
                receiver,
            } if cpu == &state.borrow().id => Some(receiver.clone()),
            _ => None,
        })
        .collect::<Vec<Receiver<ShardPacket>>>();
    assert_eq!(receivers.len(), 1);
    let receiver = &receivers[0];

    loop {
        let (id, msg) = receiver.recv().await?;
        if id == state.borrow().id {
            continue;
        }

        match msg {
            ShardMessage::CreateCollection(name) => {
                create_collection_for_shard(state.clone(), name).await?;
            }
            ShardMessage::DropCollection(name) => {
                drop_collection_for_shard(state.clone(), name)?;
            }
        };
    }
}

async fn run_compaction_loop(state: SharedPerShardState) {
    loop {
        let trees: Vec<Rc<LSMTree>> =
            state.borrow().trees.values().map(|t| t.clone()).collect();
        for tree in trees {
            'current_tree_compaction: loop {
                let (even, mut odd): (Vec<usize>, Vec<usize>) =
                    tree.sstable_indices().iter().partition(|i| *i % 2 == 0);

                if even.len() >= COMPACTION_FACTOR {
                    let new_index = even[even.len() - 1] + 1;
                    if let Err(e) =
                        tree.compact(even, new_index, odd.is_empty()).await
                    {
                        error!("Failed to compact files: {}", e);
                    }
                    continue 'current_tree_compaction;
                }

                if odd.len() >= COMPACTION_FACTOR && even.len() >= 1 {
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

    // TODO: clear cache only for current collection, not all.
    state.borrow_mut().cache.borrow_mut().purge();

    Ok(())
}

async fn send_message_to_all_shards(
    state: SharedPerShardState,
    message: ShardMessage,
) -> Result<()> {
    let senders = state
        .borrow()
        .partitions
        .iter()
        .flat_map(|p| match &p.connection {
            PartitionConnection::Local {
                cpu: _,
                sender,
                receiver: _,
            } => Some(sender.clone()),
            _ => None,
        })
        .collect::<Vec<Sender<ShardPacket>>>();

    let id = state.borrow().id;

    // TODO: remove unwrap.
    for sender in senders {
        sender.send((id, message.clone())).await.unwrap();
    }

    Ok(())
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

                send_message_to_all_shards(
                    state.clone(),
                    ShardMessage::CreateCollection(name),
                )
                .await?;
            }
            Some("drop_collection") => {
                let name = extract_field_as_str(&map, "name")?;
                drop_collection_for_shard(state.clone(), name.clone())?;

                send_message_to_all_shards(
                    state.clone(),
                    ShardMessage::DropCollection(name),
                )
                .await?;
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
            let error_string = format!("Error: {}", e);
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

async fn run_shard(id: usize, partitions: Vec<Partition>) -> Result<()> {
    info!("Starting shard of id: {}", id);

    let cache = PageCache::new(
        PAGE_CACHE_SIZE / partitions.len(),
        PAGE_CACHE_SAMPLES / partitions.len(),
    )
    .map_err(|e| Error::CacheCreationError(e.to_string()))?;

    let address = format!("{}:{}", LISTEN_IP, LISTEN_PORT_BASE + id);
    let server = TcpListener::bind(address.as_str())?;
    trace!("Listening on: {}", address);

    let state =
        Rc::new(RefCell::new(PerShardState::new(id, partitions, cache)));

    spawn_local(enclose!((state.clone() => state) async move {
        if let Err(e) = run_shard_messages_receiver(state).await {
            error!("Error running shard messages receiver: {}", e);
        }
    }))
    .detach();

    spawn_local(enclose!((state.clone() => state) async move {
        run_compaction_loop(state).await;
    }))
    .detach();

    loop {
        match server.accept().await {
            Ok(mut client) => {
                spawn_local(enclose!((state.clone() => state) async move {
                    handle_client(state, &mut client).await.unwrap();
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

    let benchmark = std::env::var("BENCHMARK")
        .or::<String>(Ok("false".to_string()))
        .unwrap()
        .to_lowercase();
    let skip_half_cpus = benchmark == "true" || benchmark == "1";

    let cpu_set = CpuSet::online()?;
    assert!(!cpu_set.is_empty());

    let channels = (0..cpu_set.len())
        .map(|_| async_channel::unbounded())
        .collect::<Vec<(Sender<ShardPacket>, Receiver<ShardPacket>)>>();

    let partitions = cpu_set
        .into_iter()
        .map(|x| x.cpu)
        .zip(channels)
        .map(|(cpu, (sender, receiver))| {
            let connection = PartitionConnection::Local {
                cpu,
                sender,
                receiver,
            };

            // TODO: remove unwrap.
            let hash = hash_string(&connection.name()).unwrap();

            Partition { hash, connection }
        })
        .collect::<Vec<Partition>>();

    let mut handles = Vec::with_capacity(partitions.len());
    let cpus = partitions
        .iter()
        .flat_map(|p| match &p.connection {
            PartitionConnection::Local {
                cpu,
                sender: _,
                receiver: _,
            } => Some(cpu),
            _ => None,
        })
        .map(|x| *x);

    for cpu in cpus {
        if skip_half_cpus && cpu >= partitions.len() / 2 {
            continue;
        }

        handles.push(
            LocalExecutorBuilder::new(Placement::Fixed(cpu))
                .name(format!("dbil({})", cpu).as_str())
                .spawn(enclose!((partitions.clone() => partitions) move ||
                async move {
                    run_shard(cpu, partitions).await
                }))?,
        );
    }

    let shard_results =
        handles.into_iter().map(|h| h.join()).collect::<Vec<_>>();

    for result in shard_results {
        if result.is_err() {
            error!("Shard returned error: {}", result.unwrap_err());
        }
    }

    Ok(())
}
