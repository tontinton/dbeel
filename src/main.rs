use dbil::{
    error::{Error, Result},
    lsm_tree::{LSMTree, TOMBSTONE},
};
use futures_lite::{AsyncReadExt, AsyncWriteExt, Future};
use glommio::{
    enclose,
    net::{TcpListener, TcpStream},
    spawn_local,
    timer::sleep,
    LocalExecutorBuilder, Placement,
};
use rmpv::encode::write_value;
use rmpv::{decode::read_value_ref, encode::write_value_ref, Value, ValueRef};
use std::env::temp_dir;
use std::{cell::RefCell, time::Duration};
use std::{collections::HashMap, rc::Rc};

// How much files to compact.
const COMPACTION_FACTOR: usize = 2;

// State shared between all coroutines that run on the same core.
struct PerCoreState {
    trees: HashMap<String, Rc<LSMTree>>,
}

impl PerCoreState {
    fn new() -> Self {
        Self {
            trees: HashMap::new(),
        }
    }
}

type SharedPerCoreState = Rc<RefCell<PerCoreState>>;

async fn run_compaction_loop(state: SharedPerCoreState) {
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
                        eprintln!("Failed to compact files: {}", e);
                    }
                    continue 'current_tree_compaction;
                }

                if odd.len() >= COMPACTION_FACTOR && even.len() >= 1 {
                    debug_assert!(even[0] > odd[odd.len() - 1]);

                    odd.push(even[0]);

                    let new_index = even[0] + 1;
                    if let Err(e) = tree.compact(odd, new_index, true).await {
                        eprintln!("Failed to compact files: {}", e);
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
                eprintln!("Failed to flush memtable: {}", e);
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

fn get_collection(state: &PerCoreState, name: &String) -> Result<Rc<LSMTree>> {
    state
        .trees
        .get(name)
        .ok_or(Error::CollectionNotFound(name.to_string()))
        .map(|t| t.clone())
}

async fn handle_request(
    state: SharedPerCoreState,
    client: &mut TcpStream,
) -> Result<Option<Vec<u8>>> {
    let size_buf = read_exactly(client, 2).await?;
    let size = u16::from_le_bytes(size_buf.as_slice().try_into().unwrap());
    let request_buf = read_exactly(client, size.into()).await?;

    let msgpack_request = read_value_ref(&mut &request_buf[..])?.to_owned();
    if let Some(map_vec) = msgpack_request.as_map() {
        let map = Value::Map(map_vec.to_vec());
        match map["type"].as_str() {
            Some("create_collection") => {
                let name = extract_field_as_str(&map, "name")?;

                if state.borrow().trees.contains_key(&name) {
                    return Err(Error::CollectionAlreadyExists(name));
                }

                let mut dir = temp_dir();
                dir.push(&name);
                let tree = Rc::new(LSMTree::open_or_create(dir).await?);
                state.borrow_mut().trees.insert(name, tree);
            }
            Some("drop_collection") => {
                let name = extract_field_as_str(&map, "name")?;

                let tree = state
                    .borrow_mut()
                    .trees
                    .remove(&name)
                    .ok_or(Error::CollectionNotFound(name))?;
                tree.purge()?;
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
    state: SharedPerCoreState,
    client: &mut TcpStream,
) -> Result<()> {
    match handle_request(state, client).await {
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
            eprintln!("{}", error_string);

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

async fn run_server() -> Result<()> {
    let state = Rc::new(RefCell::new(PerCoreState::new()));

    spawn_local(enclose!((state.clone() => state) async move {
        run_compaction_loop(state).await;
    }))
    .detach();

    let server = TcpListener::bind("127.0.0.1:10000")?;
    loop {
        match server.accept().await {
            Ok(mut client) => {
                spawn_local(enclose!((state.clone() => state) async move {
                    handle_client(state, &mut client).await.unwrap();
                }))
                .detach();
            }
            Err(e) => {
                eprintln!("Failed to accept client: {}", e);
            }
        }
    }
}

fn main() -> Result<()> {
    let builder = LocalExecutorBuilder::new(Placement::Fixed(1))
        .spin_before_park(Duration::from_millis(10));
    let handle = builder.name("dbil").spawn(run_server)?;
    handle.join()?
}
