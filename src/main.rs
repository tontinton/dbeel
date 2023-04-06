use dbil::lsm_tree::{LSMTree, TOMBSTONE};
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::{
    enclose,
    net::{TcpListener, TcpStream},
    spawn_local,
    timer::sleep,
    LocalExecutorBuilder, Placement,
};
use rmpv::encode::write_value;
use rmpv::{decode::read_value_ref, encode::write_value_ref, Value, ValueRef};
use std::cell::UnsafeCell;
use std::rc::Rc;
use std::time::Duration;
use std::{env::temp_dir, io::Result};

// How much files to compact.
const COMPACTION_FACTOR: usize = 8;

async fn run_compaction_loop(tree: Rc<UnsafeCell<LSMTree>>) {
    loop {
        let indices = unsafe { (*tree.get()).sstable_indices() };
        let (even, mut odd): (Vec<usize>, Vec<usize>) =
            indices.into_iter().partition(|i| *i % 2 == 0);

        if even.len() >= COMPACTION_FACTOR {
            let new_index = even[even.len() - 1] + 1;
            if let Err(e) = unsafe {
                (*tree.get()).compact(even, new_index, odd.is_empty())
            }
            .await
            {
                eprintln!("Failed to compact files: {}", e);
            }
            continue;
        }

        if odd.len() >= COMPACTION_FACTOR && even.len() >= 1 {
            debug_assert!(even[0] > odd[odd.len() - 1]);

            odd.push(even[0]);

            let new_index = even[0] + 1;
            if let Err(e) =
                unsafe { (*tree.get()).compact(odd, new_index, true) }.await
            {
                eprintln!("Failed to compact files: {}", e);
            }
            continue;
        }

        sleep(Duration::from_millis(10)).await;
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

async fn write_to_tree(
    tree: Rc<UnsafeCell<LSMTree>>,
    key: Vec<u8>,
    value: Vec<u8>,
) -> std::io::Result<()> {
    // Wait for flush.
    while unsafe { (*tree.get()).memtable_full() } {
        futures_lite::future::yield_now().await;
    }

    unsafe { (*tree.get()).set(key, value) }.await?;

    if unsafe { (*tree.get()).memtable_full() } {
        // Capacity is full, flush memtable to disk.
        spawn_local(enclose!((tree.clone() => tree) async move {
            if let Err(e) =
                unsafe { (*tree.get()).flush() }.await
            {
                eprintln!("Failed to flush memtable: {}", e);
            }
        }))
        .detach();
    }

    Ok(())
}

async fn handle_request(
    tree: Rc<UnsafeCell<LSMTree>>,
    client: &mut TcpStream,
) -> std::io::Result<Option<Vec<u8>>> {
    let size_buf = read_exactly(client, 2).await?;
    let size = u16::from_le_bytes(size_buf.as_slice().try_into().unwrap());
    let request_buf = read_exactly(client, size.into()).await?;

    let msgpack_request = read_value_ref(&mut &request_buf[..])
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "could not parse msgpack",
            )
        })?
        .to_owned();
    if let Some(map_vec) = msgpack_request.as_map() {
        let map = Value::Map(map_vec.to_vec());
        match map["type"].as_str() {
            Some("set") => {
                let key = &map["key"];
                let value = &map["value"];

                if key.is_nil() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "field 'key' is missing",
                    ));
                }
                if value.is_nil() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "field 'value' is missing",
                    ));
                }

                let mut key_encoded: Vec<u8> = Vec::new();
                write_value(&mut key_encoded, key).unwrap();
                let mut value_encoded: Vec<u8> = Vec::new();
                write_value(&mut value_encoded, value).unwrap();

                write_to_tree(tree, key_encoded, value_encoded).await?;
            }
            Some("delete") => {
                let key = &map["key"];

                if key.is_nil() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "field 'key' is missing",
                    ));
                }

                let mut key_encoded: Vec<u8> = Vec::new();
                write_value(&mut key_encoded, key).unwrap();

                write_to_tree(tree, key_encoded, TOMBSTONE).await?;
            }
            Some("get") => {
                let key = &map["key"];

                if key.is_nil() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "field 'key' is missing",
                    ));
                }

                let mut key_encoded: Vec<u8> = Vec::new();
                write_value(&mut key_encoded, key).unwrap();

                return match unsafe { (*tree.get()).get(&key_encoded) }.await? {
                    Some(value) if value != TOMBSTONE => Ok(Some(value)),
                    _ => Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "key not found",
                    )),
                };
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "unsupported request",
                ));
            }
        }
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "request should be a map",
        ));
    }

    Ok(None)
}

async fn handle_client(tree: Rc<UnsafeCell<LSMTree>>, client: &mut TcpStream) {
    match handle_request(tree, client).await {
        Ok(None) => {
            let mut buf: Vec<u8> = Vec::new();
            write_value_ref(&mut buf, &ValueRef::String("OK".into())).unwrap();
            client.write_all(&buf).await.unwrap();
        }
        Ok(Some(buf)) => {
            client.write_all(&buf).await.unwrap();
        }
        Err(e) => {
            let error_string = format!("Error: {}", e);
            eprintln!("{}", error_string);

            let mut buf: Vec<u8> = Vec::new();
            write_value_ref(
                &mut buf,
                &ValueRef::String(error_string.as_str().into()),
            )
            .unwrap();
            client.write_all(&buf).await.unwrap();
        }
    }
}

async fn run_server() -> Result<()> {
    let mut db_dir = temp_dir();
    db_dir.push("dbil");

    let tree = Rc::new(UnsafeCell::new(LSMTree::open_or_create(db_dir).await?));

    spawn_local(enclose!((tree.clone() => tree) async move {
        run_compaction_loop(tree).await;
    }))
    .detach();

    let server = TcpListener::bind("127.0.0.1:10000")?;
    loop {
        match server.accept().await {
            Ok(mut client) => {
                spawn_local(enclose!((tree.clone() => tree) async move {
                    handle_client(tree, &mut client).await;
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
