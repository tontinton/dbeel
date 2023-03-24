use dbil::lsm_tree::LSMTree;
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::net::{TcpListener, TcpStream};
use glommio::{spawn_local, LocalExecutorBuilder, Placement};
use rmpv::encode::write_value;
use rmpv::{decode::read_value_ref, encode::write_value_ref, Value, ValueRef};
use std::cell::UnsafeCell;
use std::rc::Rc;
use std::time::Duration;
use std::{env::temp_dir, io::Result};

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

async fn handle_request(
    tree: *mut LSMTree,
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

                unsafe {
                    (*tree).set(key_encoded, value_encoded).await?;
                }
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

                let result = unsafe { (*tree).get(&key_encoded).await? };
                if let Some(value) = result {
                    return Ok(Some(value));
                } else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "key not found",
                    ));
                }
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

async fn handle_client(tree: *mut LSMTree, client: &mut TcpStream) {
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
            let error_string = format!("Error: {}", e.to_string());
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

    let server = TcpListener::bind("127.0.0.1:10000")?;
    loop {
        match server.accept().await {
            Ok(mut client) => {
                let cloned_tree = tree.clone();
                spawn_local(async move {
                    handle_client(cloned_tree.get(), &mut client).await;
                })
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
