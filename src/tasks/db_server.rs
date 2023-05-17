use std::rc::Rc;

use futures::{AsyncRead, AsyncWrite, AsyncWriteExt, Future};
use glommio::{enclose, net::TcpListener, spawn_local, Task};
use log::{error, trace};
use rmpv::{
    decode::read_value_ref,
    encode::{write_value, write_value_ref},
    Value, ValueRef,
};

use crate::{
    error::{Error, Result},
    lsm_tree::{LSMTree, TOMBSTONE},
    messages::{ShardEvent, ShardMessage},
    read_exactly::read_exactly,
    shards::MyShard,
};

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
        .ok_or_else(|| Error::MissingField(field_name.to_string()))?
        .to_string())
}

fn extract_field_encoded(map: &Value, field_name: &str) -> Result<Vec<u8>> {
    let field = extract_field(map, field_name)?;

    let mut field_encoded: Vec<u8> = Vec::new();
    write_value(&mut field_encoded, field)?;

    Ok(field_encoded)
}

fn broadcast_message_to_local_shards_in_background(
    my_shard: Rc<MyShard>,
    message: ShardMessage,
) {
    spawn_local(async move {
        if let Err(e) =
            my_shard.broadcast_message_to_local_shards(&message).await
        {
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

                broadcast_message_to_local_shards_in_background(
                    my_shard,
                    ShardMessage::Event(ShardEvent::CreateCollection(name)),
                );
            }
            Some("drop_collection") => {
                let name = extract_field_as_str(&map, "name")?;
                my_shard.clone().drop_collection(name.clone())?;

                broadcast_message_to_local_shards_in_background(
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
    client: &mut (impl AsyncRead + AsyncWrite + Unpin),
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

async fn run_server(my_shard: Rc<MyShard>) -> Result<()> {
    let port = my_shard.args.port + my_shard.id as u16;
    let address = format!("{}:{}", my_shard.args.ip, port);
    let server = TcpListener::bind(address.as_str())?;
    trace!("Listening for clients on: {}", address);

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

pub fn spawn_db_server(my_shard: Rc<MyShard>) -> Task<Result<()>> {
    spawn_local(async move {
        let result = run_server(my_shard).await;
        if let Err(e) = &result {
            error!("Error running server: {}", e);
        }
        result
    })
}
