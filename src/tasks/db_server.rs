use std::{cmp::min, rc::Rc, time::Duration};

use futures::{future::try_join, AsyncRead, AsyncWrite, AsyncWriteExt};
use glommio::{enclose, net::TcpListener, spawn_local, Task};
use log::{error, trace};
use rmp_serde::Serializer;
use rmpv::{
    decode::read_value_ref,
    encode::{write_value, write_value_ref},
    Value, ValueRef,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::{
    error::{Error, Result},
    gossip::GossipEvent,
    messages::{ShardRequest, ShardResponse},
    response_to_empty_result, response_to_result,
    shards::MyShard,
    storage_engine::TOMBSTONE,
    utils::{read_exactly::read_exactly, timeout::timeout},
};

const DEFAULT_SET_TIMEOUT_MS: u64 = 15000;
const DEFAULT_GET_TIMEOUT_MS: u64 = 15000;

#[derive(Serialize, Deserialize)]
pub struct ResponseError {
    message: String,
    name: String,
}

impl ResponseError {
    fn new(e: &Error) -> Self {
        Self {
            message: format!("{}", e),
            name: e.kind().to_string(),
        }
    }

    pub fn same_as(&self, e: &Error) -> bool {
        self.message == format!("{}", e) && self.name == e.kind().to_string()
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
        .ok_or_else(|| Error::MissingField(field_name.to_string()))?
        .to_string())
}

fn extract_field_as_u64(map: &Value, field_name: &str) -> Result<u64> {
    extract_field(map, field_name)?
        .as_u64()
        .ok_or_else(|| Error::MissingField(field_name.to_string()))
}

fn extract_field_as_u16(map: &Value, field_name: &str) -> Result<u16> {
    let number = extract_field_as_u64(map, field_name)?;
    if (0..u16::MAX as u64).contains(&number) {
        Ok(number as u16)
    } else {
        Err(Error::FieldNotU16(field_name.to_string()))
    }
}

fn extract_field_encoded(map: &Value, field_name: &str) -> Result<Vec<u8>> {
    let field = extract_field(map, field_name)?;

    let mut field_encoded: Vec<u8> = Vec::new();
    write_value(&mut field_encoded, field)?;

    Ok(field_encoded)
}

async fn handle_request(
    my_shard: Rc<MyShard>,
    buffer: Vec<u8>,
) -> Result<Option<Vec<u8>>> {
    let msgpack_request = read_value_ref(&mut &buffer[..])?.to_owned();
    if let Some(map_vec) = msgpack_request.as_map() {
        let timestamp = OffsetDateTime::now_utc();
        let map = Value::Map(map_vec.to_vec());
        match map["type"].as_str() {
            Some("get_cluster_metadata") => {
                let mut response = Vec::new();
                my_shard
                    .get_cluster_metadata()
                    .serialize(&mut Serializer::new(&mut response))?;
                return Ok(Some(response));
            }
            Some("create_collection") => {
                let name = extract_field_as_str(&map, "name")?;
                let replication_factor =
                    extract_field_as_u16(&map, "replication_factor")
                        .unwrap_or(my_shard.args.default_replication_factor);

                if my_shard.collections.borrow().contains_key(&name) {
                    return Err(Error::CollectionAlreadyExists(name));
                }

                my_shard
                    .create_collection(name.clone(), replication_factor)
                    .await?;
                my_shard
                    .gossip(GossipEvent::CreateCollection(
                        name,
                        replication_factor,
                    ))
                    .await?;
            }
            Some("drop_collection") => {
                let name = extract_field_as_str(&map, "name")?;
                my_shard.drop_collection(&name)?;
                my_shard.gossip(GossipEvent::DropCollection(name)).await?;
            }
            Some("set") => {
                let collection_name = extract_field_as_str(&map, "collection")?;
                let key = extract_field_encoded(&map, "key")?;
                let value = extract_field_encoded(&map, "value")?;
                let write_timeout = Duration::from_millis(
                    extract_field_as_u64(&map, "timeout")
                        .unwrap_or(DEFAULT_SET_TIMEOUT_MS),
                );

                let collection = my_shard.get_collection(&collection_name)?;
                let tree = collection.tree;
                let replications = collection.metadata.replication_factor;

                let write_consistency = min(
                    extract_field_as_u16(&map, "consistency")
                        .unwrap_or(replications),
                    replications,
                );

                if replications > 1 {
                    let local_future = tree.set_with_timestamp(
                        key.clone(),
                        value.clone(),
                        timestamp,
                    );
                    let remote_future =
                        my_shard.clone().send_request_to_replicas(
                            ShardRequest::Set(
                                collection_name,
                                key,
                                value,
                                timestamp,
                            ),
                            write_consistency as usize - 1,
                            replications as usize - 1,
                            |res| {
                                response_to_empty_result!(
                                    res,
                                    ShardResponse::Set
                                )
                            },
                        );
                    timeout(
                        write_timeout,
                        try_join(local_future, remote_future),
                    )
                    .await?;
                } else {
                    timeout(
                        write_timeout,
                        tree.set_with_timestamp(key, value, timestamp),
                    )
                    .await?;
                }
            }
            Some("delete") => {
                let collection_name = extract_field_as_str(&map, "collection")?;
                let key = extract_field_encoded(&map, "key")?;
                let delete_timeout = Duration::from_millis(
                    extract_field_as_u64(&map, "timeout")
                        .unwrap_or(DEFAULT_SET_TIMEOUT_MS),
                );

                let collection = my_shard.get_collection(&collection_name)?;
                let tree = collection.tree;
                let replications = collection.metadata.replication_factor;

                let delete_consistency = min(
                    extract_field_as_u16(&map, "consistency")
                        .unwrap_or(replications),
                    replications,
                );

                if replications > 1 {
                    let local_future =
                        tree.delete_with_timestamp(key.clone(), timestamp);
                    let remote_future =
                        my_shard.clone().send_request_to_replicas(
                            ShardRequest::Delete(
                                collection_name,
                                key,
                                timestamp,
                            ),
                            delete_consistency as usize - 1,
                            replications as usize - 1,
                            |res| {
                                response_to_empty_result!(
                                    res,
                                    ShardResponse::Delete
                                )
                            },
                        );
                    timeout(
                        delete_timeout,
                        try_join(local_future, remote_future),
                    )
                    .await?;
                } else {
                    timeout(
                        delete_timeout,
                        tree.delete_with_timestamp(key, timestamp),
                    )
                    .await?;
                }
            }
            Some("get") => {
                let collection_name = extract_field_as_str(&map, "collection")?;
                let key = extract_field_encoded(&map, "key")?;
                let read_timeout = Duration::from_millis(
                    extract_field_as_u64(&map, "timeout")
                        .unwrap_or(DEFAULT_GET_TIMEOUT_MS),
                );

                let collection = my_shard.get_collection(&collection_name)?;
                let tree = collection.tree;
                let replications = collection.metadata.replication_factor;

                let read_consistency = min(
                    extract_field_as_u16(&map, "consistency")
                        .unwrap_or(replications),
                    replications,
                );

                return if replications > 1 {
                    let local_future = tree.get_entry(&key);
                    let remote_future =
                        my_shard.clone().send_request_to_replicas(
                            ShardRequest::Get(collection_name, key.clone()),
                            read_consistency as usize - 1,
                            replications as usize - 1,
                            |res| response_to_result!(res, ShardResponse::Get),
                        );
                    let (local_value, mut values) = timeout(
                        read_timeout,
                        try_join(local_future, remote_future),
                    )
                    .await?;

                    values.push(local_value);

                    match values
                        .into_iter()
                        .flatten()
                        .max_by_key(|v| v.timestamp)
                        .map(|v| v.data)
                    {
                        Some(value) if value != TOMBSTONE => Ok(Some(value)),
                        _ => Err(Error::KeyNotFound),
                    }
                } else {
                    match timeout(read_timeout, tree.get(&key)).await? {
                        Some(value) if value != TOMBSTONE => Ok(Some(value)),
                        _ => Err(Error::KeyNotFound),
                    }
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
            if !matches!(e, Error::KeyNotFound) {
                error!("Error while handling request: {0:?}, '{0}'", e);
            }

            let mut buf: Vec<u8> = Vec::new();
            ResponseError::new(&e).serialize(&mut Serializer::new(&mut buf))?;
            client.write_all(&buf).await?;
        }
    }

    client.close().await?;

    Ok(())
}

async fn run_server(my_shard: Rc<MyShard>) -> Result<()> {
    let port = my_shard.args.port + my_shard.id;
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

pub fn spawn_db_server_task(my_shard: Rc<MyShard>) -> Task<Result<()>> {
    spawn_local(async move {
        let result = run_server(my_shard).await;
        if let Err(e) = &result {
            error!("Error running server: {}", e);
        }
        result
    })
}
