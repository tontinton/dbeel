pub mod error;

use std::{
    collections::HashSet,
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
    time::Duration,
};

use async_rwlock::RwLock;
use dbeel::{
    shards::{hash_bytes, hash_string, ClusterMetadata, CollectionMetadata},
    tasks::db_server::{ResponseError, ResponseType},
};
use error::VecError;
use futures_lite::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use glommio::net::TcpStream;
use rmp_serde::from_slice;
use rmpv::{encode::write_value, Integer, Utf8String, Value};

use crate::error::{Error, Result};

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
struct Shard {
    hash: u32,
    address: SocketAddr,
    node_name: String,
}

#[derive(Debug, Clone)]
pub struct DbeelClient {
    seed_shards: Vec<SocketAddr>,
    hash_ring: Arc<RwLock<Vec<Shard>>>,
    connect_timeout: Duration,
    read_timeout: Duration,
    write_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct Collection {
    client: DbeelClient,
    name: Utf8String,
    metadata: CollectionMetadata,
}

fn to_utf8string<S: Into<Utf8String>>(
    maybe_utf8string: S,
) -> Result<Utf8String> {
    let utf8 = maybe_utf8string.into();
    if !utf8.is_str() {
        return Err(Error::InvalidUtf8String(utf8));
    }
    Ok(utf8)
}

fn hash_key(key: &Value) -> Result<u32> {
    let mut buf: Vec<u8> = Vec::new();
    write_value(&mut buf, key)?;
    hash_bytes(&buf).map_err(Error::HashKey)
}

enum ShardedRequestResult {
    Buf(Vec<u8>),
    Resync,
}

impl DbeelClient {
    pub async fn from_seed_nodes<A>(addresses: &[A]) -> Result<Self>
    where
        A: ToSocketAddrs,
    {
        let mut seed_addresses = vec![];
        for address in addresses {
            match address.to_socket_addrs() {
                Ok(addrs) => seed_addresses.extend(addrs),
                Err(e) => return Err(Error::ParsingSocketAddress(e)),
            };
        }

        let this = Self {
            seed_shards: seed_addresses,
            hash_ring: Arc::new(RwLock::new(Vec::new())),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            read_timeout: DEFAULT_READ_TIMEOUT,
            write_timeout: DEFAULT_WRITE_TIMEOUT,
        };

        this.sync_hash_ring().await?;

        Ok(this)
    }

    async fn sync_hash_ring(&self) -> Result<()> {
        let request = Value::Map(vec![(
            Value::String("type".into()),
            Value::String("get_cluster_metadata".into()),
        )]);

        let buf = Self::send_request_ex(
            &self.seed_shards,
            request,
            DEFAULT_CONNECT_TIMEOUT,
            Some(DEFAULT_READ_TIMEOUT),
            Some(DEFAULT_WRITE_TIMEOUT),
        )
        .await?;

        let metadata: ClusterMetadata = from_slice(&buf)?;

        let mut hash_ring = Vec::new();
        for node in metadata.nodes {
            for shard_id in node.ids {
                let address =
                    format!("{}:{}", node.ip, node.db_port + shard_id)
                        .to_socket_addrs()
                        .map_err(Error::ParsingSocketAddress)?
                        .collect::<Vec<_>>()[0];

                let shard_name = format!("{}-{}", node.name, shard_id);
                let hash =
                    hash_string(&shard_name).map_err(Error::HashShardName)?;
                hash_ring.push(Shard {
                    hash,
                    address,
                    node_name: node.name.clone(),
                });
            }
        }
        hash_ring.sort_unstable_by_key(|s| s.hash);

        let mut ring = self.hash_ring.write().await;
        *ring = hash_ring;

        Ok(())
    }

    pub fn set_connect_timeout(&mut self, timeout: Duration) {
        self.connect_timeout = timeout;
    }

    pub fn set_read_timeout(&mut self, timeout: Duration) {
        self.read_timeout = timeout;
    }

    pub fn set_write_timeout(&mut self, timeout: Duration) {
        self.write_timeout = timeout;
    }

    pub async fn collection(&self, name: &str) -> Result<Collection> {
        let hash = hash_string(name).map_err(Error::HashShardName)?;
        let request = Value::Map(vec![
            (
                Value::String("type".into()),
                Value::String("get_collection".into()),
            ),
            (Value::String("name".into()), Value::String(name.into())),
        ]);
        let response = self.send_sharded_request(hash, request, 1).await?;
        let metadata: CollectionMetadata = from_slice(&response)?;

        Ok(Collection {
            client: self.clone(),
            name: name.into(),
            metadata,
        })
    }

    async fn send_buffer(
        stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
        buffer: &[u8],
    ) -> Result<Vec<u8>> {
        let size_buffer = (buffer.len() as u16).to_le_bytes();
        stream
            .write_all(&size_buffer)
            .await
            .map_err(Error::CommunicateWithShard)?;
        stream
            .write_all(buffer)
            .await
            .map_err(Error::CommunicateWithShard)?;

        let mut size_buf = [0; 4];
        stream
            .read_exact(&mut size_buf)
            .await
            .map_err(Error::CommunicateWithShard)?;
        let size = u32::from_le_bytes(size_buf);
        let mut response_buffer = vec![0; size as usize];
        stream
            .read_exact(&mut response_buffer)
            .await
            .map_err(Error::CommunicateWithShard)?;

        Ok(response_buffer)
    }

    async fn send_request(
        &self,
        addresses: &[SocketAddr],
        request: Value,
    ) -> Result<Vec<u8>> {
        Self::send_request_ex(
            addresses,
            request,
            self.connect_timeout,
            Some(self.read_timeout),
            Some(self.write_timeout),
        )
        .await
    }

    async fn send_request_ex(
        addresses: &[SocketAddr],
        request: Value,
        connect_timeout: Duration,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
    ) -> Result<Vec<u8>> {
        if addresses.is_empty() {
            return Err(Error::NoAddresses);
        }

        let mut data_encoded: Vec<u8> = Vec::new();
        write_value(&mut data_encoded, &request)?;

        let mut errors = vec![];
        for address in addresses {
            let mut stream =
                TcpStream::connect_timeout(address, connect_timeout)
                    .await
                    .map_err(Error::ConnectToShard)?;
            stream
                .set_read_timeout(read_timeout)
                .map_err(Error::SetTimeout)?;
            stream
                .set_write_timeout(write_timeout)
                .map_err(Error::SetTimeout)?;

            let response_result =
                Self::send_buffer(&mut stream, &data_encoded).await;

            let _ = stream.close().await;

            match response_result {
                Ok(mut response_encoded)
                    if response_encoded.last()
                        != Some(ResponseType::Err.into()).as_ref() =>
                {
                    response_encoded.pop().unwrap();
                    return Ok(response_encoded);
                }
                Ok(mut response_encoded) => {
                    response_encoded.pop().unwrap();
                    let err: ResponseError = from_slice(&response_encoded)?;
                    errors.push(Error::ServerErr(err.name, err.message));
                }
                Err(e) => {
                    errors.push(e);
                }
            }
        }

        Err(Error::SendRequestToCluster(VecError(errors)))
    }

    async fn _send_sharded_request(
        &self,
        hash: u32,
        request: &Value,
        replication_factor: u16,
    ) -> Result<ShardedRequestResult> {
        let ring = self.hash_ring.read().await;
        let start_shard_index =
            ring.iter().position(|s| s.hash >= hash).unwrap_or(0);

        let mut errors = Vec::new();

        let mut owning_shards_found = 0;
        let mut nodes = HashSet::new();
        let mut i = 0;
        let mut shard_index = start_shard_index;
        while i == 0 || shard_index != start_shard_index {
            let shard = &ring[shard_index];
            if !nodes.contains(&shard.node_name) {
                let mut replica_request = request.clone();
                if let Value::Map(items) = &mut replica_request {
                    items.push((
                        "replica_index".into(),
                        owning_shards_found.into(),
                    ));
                }

                match self.send_request(&[shard.address], replica_request).await
                {
                    Ok(response) => {
                        return Ok(ShardedRequestResult::Buf(response));
                    }
                    Err(Error::SendRequestToCluster(mut e)) => {
                        let request_err = e.pop().unwrap();
                        if let Error::ServerErr(ref name, _) = request_err {
                            let re = ResponseError::new(
                                &dbeel::error::Error::KeyNotOwnedByShard,
                            );
                            if name == &re.name {
                                return Ok(ShardedRequestResult::Resync);
                            }
                        }
                        errors.push(request_err);
                    }
                    Err(e) => {
                        errors.push(e);
                    }
                }

                owning_shards_found += 1;
                if owning_shards_found >= replication_factor {
                    break;
                }

                nodes.insert(&shard.node_name);
            }

            i += 1;
            shard_index = (start_shard_index + i as usize) % ring.len();
        }

        Err(Error::SendRequestToCluster(VecError(errors)))
    }

    async fn send_sharded_request(
        &self,
        hash: u32,
        request: Value,
        replication_factor: u16,
    ) -> Result<Vec<u8>> {
        loop {
            match self
                ._send_sharded_request(hash, &request, replication_factor)
                .await?
            {
                ShardedRequestResult::Buf(buf) => return Ok(buf),
                ShardedRequestResult::Resync => {
                    self.sync_hash_ring().await?;
                }
            };
        }
    }

    pub async fn create_collection_with_replication(
        &self,
        name: &str,
        replication_factor: u16,
    ) -> Result<Collection> {
        let request = Value::Map(vec![
            (
                Value::String("type".into()),
                Value::String("create_collection".into()),
            ),
            (Value::String("name".into()), Value::String(name.into())),
            (
                Value::String("replication_factor".into()),
                Value::Integer(replication_factor.into()),
            ),
        ]);
        self.send_request(&self.seed_shards, request).await?;

        Ok(Collection {
            client: self.clone(),
            name: name.into(),
            metadata: CollectionMetadata { replication_factor },
        })
    }

    pub async fn create_collection(&self, name: &str) -> Result<Collection> {
        self.create_collection_with_replication(name, 1).await
    }

    pub(crate) async fn drop_collection<S: Into<Utf8String>>(
        &self,
        name: S,
    ) -> Result<()> {
        let name = to_utf8string(name)?;
        let request = Value::Map(vec![
            (
                Value::String("type".into()),
                Value::String("drop_collection".into()),
            ),
            (Value::String("name".into()), Value::String(name.clone())),
        ]);
        self.send_request(&self.seed_shards, request).await?;
        Ok(())
    }
}

impl Collection {
    pub async fn get_consistent<I>(
        &self,
        key: Value,
        consistency: I,
    ) -> Result<Vec<u8>>
    where
        I: Into<Integer>,
    {
        let hash = hash_key(&key)?;
        let request = Value::Map(vec![
            (Value::String("type".into()), Value::String("get".into())),
            (Value::String("key".into()), key),
            (
                Value::String("collection".into()),
                Value::String(self.name.clone()),
            ),
            (
                Value::String("consistency".into()),
                Value::Integer(consistency.into()),
            ),
        ]);
        self.client
            .send_sharded_request(
                hash,
                request,
                self.metadata.replication_factor,
            )
            .await
    }

    pub async fn get(&self, key: Value) -> Result<Vec<u8>> {
        self.get_consistent(key, 1).await
    }

    pub async fn get_from_str_key<S>(&self, key: S) -> Result<Vec<u8>>
    where
        S: Into<Utf8String>,
    {
        self.get(Value::String(key.into())).await
    }

    pub async fn set_consistent<I>(
        &self,
        key: Value,
        value: Value,
        consistency: I,
    ) -> Result<Vec<u8>>
    where
        I: Into<Integer>,
    {
        let hash = hash_key(&key)?;
        let request = Value::Map(vec![
            (Value::String("type".into()), Value::String("set".into())),
            (Value::String("key".into()), key),
            (Value::String("value".into()), value),
            (
                Value::String("collection".into()),
                Value::String(self.name.clone()),
            ),
            (
                Value::String("consistency".into()),
                Value::Integer(consistency.into()),
            ),
        ]);
        self.client
            .send_sharded_request(
                hash,
                request,
                self.metadata.replication_factor,
            )
            .await
    }

    pub async fn set(&self, key: Value, value: Value) -> Result<Vec<u8>> {
        self.set_consistent(key, value, 1).await
    }

    pub async fn set_from_str_key<S: Into<Utf8String>>(
        &self,
        key: S,
        value: Value,
    ) -> Result<Vec<u8>> {
        self.set(Value::String(key.into()), value).await
    }

    pub async fn delete_consistent<I>(
        &self,
        key: Value,
        consistency: I,
    ) -> Result<Vec<u8>>
    where
        I: Into<Integer>,
    {
        let hash = hash_key(&key)?;
        let request = Value::Map(vec![
            (Value::String("type".into()), Value::String("delete".into())),
            (Value::String("key".into()), key),
            (
                Value::String("collection".into()),
                Value::String(self.name.clone()),
            ),
            (
                Value::String("consistency".into()),
                Value::Integer(consistency.into()),
            ),
        ]);
        self.client
            .send_sharded_request(
                hash,
                request,
                self.metadata.replication_factor,
            )
            .await
    }

    pub async fn delete(&self, key: Value) -> Result<Vec<u8>> {
        self.delete_consistent(key, 1).await
    }

    pub async fn delete_from_str_key<S: Into<Utf8String>>(
        &self,
        key: S,
    ) -> Result<Vec<u8>> {
        self.delete(Value::String(key.into())).await
    }

    pub async fn drop(self) -> Result<()> {
        self.client.drop_collection(self.name).await
    }
}
