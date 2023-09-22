pub mod error;

use std::{
    net::{SocketAddr, ToSocketAddrs},
    time::Duration,
};

use dbeel::shards::{hash_bytes, hash_string, ClusterMetadata};
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::net::TcpStream;
use rmp_serde::from_slice;
use rmpv::{encode::write_value, Integer, Utf8String, Value};

use crate::error::{Error, Result};

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

struct Shard {
    hash: u32,
    address: SocketAddr,
}

pub struct DbeelClient {
    seed_shards: Vec<SocketAddr>,
    hash_ring: Vec<Shard>,
    connect_timeout: Duration,
    read_timeout: Duration,
    write_timeout: Duration,
}

pub struct Collection<'a> {
    client: &'a DbeelClient,
    name: Utf8String,
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

        let request = Value::Map(vec![(
            Value::String("type".into()),
            Value::String("get_cluster_metadata".into()),
        )]);

        let buf = Self::send_request_ex(
            &seed_addresses,
            request,
            DEFAULT_CONNECT_TIMEOUT,
            Some(DEFAULT_READ_TIMEOUT),
            Some(DEFAULT_WRITE_TIMEOUT),
        )
        .await?;

        let metadata: ClusterMetadata = from_slice(&buf)?;

        let mut hash_ring = Vec::new();
        for node in metadata.nodes {
            let address = format!("{}:{}", node.ip, node.db_port)
                .to_socket_addrs()
                .map_err(Error::ParsingSocketAddress)?
                .collect::<Vec<_>>()[0];
            for shard_id in node.ids {
                let shard_name = format!("{}-{}", node.name, shard_id);
                let hash =
                    hash_string(&shard_name).map_err(Error::HashShardName)?;
                hash_ring.push(Shard { hash, address });
            }
        }
        hash_ring.sort_unstable_by_key(|s| s.hash);

        Ok(Self {
            seed_shards: seed_addresses,
            hash_ring,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            read_timeout: DEFAULT_READ_TIMEOUT,
            write_timeout: DEFAULT_WRITE_TIMEOUT,
        })
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

    pub fn collection<S: Into<Utf8String>>(&self, name: S) -> Collection {
        Collection {
            client: self,
            name: name.into(),
        }
    }

    async fn send_buffer(
        address: &SocketAddr,
        buffer: &Vec<u8>,
        connect_timeout: Duration,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
    ) -> Result<Vec<u8>> {
        let mut stream = TcpStream::connect_timeout(address, connect_timeout)
            .await
            .map_err(Error::ConnectToShard)?;
        stream
            .set_read_timeout(read_timeout)
            .map_err(Error::SetTimeout)?;
        stream
            .set_write_timeout(write_timeout)
            .map_err(Error::SetTimeout)?;

        let size_buffer = (buffer.len() as u16).to_le_bytes();
        stream
            .write_all(&size_buffer)
            .await
            .map_err(Error::CommunicateWithShard)?;
        stream
            .write_all(buffer)
            .await
            .map_err(Error::CommunicateWithShard)?;

        let mut response_buffer = Vec::new();
        stream
            .read_to_end(&mut response_buffer)
            .await
            .map_err(Error::CommunicateWithShard)?;

        Ok(response_buffer)
    }

    pub(crate) async fn send_request(
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
            match Self::send_buffer(
                address,
                &data_encoded,
                connect_timeout,
                read_timeout,
                write_timeout,
            )
            .await
            {
                Ok(response_encoded) => {
                    return Ok(response_encoded);
                }
                Err(e) => {
                    errors.push(e);
                }
            }
        }

        Err(Error::SendRequestToCluster(errors))
    }

    pub(crate) async fn send_sharded_request(
        &self,
        hash: u32,
        request: Value,
    ) -> Result<Vec<u8>> {
        let position = self
            .hash_ring
            .iter()
            .position(|s| s.hash >= hash)
            .unwrap_or(0);
        self.send_request(&[self.hash_ring[position].address], request)
            .await
    }

    pub async fn create_collection_with_replication<S: Into<Utf8String>>(
        &self,
        name: S,
        replication_factor: u16,
    ) -> Result<Collection> {
        let name = to_utf8string(name)?;
        let request = Value::Map(vec![
            (
                Value::String("type".into()),
                Value::String("create_collection".into()),
            ),
            (Value::String("name".into()), Value::String(name.clone())),
            (
                Value::String("replication_factor".into()),
                Value::Integer(replication_factor.into()),
            ),
        ]);
        self.send_request(&self.seed_shards, request).await?;

        Ok(self.collection(name))
    }

    pub async fn create_collection<S: Into<Utf8String>>(
        &self,
        name: S,
    ) -> Result<Collection> {
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

impl<'a> Collection<'a> {
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
        self.client.send_sharded_request(hash, request).await
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
        self.client.send_sharded_request(hash, request).await
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
        self.client.send_sharded_request(hash, request).await
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
