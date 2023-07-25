pub mod error;

use std::{
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
};

use dbeel::shards::{hash_string, ClusterMetadata};
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::net::TcpStream;
use rmp_serde::from_slice;
use rmpv::{encode::write_value, Utf8String, Value};

use crate::error::{Error, Result};

struct Shard {
    hash: u32,
    address: SocketAddr,
}

pub struct DbeelClient {
    seed_shards: Vec<SocketAddr>,
    hash_ring: Vec<Shard>,
    replication_factor: u32,
}

pub struct Collection {
    client: Arc<DbeelClient>,
    name: Utf8String,
}

impl DbeelClient {
    pub async fn from_seed_nodes<A>(addresses: &[A]) -> Result<Arc<Self>>
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
        let buf = Self::send_request(&seed_addresses, request).await?;
        let metadata: ClusterMetadata = from_slice(&buf)?;
        let flatten_shards = metadata
            .nodes
            .into_iter()
            .map(|node| format!("{}:{}", node.ip, node.db_port))
            .flat_map(|address| {
                address
                    .to_socket_addrs()
                    .map(|socket_addr| {
                        let hash =
                            hash_string(&address).map_err(Error::HashShardName);
                        (hash, socket_addr)
                    })
                    .map_err(Error::ParsingSocketAddress)
            })
            .collect::<Vec<(Result<u32>, std::vec::IntoIter<SocketAddr>)>>();

        let mut hash_ring = Vec::new();
        for (hash_result, socket_addrs) in flatten_shards {
            let hash = hash_result?;
            for address in socket_addrs {
                hash_ring.push(Shard { hash, address });
            }
        }
        hash_ring.sort_unstable_by_key(|s| s.hash);

        Ok(Arc::new(Self {
            seed_shards: seed_addresses,
            hash_ring,
            replication_factor: metadata.replication_factor,
        }))
    }

    pub fn collection(self: Arc<Self>, name: Utf8String) -> Collection {
        Collection { client: self, name }
    }

    async fn send_buffer(
        address: &SocketAddr,
        buffer: &Vec<u8>,
    ) -> Result<Vec<u8>> {
        let mut stream = TcpStream::connect(address)
            .await
            .map_err(Error::ConnectToShard)?;

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
        addresses: &[SocketAddr],
        request: Value,
    ) -> Result<Vec<u8>> {
        if addresses.is_empty() {
            return Err(Error::NoAddresses);
        }

        let mut data_encoded: Vec<u8> = Vec::new();
        write_value(&mut data_encoded, &request)?;

        let mut errors = vec![];
        for address in addresses {
            match Self::send_buffer(address, &data_encoded).await {
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

    pub async fn create_collection<S: Into<Utf8String>>(
        self: Arc<Self>,
        name: S,
    ) -> Result<Collection> {
        let utf8_name = name.into();
        if !utf8_name.is_str() {
            return Err(Error::InvalidUtf8String(utf8_name));
        }

        let request = Value::Map(vec![
            (
                Value::String("type".into()),
                Value::String("create_collection".into()),
            ),
            (
                Value::String("name".into()),
                Value::String(utf8_name.clone()),
            ),
        ]);
        Self::send_request(&self.seed_shards, request).await?;

        Ok(self.collection(utf8_name))
    }
}

impl Collection {
    pub async fn get<S: Into<Utf8String>>(&self, key: S) -> Result<Vec<u8>> {
        let utf8_key = key.into();
        if !utf8_key.is_str() {
            return Err(Error::InvalidUtf8String(utf8_key));
        }

        let request = Value::Map(vec![
            (Value::String("type".into()), Value::String("get".into())),
            (Value::String("key".into()), Value::String(utf8_key.clone())),
            (
                Value::String("collection".into()),
                Value::String(self.name.clone()),
            ),
        ]);
        let hash = hash_string(&(utf8_key.into_str().unwrap()))
            .map_err(Error::HashShardName)?;
        let position = self
            .client
            .hash_ring
            .iter()
            .position(|s| s.hash >= hash)
            .unwrap_or(0);

        let mut owning_shards = Vec::new();
        for i in 0..self.client.replication_factor {
            let index = (position + i as usize) % self.client.hash_ring.len();
            if i > 0 && index == position {
                break;
            }
            owning_shards.push(self.client.hash_ring[index].address);
        }
        dbg!(&owning_shards);
        Ok(DbeelClient::send_request(&owning_shards, request).await?)
    }
}
