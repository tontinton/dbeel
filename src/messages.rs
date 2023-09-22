use async_channel::Sender;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::{gossip::GossipEvent, storage_engine::EntryValue};

#[derive(Clone, Serialize, Deserialize)]
pub enum ShardEvent {
    Gossip(GossipEvent),
    Set(String, Vec<u8>, Vec<u8>, OffsetDateTime),
}

#[derive(Clone, Serialize, Deserialize)]
pub enum ShardRequest {
    Ping,
    GetMetadata,
    GetCollections,
    Set(String, Vec<u8>, Vec<u8>, OffsetDateTime),
    Delete(String, Vec<u8>, OffsetDateTime),
    Get(String, Vec<u8>),
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct NodeMetadata {
    pub name: String,
    pub ip: String,
    pub remote_shard_base_port: u16,
    pub ids: Vec<u16>,
    pub gossip_port: u16,
    pub db_port: u16,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum ShardResponse {
    Pong,
    GetMetadata(Vec<NodeMetadata>),
    GetCollections(Vec<String>),
    Set,
    Delete,
    Get(Option<EntryValue>),
    Error(String),
}

/// Map a ShardResponse with a value to Result<T>.
/// Must also import ShardResponse to compile.
#[macro_export]
macro_rules! response_to_result {
    ($response:expr, $identifier:path) => {
        match $response {
            $identifier(inner_value) => Ok(inner_value),
            ShardResponse::Error(e) => Err(Error::ResponseError(e)),
            _ => Err(Error::ResponseWrongType),
        }
    };
}

/// Map a ShardResponse with no value to Result<()>.
/// Must also import ShardResponse to compile.
#[macro_export]
macro_rules! response_to_empty_result {
    ($response:expr, $identifier:path) => {
        match $response {
            $identifier => Ok(()),
            ShardResponse::Error(e) => Err(Error::ResponseError(e)),
            _ => Err(Error::ResponseWrongType),
        }
    };
}

#[derive(Clone, Serialize, Deserialize)]
pub enum ShardMessage {
    Event(ShardEvent),
    Request(ShardRequest),
    Response(ShardResponse),
}

// A packet that is sent between local shards.
pub struct ShardPacket {
    pub source_id: u16,
    pub message: ShardMessage,
    pub response_sender: Option<Sender<ShardResponse>>,
}

impl ShardPacket {
    pub fn new(id: u16, message: ShardMessage) -> Self {
        Self {
            source_id: id,
            message,
            response_sender: None,
        }
    }

    pub fn new_request(
        id: u16,
        message: ShardMessage,
        sender: Sender<ShardResponse>,
    ) -> Self {
        Self {
            source_id: id,
            message,
            response_sender: Some(sender),
        }
    }
}
