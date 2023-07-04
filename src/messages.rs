use async_channel::Sender;
use serde::{Deserialize, Serialize};

use crate::lsm_tree::EntryValue;

#[derive(Clone, Serialize, Deserialize)]
pub enum ShardEvent {
    CreateCollection(String),
    DropCollection(String),
}

#[derive(Clone, Serialize, Deserialize)]
pub enum ShardRequest {
    Ping,
    GetMetadata,
    Set(String, Vec<u8>, Vec<u8>),
    Delete(String, Vec<u8>),
    Get(String, Vec<u8>),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct NodeMetadata {
    pub name: String,
    pub ip: String,
    pub shard_ports: Vec<u16>,
    pub gossip_port: u16,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum ShardResponse {
    Pong,
    GetMetadata(Vec<NodeMetadata>),
    Set,
    Delete,
    Get(Option<EntryValue>),
}

#[derive(Clone, Serialize, Deserialize)]
pub enum ShardMessage {
    Event(ShardEvent),
    Request(ShardRequest),
    Response(ShardResponse),
}

// A packet that is sent between local shards.
pub struct ShardPacket {
    pub source_id: usize,
    pub message: ShardMessage,
    pub response_sender: Option<Sender<ShardResponse>>,
}

impl ShardPacket {
    pub fn new(id: usize, message: ShardMessage) -> Self {
        Self {
            source_id: id,
            message,
            response_sender: None,
        }
    }

    pub fn new_request(
        id: usize,
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
