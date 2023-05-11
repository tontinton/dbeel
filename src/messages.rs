use async_channel::Sender;
use serde::{Serialize, Deserialize};

#[derive(Clone, Serialize, Deserialize)]
pub enum ShardEvent {
    CreateCollection(String),
    DropCollection(String),
}

#[derive(Clone, Serialize, Deserialize)]
pub enum ShardRequest {
    GetShards,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum ShardResponse {
    GetShards(Vec<(String, String)>),
}

#[derive(Clone, Serialize, Deserialize)]
pub enum ShardMessage {
    Event(ShardEvent),
    Request(ShardRequest),
    Response(ShardResponse),
}

// A packet that is sent between shards.
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
