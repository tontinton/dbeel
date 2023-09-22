use bincode::Options;
use kinded::Kinded;
use serde::{Deserialize, Serialize};

use crate::{
    error::Result, messages::NodeMetadata, utils::bincode::bincode_options,
};

#[derive(Serialize, Deserialize, Debug, Clone, Kinded)]
#[kinded(derive(Hash))]
pub enum GossipEvent {
    Alive(NodeMetadata),
    Dead(String),
    CreateCollection(String, u16),
    DropCollection(String),
}

#[derive(Serialize, Deserialize)]
pub struct GossipMessage {
    pub source: String,
    pub event: GossipEvent,
}

impl GossipMessage {
    pub fn new(source: String, event: GossipEvent) -> Self {
        Self { source, event }
    }
}

pub fn deserialize_gossip_message(buf: &[u8]) -> Result<GossipMessage> {
    let mut cursor = std::io::Cursor::new(buf);
    Ok(bincode_options().deserialize_from::<_, GossipMessage>(&mut cursor)?)
}

pub fn serialize_gossip_message(message: &GossipMessage) -> Result<Vec<u8>> {
    Ok(bincode_options().serialize(&message)?)
}
