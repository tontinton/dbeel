use bincode::{
    config::{
        FixintEncoding, RejectTrailing, WithOtherIntEncoding, WithOtherTrailing,
    },
    DefaultOptions, Options,
};
use kinded::Kinded;
use serde::{Deserialize, Serialize};

use crate::{error::Result, messages::NodeMetadata};

#[derive(Serialize, Deserialize, Debug, Clone, Kinded)]
#[kinded(derive(Hash))]
pub enum GossipEvent {
    Alive(NodeMetadata),
    Dead(String),
    CreateCollection(String),
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

fn bincode_options() -> WithOtherIntEncoding<
    WithOtherTrailing<DefaultOptions, RejectTrailing>,
    FixintEncoding,
> {
    DefaultOptions::new()
        .reject_trailing_bytes()
        .with_fixint_encoding()
}

pub fn deserialize_gossip_message(buf: &[u8]) -> Result<GossipMessage> {
    let mut cursor = std::io::Cursor::new(buf);
    Ok(bincode_options().deserialize_from::<_, GossipMessage>(&mut cursor)?)
}

pub fn serialize_gossip_message(message: &GossipMessage) -> Result<Vec<u8>> {
    Ok(bincode_options().serialize(&message)?)
}
