use bincode::{
    config::{
        FixintEncoding, RejectTrailing, WithOtherIntEncoding, WithOtherTrailing,
    },
    DefaultOptions, Options,
};
use serde::{Deserialize, Serialize};

use crate::{error::Result, messages::NodeMetadata};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum GossipEvent {
    Alive(NodeMetadata),
    Dead(String),
}

impl From<&GossipEvent> for u8 {
    fn from(value: &GossipEvent) -> Self {
        match value {
            GossipEvent::Alive(_) => 0,
            GossipEvent::Dead(_) => 1,
        }
    }
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
