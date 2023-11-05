use async_channel::{Receiver, Sender};

use crate::{
    error::Result,
    messages::{ShardMessage, ShardPacket, ShardRequest, ShardResponse},
};

#[derive(Debug, Clone)]
pub struct LocalShardConnection {
    pub id: u16,
    pub sender: Sender<ShardPacket>,
    pub receiver: Receiver<ShardPacket>,
    pub stop_sender: Sender<()>,
    pub stop_receiver: Receiver<()>,
}

impl LocalShardConnection {
    #[must_use]
    pub fn new(id: u16) -> Self {
        let (sender, receiver) = async_channel::unbounded();
        let (stop_sender, stop_receiver) = async_channel::bounded(1);
        Self {
            id,
            sender,
            receiver,
            stop_sender,
            stop_receiver,
        }
    }

    pub async fn send_request(
        &self,
        sender_id: u16,
        request: ShardRequest,
    ) -> Result<ShardResponse> {
        let (sender, receiver) = async_channel::bounded(1);
        let message = ShardMessage::Request(request);

        // Must clone sender, or the channel will be closed.
        self.sender
            .send(ShardPacket::new_request(sender_id, message, sender.clone()))
            .await?;

        Ok(receiver.recv().await?)
    }
}
