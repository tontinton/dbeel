use async_channel::{Receiver, Sender};

use crate::{
    error::Result,
    messages::{ShardMessage, ShardPacket, ShardRequest, ShardResponse},
};

#[derive(Debug, Clone)]
pub struct LocalShardConnection {
    pub id: usize,
    pub sender: Sender<ShardPacket>,
    pub receiver: Receiver<ShardPacket>,
    pub stop_sender: Sender<()>,
    pub stop_receiver: Receiver<()>,
}

impl LocalShardConnection {
    pub fn new(id: usize) -> Self {
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
        request: ShardRequest,
    ) -> Result<ShardResponse> {
        let (sender, receiver) = async_channel::bounded(1);
        let message = ShardMessage::Request(request);
        self.sender
            .send(ShardPacket::new_request(self.id, message, sender))
            .await?;
        Ok(receiver.recv().await?)
    }
}
