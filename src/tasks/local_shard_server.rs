use std::rc::Rc;

use async_channel::Receiver;
use glommio::{Task, spawn_local};
use log::error;

use crate::{error::Result, messages::ShardPacket, shards::MyShard};

async fn run_shard_messages_receiver(
    my_shard: Rc<MyShard>,
    receiver: Receiver<ShardPacket>,
) -> Result<()> {
    let shard_id = my_shard.id;

    loop {
        let packet = receiver.recv().await?;
        if packet.source_id == shard_id {
            continue;
        }

        match my_shard.clone().handle_shard_message(packet.message).await {
            Ok(maybe_response) => {
                if let Some(response_msg) = maybe_response {
                    if let Err(e) =
                        packet.response_sender.unwrap().send(response_msg).await
                    {
                        error!(
                            "Failed to reply to local shard ({}): {}",
                            shard_id, e
                        );
                    }
                }
            }
            Err(e) => error!("Failed to handle shard message: {}", e),
        }
    }
}

pub fn spawn_local_shard_server_task(
    my_shard: Rc<MyShard>,
    receiver: Receiver<ShardPacket>,
) -> Task<Result<()>> {
    spawn_local(async move {
        let result = run_shard_messages_receiver(my_shard, receiver).await;
        if let Err(e) = &result {
            error!("Error running shard messages receiver: {}", e);
        }
        result
    })
}
