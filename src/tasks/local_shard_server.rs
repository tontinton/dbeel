use std::rc::Rc;

use glommio::{spawn_local, Task};
use log::error;

use crate::{error::Result, shards::MyShard};

async fn run_shard_messages_receiver(my_shard: Rc<MyShard>) -> Result<()> {
    let shard_id = my_shard.id;

    loop {
        let packet = my_shard.local_shards_packet_receiver.recv().await?;
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
) -> Task<Result<()>> {
    spawn_local(async move {
        let result = run_shard_messages_receiver(my_shard).await;
        if let Err(e) = &result {
            error!("Error running shard messages receiver: {}", e);
        }
        result
    })
}
