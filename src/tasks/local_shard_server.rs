use std::{rc::Rc, time::Duration};

use glommio::{executor, spawn_local_into, Latency, Shares, Task};
use log::error;

use crate::{error::Result, messages::ShardResponse, shards::MyShard};

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
            Err(response_err) => {
                if let Err(e) = packet
                    .response_sender
                    .unwrap()
                    .send(ShardResponse::new_err(&response_err))
                    .await
                {
                    error!(
                        "Failed to reply error to local shard ({}): {}, {}",
                        shard_id, e, response_err
                    );
                }
            }
        }
    }
}

pub fn spawn_local_shard_server_task(
    my_shard: Rc<MyShard>,
) -> Task<Result<()>> {
    let shares = my_shard.args.foreground_tasks_shares.into();
    spawn_local_into(
        async move {
            let result = run_shard_messages_receiver(my_shard).await;
            if let Err(e) = &result {
                error!("Error running shard messages receiver: {}", e);
            }
            result
        },
        executor().create_task_queue(
            Shares::Static(shares),
            Latency::Matters(Duration::from_millis(50)),
            "local-shard-server",
        ),
    )
    .unwrap()
}
