use std::{rc::Rc, time::Duration};

use glommio::{
    executor, spawn_local_into, timer::sleep, Latency, Shares, Task,
};
use log::{error, info};
use rand::{seq::IteratorRandom, thread_rng};

use crate::{
    error::Result,
    gossip::GossipEvent,
    messages::{ShardEvent, ShardMessage},
    remote_shard_connection::RemoteShardConnection,
    shards::MyShard,
};

async fn run_failure_detector(my_shard: Rc<MyShard>) -> Result<()> {
    let interval =
        Duration::from_millis(my_shard.args.failure_detection_interval);
    let mut should_sleep = true;

    loop {
        if should_sleep {
            sleep(interval).await;
        }
        should_sleep = true;

        let mut rng = thread_rng();
        let node = if let Some(node) = my_shard
            .nodes
            .borrow()
            .iter()
            .map(|(_, node)| node)
            .filter(|node| !node.ids.is_empty())
            .choose(&mut rng)
        {
            node.clone()
        } else {
            continue;
        };

        sleep(interval).await;
        should_sleep = false;

        let connection = RemoteShardConnection::from_args(
            format!(
                "{}:{}",
                node.ip,
                node.ids
                    .iter()
                    .map(|id| node.remote_shard_base_port + id)
                    .choose(&mut rng)
                    .unwrap()
            ),
            &my_shard.args,
        );

        if let Err(e) = connection.ping().await {
            my_shard.clone().handle_dead_node(&node.name).await;

            info!(
                "Notifying cluster that we failed to ping '{}': {}",
                connection.address, e
            );

            let gossip_event = GossipEvent::Dead(node.name);

            if let Err(e) = my_shard
                .clone()
                .broadcast_message_to_local_shards(&ShardMessage::Event(
                    ShardEvent::Gossip(gossip_event.clone()),
                ))
                .await
            {
                error!(
                    "Failed to broadcast to local shards, node death event: {}",
                    e
                );
            }

            if let Err(e) = my_shard.clone().gossip(gossip_event).await {
                error!("Failed to gossip node death event: {}", e);
            }
        }
    }
}

pub fn spawn_failure_detector_task(my_shard: Rc<MyShard>) -> Task<Result<()>> {
    let shares = my_shard.args.background_tasks_shares.into();
    spawn_local_into(
        async move {
            let result = run_failure_detector(my_shard).await;
            if let Err(e) = &result {
                error!("Error starting failure detector: {}", e);
            }
            result
        },
        executor().create_task_queue(
            Shares::Static(shares),
            Latency::NotImportant,
            "failure-detector",
        ),
    )
    .unwrap()
}
