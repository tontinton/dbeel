use std::{rc::Rc, time::Duration};

use glommio::{spawn_local, timer::sleep, Task};
use log::{error, info};
use rand::{seq::IteratorRandom, thread_rng};

use crate::{
    error::Result, gossip::GossipEvent,
    remote_shard_connection::RemoteShardConnection, shards::MyShard,
};

async fn run_failure_detector(my_shard: Rc<MyShard>) -> Result<()> {
    loop {
        sleep(Duration::from_millis(500)).await;

        let mut rng = thread_rng();
        let node = if let Some(node) = my_shard
            .nodes
            .borrow()
            .iter()
            .map(|(_, node)| node)
            .filter(|node| !node.shard_ports.is_empty())
            .choose(&mut rng)
        {
            node.clone()
        } else {
            continue;
        };

        let connection = RemoteShardConnection::new(
            format!("{}:{}", node.ip, node.shard_ports[0]),
            Duration::from_millis(my_shard.args.remote_shard_connect_timeout),
        );

        if let Err(e) = connection.ping().await {
            my_shard.handle_dead_node(&node.name);

            info!(
                "Notifying cluster that we failed to ping '{}': {}",
                connection.address, e
            );

            if let Err(e) =
                my_shard.clone().gossip(GossipEvent::Dead(node.name)).await
            {
                error!("Failed to gossip node death event: {}", e);
            }
        }
    }
}

pub fn spawn_failure_detector_task(my_shard: Rc<MyShard>) -> Task<Result<()>> {
    spawn_local(async move {
        let result = run_failure_detector(my_shard).await;
        if let Err(e) = &result {
            error!("Error starting failure detector: {}", e);
        }
        result
    })
}
