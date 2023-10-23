use std::rc::Rc;

use glommio::{executor, spawn_local_into, Latency, Shares, Task};
use log::error;

use crate::{
    error::{Error, Result},
    shards::MyShard,
};

pub fn spawn_stop_event_waiter_task(my_shard: Rc<MyShard>) -> Task<Result<()>> {
    let shares = my_shard.args.background_tasks_shares.into();
    spawn_local_into(
        async move {
            if let Err(e) = my_shard.stop_receiver.recv().await {
                error!("Failed to receive stop event: {}", e);
            }
            Err(Error::ShardStopped)
        },
        executor().create_task_queue(
            Shares::Static(shares),
            Latency::NotImportant,
            "gossip-server",
        ),
    )
    .unwrap()
}
