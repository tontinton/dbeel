use std::rc::Rc;

use glommio::{spawn_local, Task};
use log::error;

use crate::{
    error::{Error, Result},
    shards::MyShard,
};

pub fn spawn_stop_event_waiter_task(my_shard: Rc<MyShard>) -> Task<Result<()>> {
    spawn_local(async move {
        if let Err(e) = my_shard.stop_receiver.recv().await {
            error!("Failed to receive stop event: {}", e);
        }
        Err(Error::ShardStopped)
    })
}
