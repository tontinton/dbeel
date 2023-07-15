use std::rc::Rc;

use dbeel::{
    args::parse_args_from,
    error::Result,
    run_shard::{create_shard, run_shard},
    shards::MyShard,
    flow_events::FlowEvent,
    local_shard::LocalShardConnection,
};
use futures_lite::Future;
use glommio::{
    enclose, spawn_local, LocalExecutorBuilder, Placement,
};

pub fn test_on_shard<G, F, T>(test_future: G) -> Result<()>
where
    G: FnOnce(Rc<MyShard>) -> F + Send + 'static,
    F: Future<Output = T> + 'static,
    T: Send + 'static,
{
    let builder = LocalExecutorBuilder::new(Placement::Fixed(1));
    let handle = builder.name("test").spawn(|| async move {
        let args = parse_args_from([""]);
        let id = 0;
        let shard = create_shard(args, id, vec![LocalShardConnection::new(id)]);
        let start_event_receiver =
            shard.subscribe_to_flow_event(FlowEvent::StartTasks.into());
        let shard_run_handle =
            spawn_local(enclose!((shard.clone() => shard) async move {
                run_shard(shard, false).await
            }));
        start_event_receiver.recv().await.unwrap();

        // Test start
        test_future(shard).await;
        // Test end

        shard_run_handle.cancel().await;
    })?;
    handle.join()?;
    Ok(())
}
