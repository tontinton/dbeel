use std::rc::Rc;

use dbeel::{
    args::Args,
    error::Result,
    flow_events::FlowEvent,
    local_shard::LocalShardConnection,
    run_shard::{create_shard, run_shard},
    shards::MyShard,
};

use futures::future::try_join_all;
use futures_lite::Future;
use glommio::{
    enclose, spawn_local, ExecutorJoinHandle, LocalExecutorBuilder, Placement,
};
use pretty_env_logger::formatted_timed_builder;

pub fn install_logger() {
    let mut log_builder = formatted_timed_builder();
    log_builder.parse_filters("trace");
    log_builder.try_init().unwrap();
}

pub fn test_shard<G, F, T>(args: Args, test_future: G) -> Result<()>
where
    G: FnOnce(Rc<MyShard>) -> F + Send + 'static,
    F: Future<Output = T> + 'static,
    T: Send + 'static,
{
    let builder = LocalExecutorBuilder::new(Placement::Unbound);
    let handle = builder.name("test").spawn(|| async move {
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

pub fn test_node<G, F, T>(
    number_of_shards: usize,
    args: Args,
    test_future: G,
) -> Result<ExecutorJoinHandle<()>>
where
    G: FnOnce(Rc<MyShard>, Vec<Rc<MyShard>>) -> F + Send + 'static,
    F: Future<Output = T> + 'static,
    T: Send + 'static,
{
    assert!(number_of_shards >= 1);

    let builder = LocalExecutorBuilder::new(Placement::Unbound);
    let handle = builder.name("test").spawn(move || async move {
        let local_connections = (0..number_of_shards)
            .into_iter()
            .map(LocalShardConnection::new)
            .collect::<Vec<_>>();

        let mut shards = (0..number_of_shards)
            .into_iter()
            .map(|id| create_shard(args.clone(), id, local_connections.clone()))
            .rev()
            .collect::<Vec<_>>();

        let start_event_receivers = shards
            .iter()
            .map(|s| s.subscribe_to_flow_event(FlowEvent::StartTasks.into()))
            .collect::<Vec<_>>();

        let node_shard = shards.pop().unwrap();
        let shard_run_handle =
            spawn_local(enclose!((node_shard.clone() => node_shard,
                                  shards.clone() => shards) async move {
                let mut futures = Vec::with_capacity(shards.len() + 1);
                futures.push(run_shard(node_shard, true));
                futures.extend(shards
                    .into_iter()
                    .map(|shard| run_shard(shard, false)));
                try_join_all(futures).await.unwrap();
            }));
        try_join_all(
            start_event_receivers.iter().map(|receiver| receiver.recv()),
        )
        .await
        .unwrap();

        // Test start
        test_future(node_shard, shards).await;
        // Test end

        shard_run_handle.cancel().await;
    })?;

    Ok(handle)
}
