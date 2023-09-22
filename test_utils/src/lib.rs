use std::rc::Rc;

use dbeel::{
    args::Args,
    error::Result,
    flow_events::FlowEvent,
    local_shard::LocalShardConnection,
    run_shard::{create_shard, run_shard},
    shards::MyShard,
};

use async_channel::Receiver;
use futures::future::try_join_all;
use futures_lite::Future;
use glommio::{
    enclose, spawn_local, ExecutorJoinHandle, LocalExecutorBuilder, Placement,
};
use pretty_env_logger::formatted_timed_builder;

pub fn install_logger() {
    let mut log_builder = formatted_timed_builder();
    log_builder.parse_filters("trace,glommio=info");
    log_builder.try_init().unwrap();
}

pub fn subscribe_to_flow_events(
    shards: &[Rc<MyShard>],
    event: FlowEvent,
) -> Vec<Receiver<()>> {
    let event_id = event.into();
    shards
        .iter()
        .map(|s| s.subscribe_to_flow_event(event_id))
        .collect::<Vec<_>>()
}

pub async fn wait_for_flow_events(receivers: Vec<Receiver<()>>) -> Result<()> {
    try_join_all(receivers.iter().map(|receiver| receiver.recv())).await?;
    Ok(())
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
                run_shard(shard, false).await.unwrap();
            }));
        start_event_receiver.recv().await.unwrap();

        // Test start
        test_future(shard.clone()).await;
        // Test end

        shard.stop().await.unwrap();
        shard_run_handle.await;
    })?;
    handle.join()?;
    Ok(())
}

pub fn test_node_ex<G, F, T>(
    number_of_shards: u16,
    args: Args,
    crash_at_shutdown: bool,
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
            .map(LocalShardConnection::new)
            .collect::<Vec<_>>();

        let mut shards = (0..number_of_shards)
            .map(|id| create_shard(args.clone(), id, local_connections.clone()))
            .rev()
            .collect::<Vec<_>>();

        let start_event_receivers =
            subscribe_to_flow_events(&shards, FlowEvent::StartTasks);

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
        wait_for_flow_events(start_event_receivers).await.unwrap();

        // Test start
        test_future(node_shard.clone(), shards.clone()).await;
        // Test end

        if crash_at_shutdown {
            shard_run_handle.cancel().await;
        } else {
            let mut futures =
                shards.iter().map(|s| s.stop()).collect::<Vec<_>>();
            futures.push(node_shard.stop());
            try_join_all(futures).await.unwrap();
            shard_run_handle.await;
        }
    })?;

    Ok(handle)
}

pub fn test_node<G, F, T>(
    number_of_shards: u16,
    args: Args,
    test_future: G,
) -> Result<ExecutorJoinHandle<()>>
where
    G: FnOnce(Rc<MyShard>, Vec<Rc<MyShard>>) -> F + Send + 'static,
    F: Future<Output = T> + 'static,
    T: Send + 'static,
{
    test_node_ex(number_of_shards, args, false, test_future)
}

pub fn test_node_with_crash_at_end<G, F, T>(
    number_of_shards: u16,
    args: Args,
    test_future: G,
) -> Result<ExecutorJoinHandle<()>>
where
    G: FnOnce(Rc<MyShard>, Vec<Rc<MyShard>>) -> F + Send + 'static,
    F: Future<Output = T> + 'static,
    T: Send + 'static,
{
    test_node_ex(number_of_shards, args, true, test_future)
}

pub fn next_node_args(
    mut args: Args,
    name: String,
    number_of_shards: u16,
) -> Args {
    args.remote_shard_port += number_of_shards;
    args.port += number_of_shards;
    args.gossip_port += number_of_shards;
    args.name = name;
    args
}
