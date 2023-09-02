use crate::{
    args::Args,
    error::{Error, Result},
    gossip::GossipEvent,
    local_shard::LocalShardConnection,
    messages::NodeMetadata,
    notify_flow_event,
    page_cache::{PageCache, PAGE_SIZE},
    remote_shard_connection::RemoteShardConnection,
    shards::{MyShard, Shard, ShardConnection},
    tasks::{
        compaction::spawn_compaction_task, db_server::spawn_db_server_task,
        failure_detector::spawn_failure_detector_task,
        gossip_server::spawn_gossip_server_task,
        local_shard_server::spawn_local_shard_server_task,
        remote_shard_server::spawn_remote_shard_server_task,
        stop_event_waiter::spawn_stop_event_waiter_task,
    },
};
use futures::future::try_join_all;
use log::{error, info, trace};
use std::rc::Rc;

#[cfg(feature = "flow-events")]
use crate::flow_events::FlowEvent;

async fn discover_collections(my_shard: &MyShard) -> Result<()> {
    for name in my_shard.get_collection_names_from_disk()? {
        my_shard.create_collection(name).await?;
    }
    Ok(())
}

async fn get_nodes_metadata(
    seed_shards: &Vec<RemoteShardConnection>,
) -> Option<Vec<NodeMetadata>> {
    for c in seed_shards {
        match c.get_metadata().await {
            Ok(metadata) => return Some(metadata),
            Err(e) => {
                error!("Failed to get shards from '{}': {}", c.address, e);
            }
        }
    }

    None
}

async fn discover_nodes(my_shard: &MyShard) -> Result<()> {
    if my_shard.args.seed_nodes.is_empty() {
        return Ok(());
    }

    let nodes = get_nodes_metadata(
        &my_shard
            .args
            .seed_nodes
            .iter()
            .map(|seed_node| {
                RemoteShardConnection::from_args(
                    seed_node.clone(),
                    &my_shard.args,
                )
            })
            .collect::<Vec<_>>(),
    )
    .await
    .ok_or(Error::NoRemoteShardsFoundInSeedNodes)?;

    my_shard.nodes.replace(
        nodes
            .iter()
            .filter(|n| n.name != my_shard.args.name)
            .map(|n| (n.name.clone(), n.clone()))
            .collect(),
    );

    trace!(
        "Got {} number of nodes in discovery",
        my_shard.nodes.borrow().len()
    );

    my_shard.add_shards_of_nodes(nodes);

    Ok(())
}

pub async fn run_shard(
    my_shard: Rc<MyShard>,
    is_node_managing: bool,
) -> Result<()> {
    info!("Starting shard of id: {}", my_shard.id);
    discover_collections(&my_shard).await?;
    discover_nodes(&my_shard).await?;

    // Tasks that all shards run.
    let mut tasks = vec![
        spawn_remote_shard_server_task(my_shard.clone()),
        spawn_local_shard_server_task(my_shard.clone()),
        spawn_compaction_task(my_shard.clone()),
        spawn_db_server_task(my_shard.clone()),
        spawn_stop_event_waiter_task(my_shard.clone()),
    ];

    // Tasks that only one shard of a node runs.
    if is_node_managing {
        tasks.push(spawn_gossip_server_task(my_shard.clone()));
        tasks.push(spawn_failure_detector_task(my_shard.clone()));

        // Notify all nodes that we are now alive.
        my_shard
            .gossip(GossipEvent::Alive(my_shard.get_node_metadata()))
            .await?;
    }

    notify_flow_event!(my_shard, FlowEvent::StartTasks);

    // Await all, returns when first fails, cancels all others.
    let result = try_join_all(tasks).await;

    if matches!(result, Err(Error::ShardStopped)) {
        trace!("Shard of id {} got stopped", my_shard.id);
    }

    my_shard.try_to_stop_local_shards();

    if is_node_managing {
        // Notify all nodes that we are now dead.
        my_shard
            .gossip(GossipEvent::Dead(my_shard.args.name.clone()))
            .await?;
    }

    info!("Exiting shard of id: {}", my_shard.id);

    if !matches!(result, Err(Error::ShardStopped)) {
        result?;
    }

    Ok(())
}

pub fn create_shard(
    args: Args,
    id: usize,
    local_connections: Vec<LocalShardConnection>,
) -> Rc<MyShard> {
    let (receiver, stop_receiver, stop_sender) = local_connections
        .iter()
        .filter(|c| c.id == id)
        .map(|c| {
            (
                c.receiver.clone(),
                c.stop_receiver.clone(),
                c.stop_sender.clone(),
            )
        })
        .next()
        .unwrap();

    let shards = local_connections
        .into_iter()
        .map(|c| {
            Shard::new(
                args.name.clone(),
                format!("{}-{}", args.name, c.id),
                ShardConnection::Local(c),
            )
        })
        .collect::<Vec<_>>();

    let cache_len = args.page_cache_size / PAGE_SIZE / shards.len();
    let cache = PageCache::new(cache_len, cache_len / 16);

    Rc::new(MyShard::new(
        args,
        id,
        shards,
        cache,
        receiver,
        stop_receiver,
        stop_sender,
    ))
}
