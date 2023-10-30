use crate::{
    args::Args,
    error::{Error, Result},
    gossip::GossipEvent,
    local_shard::LocalShardConnection,
    messages::NodeMetadata,
    notify_flow_event,
    remote_shard_connection::RemoteShardConnection,
    shards::{MyShard, Shard, ShardConnection},
    storage_engine::page_cache::{PageCache, PAGE_SIZE},
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

async fn get_collections(
    seed_shards: &[RemoteShardConnection],
) -> Option<Vec<(String, u16)>> {
    for c in seed_shards {
        match c.get_collections().await {
            Ok(collections) => return Some(collections),
            Err(e) => {
                error!("Failed to get collections from '{}': {}", c.address, e);
            }
        }
    }

    None
}

async fn discover_collections(
    my_shard: &MyShard,
    seed_shards: &[RemoteShardConnection],
) -> Result<()> {
    for (collection, metadata) in my_shard.get_collections_from_disk().await? {
        my_shard
            .create_collection(collection, metadata.replication_factor)
            .await?;
    }

    if let Some(collections) = get_collections(seed_shards).await {
        for (collection, replication_factor) in collections {
            if !my_shard.collections.borrow().contains_key(&collection) {
                my_shard
                    .create_collection(collection, replication_factor)
                    .await?;
            }
        }
    }

    Ok(())
}

async fn get_nodes_metadata(
    seed_shards: &[RemoteShardConnection],
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

async fn discover_nodes(
    my_shard: &MyShard,
    seed_shards: &[RemoteShardConnection],
) -> Result<()> {
    if my_shard.args.seed_nodes.is_empty() {
        return Ok(());
    }

    let nodes = get_nodes_metadata(seed_shards)
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

    let remote_shard_connections = &my_shard
        .args
        .seed_nodes
        .iter()
        .map(|seed_node| {
            RemoteShardConnection::from_args(seed_node.clone(), &my_shard.args)
        })
        .collect::<Vec<_>>();
    discover_collections(&my_shard, remote_shard_connections).await?;
    discover_nodes(&my_shard, remote_shard_connections).await?;

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
            .clone()
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
            .clone()
            .gossip(GossipEvent::Dead(my_shard.args.name.clone()))
            .await?;
    }

    info!("Exiting shard of id: {}", my_shard.id);

    if !matches!(result, Err(Error::ShardStopped)) {
        result?;
    }

    Ok(())
}

#[must_use]
pub fn create_shard(
    args: Args,
    id: u16,
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
            let shard_name = format!("{}-{}", args.name, c.id);
            Shard::new(args.name.clone(), shard_name, ShardConnection::Local(c))
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
