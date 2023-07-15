use crate::{
    args::Args,
    error::{Error, Result},
    flow_events::FlowEvent,
    gossip::GossipEvent,
    local_shard::LocalShardConnection,
    messages::NodeMetadata,
    page_cache::{PageCache, PAGE_SIZE},
    remote_shard_connection::RemoteShardConnection,
    shards::{MyShard, OtherShard, ShardConnection},
    tasks::{
        compaction::spawn_compaction_task, db_server::spawn_db_server,
        failure_detector::spawn_failure_detector_task,
        gossip_server::spawn_gossip_server_task,
        local_shard_server::spawn_local_shard_server_task,
        remote_shard_server::spawn_remote_shard_server_task,
    },
};
use futures::future::try_join_all;
use log::{error, info, trace};
use std::rc::Rc;
use std::time::Duration;

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
                RemoteShardConnection::new(
                    seed_node.clone(),
                    Duration::from_millis(
                        my_shard.args.remote_shard_connect_timeout,
                    ),
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
        spawn_db_server(my_shard.clone()),
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

    my_shard
        .notify_flow_event(FlowEvent::StartTasks.into())
        .await;

    // Await all, returns when first fails, cancels all others.
    try_join_all(tasks).await?;

    if is_node_managing {
        // Notify all nodes that we are now dead.
        my_shard
            .gossip(GossipEvent::Dead(my_shard.args.name.clone()))
            .await?;
    }

    info!("Exiting shard of id: {}", my_shard.id);

    Ok(())
}

pub fn create_shard(
    args: Args,
    id: usize,
    local_connections: Vec<LocalShardConnection>,
) -> Rc<MyShard> {
    let receiver = local_connections
        .iter()
        .filter(|c| c.id == id)
        .map(|c| c.receiver.clone())
        .next()
        .unwrap();

    let shard_name = format!("{}-{}", args.name, id);
    let shards = local_connections
        .into_iter()
        .map(|c| {
            OtherShard::new(
                args.name.clone(),
                shard_name.clone(),
                ShardConnection::Local(c),
            )
        })
        .collect::<Vec<_>>();

    let cache_len = args.page_cache_size / PAGE_SIZE / shards.len();
    let cache = PageCache::new(cache_len, cache_len / 16);

    Rc::new(MyShard::new(args, id, shards, cache, receiver))
}
