use dbeel::{
    args::{get_args, Args},
    error::{Error, Result},
    gossip::GossipEvent,
    local_shard::LocalShardConnection,
    messages::{NodeMetadata, ShardRequest, ShardResponse},
    page_cache::{PageCache, PAGE_SIZE},
    remote_shard_connection::RemoteShardConnection,
    shards::{MyShard, OtherShard, ShardConnection},
    tasks::{
        compaction::spawn_compaction_task, db_server::spawn_db_server,
        gossip_server::spawn_gossip_server_task,
        local_shard_server::spawn_local_shard_server_task,
        remote_shard_server::spawn_remote_shard_server_task,
    },
};
use futures::future::try_join_all;
use glommio::{enclose, CpuSet, LocalExecutorBuilder, Placement};
use pretty_env_logger::formatted_timed_builder;
use std::rc::Rc;
use std::time::Duration;

extern crate pretty_env_logger;
#[macro_use]
extern crate log;

const DEFAULT_DEBUG_LOG_LEVEL: &str = "dbeel=trace";
const DEFAULT_RELEASE_LOG_LEVEL: &str = "dbeel=info";

async fn get_nodes_metadata(
    seed_shards: &Vec<RemoteShardConnection>,
) -> Option<Vec<NodeMetadata>> {
    for c in seed_shards {
        match c.send_request(ShardRequest::GetMetadata).await {
            Ok(ShardResponse::GetMetadata(metadata)) => return Some(metadata),
            Err(e) => {
                error!("Failed to get shards from '{}': {}", c.address, e);
            }
        }
    }

    None
}

async fn discover_nodes(my_shard: Rc<MyShard>) -> Result<()> {
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

async fn run_shard(
    args: Args,
    id: usize,
    local_connections: Vec<LocalShardConnection>,
) -> Result<()> {
    info!("Starting shard of id: {}", id);

    let receiver = local_connections
        .iter()
        .filter(|c| c.id == id)
        .map(|c| c.receiver.clone())
        .next()
        .unwrap();

    let shard_name = format!("{}-{}", args.name, id);
    let shards = local_connections
        .into_iter()
        .map(|c| OtherShard::new(shard_name.clone(), ShardConnection::Local(c)))
        .collect::<Vec<_>>();

    let cache_len = args.page_cache_size / PAGE_SIZE / shards.len();
    let cache = PageCache::new(cache_len, cache_len / 16)
        .map_err(|e| Error::CacheCreationError(e.to_string()))?;

    let my_shard = Rc::new(MyShard::new(args, id, shards, cache));

    // Start listening for other shards messages to be able to receive
    // responses to requests, for example receiving all remote shards from the
    // seed nodes.
    let remote_shard_server_task =
        spawn_remote_shard_server_task(my_shard.clone());

    discover_nodes(my_shard.clone()).await?;

    // Sort by hash to make it a consistant hashing ring.
    my_shard
        .shards
        .borrow_mut()
        .sort_unstable_by_key(|x| x.hash);

    let shard_messages_receiver_task =
        spawn_local_shard_server_task(my_shard.clone(), receiver);

    let compaction_task = spawn_compaction_task(my_shard.clone());

    let server_task = spawn_db_server(my_shard.clone());

    // Tasks that all shards run.
    let mut tasks = vec![
        remote_shard_server_task,
        shard_messages_receiver_task,
        compaction_task,
        server_task,
    ];

    // Tasks that only one shard of a node runs.
    if id == 0 {
        tasks.push(spawn_gossip_server_task(my_shard.clone()));

        // Notify all nodes that we are now alive.
        my_shard
            .clone()
            .gossip(GossipEvent::Alive(my_shard.get_node_metadata()))
            .await?;
    }

    // Await all, returns when first fails, cancels all others.
    try_join_all(tasks).await?;

    if id == 0 {
        // Notify all nodes that we are now dead.
        my_shard
            .clone()
            .gossip(GossipEvent::Dead(my_shard.args.name.clone()))
            .await?;
    }

    Ok(())
}

fn main() -> Result<()> {
    let default_log_level = if cfg!(debug_assertions) {
        DEFAULT_DEBUG_LOG_LEVEL
    } else {
        DEFAULT_RELEASE_LOG_LEVEL
    };

    let mut log_builder = formatted_timed_builder();
    log_builder.parse_filters(
        &std::env::var("RUST_LOG")
            .unwrap_or_else(|_| default_log_level.to_string()),
    );
    log_builder.try_init().unwrap();

    let args = get_args();

    let cpu_set = CpuSet::online()?;
    assert!(!cpu_set.is_empty());

    let local_connections = cpu_set
        .iter()
        .map(|x| x.cpu)
        .map(LocalShardConnection::new)
        .collect::<Vec<_>>();

    let handles = cpu_set
        .into_iter()
        .map(|x| x.cpu)
        .map(|cpu| {
            LocalExecutorBuilder::new(Placement::Fixed(cpu))
                .name(format!("executor({})", cpu).as_str())
                .spawn(enclose!((local_connections.clone() => connections,
                                args.clone() => args) move || async move {
                    run_shard(args, cpu, connections).await
                }))
                .map_err(Error::GlommioError)
        })
        .collect::<Result<Vec<_>>>()?;

    handles
        .into_iter()
        .map(|h| h.join().map_err(Error::GlommioError))
        .collect::<Result<Vec<_>>>()?;

    Ok(())
}
