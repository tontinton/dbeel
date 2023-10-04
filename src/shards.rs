use std::collections::HashSet;
use std::{cell::RefCell, collections::HashMap, path::PathBuf, rc::Rc};

use async_channel::{Receiver, Sender};
use bincode::Options;
use event_listener::Event;
use futures::{
    future::join_all,
    stream::{FuturesUnordered, StreamExt},
};
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::io::StreamWriterBuilder;
use glommio::{
    io::{BufferedFile, StreamReaderBuilder},
    net::UdpSocket,
    spawn_local,
};
use itertools::Itertools;
use log::{error, trace};
use murmur3::murmur3_32;
use rand::seq::IteratorRandom;
use rand::thread_rng;
use regex::Regex;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::gossip::{
    serialize_gossip_message, GossipEvent, GossipEventKind, GossipMessage,
};
use crate::messages::{NodeMetadata, ShardRequest, ShardResponse};
use crate::tasks::migration::{
    spawn_migration_actions_tasks, MigrationAction, RangeAndAction,
};
use crate::utils::{bincode::bincode_options, get_first_capture};
use crate::{
    args::Args,
    error::{Error, Result},
    local_shard::LocalShardConnection,
    messages::{ShardEvent, ShardMessage, ShardPacket},
    remote_shard_connection::RemoteShardConnection,
    storage_engine::{
        cached_file_reader::FileId,
        lsm_tree::LSMTree,
        page_cache::{PageCache, PartitionPageCache},
    },
};

#[cfg(feature = "flow-events")]
use crate::flow_events::{FlowEvent, FlowEventKind};

#[macro_export]
macro_rules! notify_flow_event {
    ($self:expr, $flow_event:expr) => {
        #[cfg(feature = "flow-events")]
        $self.notify_flow_event($flow_event.kind()).await;
    };
}

#[derive(Serialize, Deserialize)]
pub struct ClusterMetadata {
    pub nodes: Vec<NodeMetadata>,
    pub collections: HashMap<String, u16>,
}

#[derive(Debug, Clone)]
pub enum ShardConnection {
    Local(LocalShardConnection),
    Remote(RemoteShardConnection),
}

// A struct to hold shard metadata + connection info to communicate with.
#[derive(Debug, Clone)]
pub struct Shard {
    // The unique node name.
    pub node_name: String,

    // The unique shard name.
    pub name: String,

    // The hash of the unique name, used for consistent hashing.
    pub hash: u32,

    // Communicate with a shard by abstracting away if it's remote or local.
    pub connection: ShardConnection,
}

pub fn hash_string(s: &str) -> std::io::Result<u32> {
    murmur3_32(&mut std::io::Cursor::new(s), 0)
}

pub fn hash_bytes(bytes: &[u8]) -> std::io::Result<u32> {
    murmur3_32(&mut std::io::Cursor::new(bytes), 0)
}

fn is_between(item: u32, start: u32, end: u32) -> bool {
    if end < start {
        item >= end || item < start
    } else {
        item >= start && item < end
    }
}

impl Shard {
    pub fn new(
        node_name: String,
        name: String,
        connection: ShardConnection,
    ) -> Self {
        let hash = hash_string(&name).unwrap();
        Self {
            node_name,
            name,
            hash,
            connection,
        }
    }
}

/// The metadata of a collection (saved to disk for each collection).
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct CollectionMetadata {
    /// Number of nodes (replicas) that hold a copy for a specific key for
    /// tunable availability / consistency.
    pub replication_factor: u16,
}

#[derive(Clone)]
pub struct Collection {
    /// The K/V store of the collection.
    pub tree: Rc<LSMTree>,

    /// The metadata of a collection.
    pub metadata: CollectionMetadata,
}

impl Collection {
    fn new(tree: LSMTree, metadata: CollectionMetadata) -> Self {
        Self {
            tree: Rc::new(tree),
            metadata,
        }
    }
}

/// Holder of state to the shard running on the current thread.
/// To share the state between all coroutines, async methods get Rc<Self>.
pub struct MyShard {
    /// Input config from the user.
    pub args: Args,

    /// Current shard's cpu id.
    pub id: u16,

    /// Shard unique name, if you want the node unique name, it's in args.name.
    pub shard_name: String,

    /// The shard's hash.
    pub hash: u32,

    /// The consistent hash ring (shards sorted by hash).
    /// Starts with the first hash that has a greater hash than our shard.
    pub shards: RefCell<Vec<Shard>>,

    /// All known nodes other than this node, key is node unique name.
    pub nodes: RefCell<HashMap<String, NodeMetadata>>,

    /// Holds the counts of gossip requests.
    pub gossip_requests: RefCell<HashMap<(String, GossipEventKind), u8>>,

    /// Collections to the lsm tree on disk.
    pub collections: RefCell<HashMap<String, Collection>>,

    /// Used for notfying any insertions / removals from |collections|.
    pub collections_change_event: Event,

    /// The shard's page cache.
    cache: Rc<RefCell<PageCache<FileId>>>,

    /// The packet receiver from other local shards.
    pub local_shards_packet_receiver: Receiver<ShardPacket>,

    /// An event receiver to stop the shard from running.
    pub stop_receiver: Receiver<()>,

    /// An event sender to stop the shard from running.
    stop_sender: Sender<()>,

    /// All registered flow event listeners (key is flow event type).
    #[cfg(feature = "flow-events")]
    flow_event_listeners: RefCell<HashMap<FlowEventKind, Vec<Sender<()>>>>,
}

impl MyShard {
    pub fn new(
        args: Args,
        id: u16,
        shards: Vec<Shard>,
        cache: PageCache<FileId>,
        local_shards_packet_receiver: Receiver<ShardPacket>,
        stop_receiver: Receiver<()>,
        stop_sender: Sender<()>,
    ) -> Self {
        let shard_name = format!("{}-{}", args.name, id);
        let hash = hash_string(&shard_name).unwrap();
        Self {
            args,
            id,
            shard_name,
            hash,
            shards: RefCell::new(shards),
            nodes: RefCell::new(HashMap::new()),
            gossip_requests: RefCell::new(HashMap::new()),
            collections: RefCell::new(HashMap::new()),
            collections_change_event: Event::new(),
            cache: Rc::new(RefCell::new(cache)),
            local_shards_packet_receiver,
            stop_receiver,
            stop_sender,
            #[cfg(feature = "flow-events")]
            flow_event_listeners: RefCell::new(HashMap::new()),
        }
    }

    pub async fn stop(&self) -> Result<()> {
        self.stop_sender.send(()).await?;
        Ok(())
    }

    pub fn get_collection(&self, name: &str) -> Result<Collection> {
        self.collections
            .borrow()
            .get(name)
            .ok_or_else(|| Error::CollectionNotFound(name.to_string()))
            .cloned()
    }

    pub fn get_collection_tree(&self, name: &str) -> Result<Rc<LSMTree>> {
        self.collections
            .borrow()
            .get(name)
            .ok_or_else(|| Error::CollectionNotFound(name.to_string()))
            .map(|c| c.tree.clone())
    }

    fn get_collection_metadata_path(&self, name: &str) -> PathBuf {
        let mut dir = PathBuf::from(self.args.dir.clone());
        dir.push(format!("{}.metadata", name));
        dir
    }

    pub async fn get_collections_from_disk(
        &self,
    ) -> Result<Vec<(String, CollectionMetadata)>> {
        if !std::fs::metadata(&self.args.dir)
            .map(|m| m.is_dir())
            .unwrap_or(false)
        {
            return Ok(Vec::new());
        }

        let pattern = format!(r#"(.*?)\-{}$"#, self.id);
        let regex = Regex::new(pattern.as_str())
            .map_err(|source| Error::RegexCreationError { source, pattern })?;
        let names = std::fs::read_dir(&self.args.dir)?
            .filter_map(std::result::Result::ok)
            .filter_map(|entry| get_first_capture(&regex, &entry))
            .collect::<Vec<_>>();

        let mut collections = Vec::with_capacity(names.len());

        let metadata_size = bincode_options()
            .serialized_size(&CollectionMetadata::default())?
            as usize;

        let mut buf = vec![0; metadata_size];
        for name in names {
            match BufferedFile::open(self.get_collection_metadata_path(&name))
                .await
            {
                Ok(file) => {
                    let mut reader = StreamReaderBuilder::new(file).build();
                    let read = reader.read(&mut buf).await?;
                    assert_eq!(metadata_size, read);
                    reader.close().await?;

                    let metadata =
                        bincode_options().deserialize_from(&mut &buf[..])?;
                    collections.push((name, metadata));
                }
                Err(e) => panic!(
                    "Collection '{}' failed to open metadata file on disk: {}",
                    name, e
                ),
            }
        }

        Ok(collections)
    }

    fn get_collection_dir(&self, name: &str) -> PathBuf {
        let mut dir = PathBuf::from(self.args.dir.clone());
        dir.push(format!("{}-{}", name, self.id));
        dir
    }

    async fn create_lsm_tree(&self, name: String) -> Result<LSMTree> {
        let cache = self.cache.clone();
        LSMTree::open_or_create(
            self.get_collection_dir(&name),
            PartitionPageCache::new(name, cache),
        )
        .await
    }

    pub async fn create_collection(
        &self,
        name: String,
        replication_factor: u16,
    ) -> Result<()> {
        if self.collections.borrow().contains_key(&name) {
            return Err(Error::CollectionAlreadyExists(name));
        }
        let tree = self.create_lsm_tree(name.clone()).await?;
        let metadata = CollectionMetadata { replication_factor };

        let path = self.get_collection_metadata_path(&name);
        let mut writer =
            StreamWriterBuilder::new(BufferedFile::create(path).await?).build();
        writer
            .write_all(&bincode_options().serialize(&metadata)?)
            .await?;
        writer.close().await?;

        self.collections
            .borrow_mut()
            .insert(name, Collection::new(tree, metadata));
        self.collections_change_event.notify(usize::MAX);

        notify_flow_event!(self, FlowEvent::CollectionCreated);

        Ok(())
    }

    pub fn drop_collection(&self, name: &str) -> Result<()> {
        let _ = std::fs::remove_file(self.get_collection_metadata_path(name));

        self.collections
            .borrow_mut()
            .remove(name)
            .ok_or_else(|| Error::CollectionNotFound(name.to_string()))?
            .tree
            .purge()?;
        self.collections_change_event.notify(usize::MAX);

        Ok(())
    }

    pub fn try_to_stop_local_shards(&self) {
        let senders = self
            .shards
            .borrow()
            .iter()
            .flat_map(|p| match &p.connection {
                ShardConnection::Local(c) => Some(c.stop_sender.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();

        for sender in senders {
            let _ = sender.try_send(());
        }
    }

    pub async fn broadcast_message_to_local_shards(
        self: Rc<Self>,
        message: &ShardMessage,
    ) -> Result<()> {
        let senders = self
            .shards
            .borrow()
            .iter()
            .flat_map(|p| match &p.connection {
                ShardConnection::Local(c) => Some(c.sender.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();

        let results = join_all(
            senders
                .iter()
                .map(|s| s.send(ShardPacket::new(self.id, message.clone()))),
        )
        .await;

        for result in results {
            result?;
        }

        Ok(())
    }

    pub async fn send_request_to_replicas<F, T>(
        self: Rc<Self>,
        request: ShardRequest,
        number_of_acks: usize,
        number_of_nodes: usize,
        response_map_fn: F,
    ) -> Result<Vec<T>>
    where
        F: Fn(ShardResponse) -> Result<T> + 'static,
        T: 'static,
    {
        let (sender, receiver) = async_channel::bounded(1);
        let my_shard = self.clone();
        spawn_local(async move {
            // Filter out remote shards of nodes we already collected.
            let mut nodes =
                HashSet::with_capacity(number_of_nodes);

            let connections = my_shard.shards.borrow()
                .iter()
                .flat_map(|p| match &p.connection {
                    ShardConnection::Remote(c)
                        if !nodes.contains(&p.node_name) =>
                    {
                        nodes.insert(&p.node_name);
                        Some(c)
                    }
                    _ => None,
                })
                .take(number_of_nodes)
                .cloned()
                .collect::<Vec<_>>();

            let mut futures = connections.iter()
                .map(|c| c.send_request(request.clone()))
                .collect::<FuturesUnordered<_>>();

            let mut results = Vec::with_capacity(number_of_acks);

            if number_of_acks > 0 {
                let mut acks = 0;
                while let Some(result) = futures.next().await {
                    match result {
                        Ok(response) => {
                            let response = response_map_fn(response);
                            if let Err(e) = response {
                                error!("Failed response from replica: {}", e);
                                continue;
                            }

                            results.push(response.unwrap());
                            acks += 1;
                            if acks >= number_of_acks {
                                break;
                            }
                        }
                        Err(e) => {
                            error!("Failed to send request to replica: {}", e)
                        }
                    }
                }
            }

            if let Err(e) = sender.send(results).await {
                error!(
                    "Failed to notify that {} replicas responded with an ack: {}",
                    number_of_acks, e
                );
            }

            // Run remaining futures in the background.
            while let Some(result) = futures.next().await {
                if let Err(e) = result {
                    error!("Failed to send request to replica in background: {}", e);
                }
            }
        })
        .detach();

        Ok(receiver.recv().await?)
    }

    async fn handle_shard_event(
        self: Rc<Self>,
        event: ShardEvent,
    ) -> Result<()> {
        // All events should be idempotent.
        match event {
            ShardEvent::Gossip(event) => {
                self.handle_gossip_event(event).await?;
            }
            ShardEvent::Set(collection, key, value, timestamp) => {
                self.handle_shard_set_message(
                    collection, key, value, timestamp,
                )
                .await?;
            }
        };

        Ok(())
    }

    pub fn get_node_metadata(&self) -> NodeMetadata {
        let ids = self
            .shards
            .borrow()
            .iter()
            .flat_map(|shard| match &shard.connection {
                ShardConnection::Remote(_) => None,
                ShardConnection::Local(c) => Some(c.id),
            })
            .collect::<Vec<_>>();

        NodeMetadata {
            name: self.args.name.clone(),
            ip: self.args.ip.clone(),
            remote_shard_base_port: self.args.remote_shard_port,
            ids,
            gossip_port: self.args.gossip_port,
            db_port: self.args.port,
        }
    }

    pub fn add_shards_of_nodes(&self, nodes: Vec<NodeMetadata>) {
        self.shards.borrow_mut().extend(
            nodes
                .into_iter()
                .flat_map(|node| {
                    node.ids
                        .into_iter()
                        .map(|id| {
                            (
                                node.name.clone(),
                                format!(
                                    "{}:{}",
                                    node.ip,
                                    node.remote_shard_base_port + id
                                ),
                                id,
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .map(|(node_name, address, id)| {
                    let shard_name = format!("{}-{}", node_name, id);
                    Shard::new(
                        node_name,
                        shard_name,
                        ShardConnection::Remote(
                            RemoteShardConnection::from_args(
                                address, &self.args,
                            ),
                        ),
                    )
                }),
        );

        self.sort_consistent_hash_ring();
    }

    fn sort_consistent_hash_ring(&self) {
        self.shards.borrow_mut().sort_unstable_by(|a, b| {
            let x = a.hash;
            let y = b.hash;
            let threshold = self.hash;
            if x < threshold && y >= threshold {
                std::cmp::Ordering::Greater
            } else if x >= threshold && y < threshold {
                std::cmp::Ordering::Less
            } else {
                x.cmp(&y)
            }
        });
    }

    pub fn get_nodes(&self) -> Vec<NodeMetadata> {
        let mut nodes = self
            .nodes
            .borrow()
            .iter()
            .map(|(_, n)| n.clone())
            .collect::<Vec<_>>();
        nodes.push(self.get_node_metadata());
        nodes
    }

    pub fn get_cluster_metadata(&self) -> ClusterMetadata {
        ClusterMetadata {
            nodes: self.get_nodes(),
            collections: self
                .collections
                .borrow()
                .iter()
                .map(|(k, v)| (k.clone(), v.metadata.replication_factor))
                .collect(),
        }
    }

    async fn handle_shard_request(
        &self,
        request: ShardRequest,
    ) -> Result<ShardResponse> {
        let response = match request {
            ShardRequest::Ping => ShardResponse::Pong,
            ShardRequest::GetMetadata => {
                ShardResponse::GetMetadata(self.get_nodes())
            }
            ShardRequest::GetCollections => ShardResponse::GetCollections(
                self.collections
                    .borrow()
                    .iter()
                    .map(|(n, c)| (n.clone(), c.metadata.replication_factor))
                    .collect::<Vec<_>>(),
            ),
            ShardRequest::Set(collection, key, value, timestamp) => {
                self.handle_shard_set_message(
                    collection, key, value, timestamp,
                )
                .await?;
                ShardResponse::Set
            }
            ShardRequest::Delete(collection, key, timestamp) => {
                let existing_tree = self
                    .collections
                    .borrow()
                    .get(&collection)
                    .map(|c| c.tree.clone());
                if let Some(tree) = existing_tree {
                    tree.delete_with_timestamp(key, timestamp).await?;
                };
                ShardResponse::Delete
            }
            ShardRequest::Get(collection, key) => {
                let existing_tree = self
                    .collections
                    .borrow()
                    .get(&collection)
                    .map(|c| c.tree.clone());
                let value = if let Some(tree) = existing_tree {
                    tree.get_entry(&key).await?
                } else {
                    None
                };
                ShardResponse::Get(value)
            }
        };

        Ok(response)
    }

    async fn handle_shard_set_message(
        &self,
        collection: String,
        key: Vec<u8>,
        value: Vec<u8>,
        timestamp: OffsetDateTime,
    ) -> Result<()> {
        let tree = self
            .collections
            .borrow()
            .get(&collection)
            .map(|c| c.tree.clone())
            .ok_or_else(|| Error::CollectionNotFound(collection))?;
        tree.set_with_timestamp(key, value, timestamp).await?;

        notify_flow_event!(self, FlowEvent::ItemSetFromShardMessage);

        Ok(())
    }

    pub async fn handle_shard_message(
        self: Rc<Self>,
        message: ShardMessage,
    ) -> Result<Option<ShardResponse>> {
        Ok(match message {
            ShardMessage::Event(event) => {
                self.handle_shard_event(event).await?;
                None
            }
            ShardMessage::Request(request) => {
                Some(self.handle_shard_request(request).await?)
            }
            ShardMessage::Response(_) => None,
        })
    }

    pub async fn gossip(self: Rc<Self>, event: GossipEvent) -> Result<()> {
        self.clone()
            .broadcast_message_to_local_shards(&ShardMessage::Event(
                ShardEvent::Gossip(event.clone()),
            ))
            .await?;

        let message = GossipMessage::new(self.args.name.clone(), event);
        self.gossip_buffer(&serialize_gossip_message(&message)?)
            .await
    }

    pub async fn gossip_buffer(&self, message_buffer: &[u8]) -> Result<()> {
        let mut rng = thread_rng();
        let addresses = self
            .nodes
            .borrow()
            .iter()
            .choose_multiple(&mut rng, self.args.gossip_fanout)
            .into_iter()
            .map(|(_, node)| format!("{}:{}", node.ip, node.gossip_port))
            .collect::<Vec<_>>();

        let sockets = (0..addresses.len())
            .map(|_| {
                UdpSocket::bind("127.0.0.1:0").map_err(Error::GlommioError)
            })
            .collect::<Result<Vec<_>>>()?;

        let futures = addresses
            .into_iter()
            .zip(sockets.iter())
            .map(|(address, socket)| socket.send_to(message_buffer, address));

        join_all(futures).await;

        Ok(())
    }

    pub async fn handle_dead_node(self: Rc<Self>, node_name: &str) {
        if self.nodes.borrow_mut().remove(node_name).is_none() {
            return;
        }

        let (removed, kept): (Vec<_>, Vec<_>) = self
            .shards
            .replace(Vec::new())
            .into_iter()
            .partition(|shard| shard.node_name == node_name);
        self.shards.replace(kept);

        trace!(
            "After death of {}: holding {} nodes and {} shards",
            node_name,
            self.nodes.borrow().len(),
            self.shards.borrow().len(),
        );

        notify_flow_event!(self, FlowEvent::DeadNodeRemoved);

        self.migrate_data_on_node_removal(&removed).await;
    }

    async fn migrate_data_on_node_removal(
        self: Rc<Self>,
        removed_shards: &[Shard],
    ) {
        assert!(!removed_shards.is_empty());

        let mut migration_actions = Vec::new();

        for (collection_name, collection) in self
            .collections
            .borrow()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
        {
            let replications = collection.metadata.replication_factor as usize;

            // No need to migrate data if no replication set.
            if replications <= 1 {
                return;
            }

            // No need to migrate data if all nodes already contain all the data.
            if self.nodes.borrow().len() + 1 < replications {
                return;
            }

            let (migrate_to_hash, migrate_to_connection) = {
                let shards = self.shards.borrow();
                let migrate_to_shard = Self::get_last_owning_shard(
                    &shards,
                    self.hash,
                    replications,
                )
                .unwrap();
                (migrate_to_shard.hash, migrate_to_shard.connection.clone())
            };

            // No need to migrate if no removed shards in the ring between the
            // current shard and the last owning shard.
            if !removed_shards
                .iter()
                .map(|s| s.hash)
                .any(|h| is_between(h, self.hash, migrate_to_hash))
            {
                return;
            }

            // The previous shard in the hash ring is the last one in the vector.
            let start =
                self.shards.borrow()[self.shards.borrow().len() - 1].hash;

            // The closest removed shard that is between the previous shard and
            // this shard, or - this shard if no removed shard found between.
            let end = removed_shards
                .iter()
                .map(|s| s.hash)
                .filter(|h| is_between(*h, start, self.hash))
                .min_by_key(|h| self.hash.wrapping_sub(*h))
                .unwrap_or(self.hash);

            migration_actions.push((
                collection_name,
                vec![RangeAndAction::new(
                    start,
                    end,
                    MigrationAction::SendToShard(migrate_to_connection),
                )],
            ));
        }

        spawn_migration_actions_tasks(self, migration_actions);
    }

    fn migrate_data_on_node_addition(self: Rc<Self>, added_shards: &[Shard]) {
        assert!(!added_shards.is_empty());

        let mut migration_actions = Vec::new();

        for (collection_name, collection) in self
            .collections
            .borrow()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
        {
            let replications = collection.metadata.replication_factor as usize;

            // No need to migrate data if no replication set.
            if replications <= 1 {
                continue;
            }

            // No need to migrate data if all nodes already contain all the data.
            if self.nodes.borrow().len() + 1 < replications {
                continue;
            }

            let mut collection_migration_actions = Vec::new();

            let shards = self.shards.borrow();
            let last_owning_shard =
                Self::get_last_owning_shard(&shards, self.hash, replications)
                    .unwrap();

            let added_shard_names = added_shards
                .iter()
                .map(|s| s.name.clone())
                .collect::<Vec<_>>();

            // Get the previous shard's hash (prior to adding the new node's
            // shards to the ring).
            let previous_shard_hash = self
                .shards
                .borrow()
                .iter()
                .rev()
                .filter(|s| !added_shard_names.contains(&s.name))
                .map(|s| s.hash)
                .next();
            if previous_shard_hash.is_none() {
                return;
            }
            let previous_shard_hash = previous_shard_hash.unwrap();

            // Step 1.
            // Migrate to the closest added shard that is between this shard and the
            // last owning one.
            if let Some(migrate_to_shard) = added_shards
                .iter()
                .filter(|shard| {
                    is_between(shard.hash, self.hash, last_owning_shard.hash)
                        || shard.hash == last_owning_shard.hash
                })
                .min_by_key(|shard| shard.hash.wrapping_sub(self.hash))
            {
                collection_migration_actions.push(RangeAndAction::new(
                    previous_shard_hash,
                    self.hash,
                    MigrationAction::SendToShard(
                        migrate_to_shard.connection.clone(),
                    ),
                ));
            }

            // Step 2.
            // Migrate to added shards between the previous shard and this shard,
            // excluding the farthest one, because the previous shard should
            // migrate to it (see step 1).
            {
                let mut migrate_to_shards = added_shards
                    .iter()
                    .filter(|s| {
                        is_between(s.hash, previous_shard_hash, self.hash)
                    })
                    .collect::<Vec<_>>();

                if migrate_to_shards.len() > 1 {
                    migrate_to_shards.sort_unstable_by_key(|s| {
                        s.hash.wrapping_sub(self.hash)
                    });

                    let actions = migrate_to_shards
                        .into_iter()
                        .tuple_windows()
                        .map(|(a, b)| {
                            RangeAndAction::new(
                                a.hash,
                                b.hash,
                                MigrationAction::SendToShard(
                                    b.connection.clone(),
                                ),
                            )
                        });
                    collection_migration_actions.extend(actions);
                }
            }

            // Step 3.
            // Delete items that are no longer owned by this shard. It's ok to
            // delete while migrating, because this shard is not the first owner of
            // these items.
            let mut seen = HashSet::with_capacity(replications);
            for (i, shard) in
                self.shards.borrow().iter().enumerate().rev().filter(
                    |(_, shard)| !added_shard_names.contains(&shard.name),
                )
            {
                seen.insert(&shard.name);
                if seen.len() == replications {
                    break;
                }

                if !self.is_owning_shard(i, replications) {
                    let prev_index = if i == 0 {
                        self.shards.borrow().len() - 1
                    } else {
                        i - 1
                    };

                    collection_migration_actions.push(RangeAndAction::new(
                        self.shards.borrow()[prev_index].hash,
                        shard.hash,
                        MigrationAction::Delete,
                    ));
                }
            }

            if !collection_migration_actions.is_empty() {
                migration_actions
                    .push((collection_name, collection_migration_actions));
            }
        }

        // Step 4.
        // Execute migration actions on background tasks.
        spawn_migration_actions_tasks(self, migration_actions);
    }

    fn get_last_owning_shard(
        shards: &[Shard],
        start_shard_hash: u32,
        replication_factor: usize,
    ) -> Option<&Shard> {
        let start_shard_index = shards
            .iter()
            .position(|s| s.hash >= start_shard_hash)
            .unwrap_or(0);

        let mut owning_shards_found = 0;
        let mut nodes = HashSet::new();
        let mut i = 0;
        let mut index = start_shard_index % shards.len();
        while i == 0 || index != start_shard_index {
            let shard = &shards[index];
            if !nodes.contains(&shard.node_name) {
                owning_shards_found += 1;
                if owning_shards_found == replication_factor {
                    return Some(shard);
                }
                nodes.insert(&shard.node_name);
            }
            i += 1;
            index = (start_shard_index + i as usize) % shards.len();
        }
        None
    }

    fn is_owning_shard(
        &self,
        start_shard_index: usize,
        replication_factor: usize,
    ) -> bool {
        let shards = self.shards.borrow();
        let mut owning_shards_found = 0;
        let mut nodes = HashSet::new();
        let mut i = 0;
        let mut index = start_shard_index % shards.len();
        while i == 0 || index != start_shard_index {
            let shard = &shards[index];
            if !nodes.contains(&shard.node_name) {
                if shard.hash == self.hash {
                    return true;
                }
                owning_shards_found += 1;
                if owning_shards_found == replication_factor {
                    break;
                }
                nodes.insert(&shard.node_name);
            }
            i += 1;
            index = (start_shard_index + i as usize) % shards.len();
        }
        false
    }

    pub async fn handle_gossip_event(
        self: Rc<Self>,
        event: GossipEvent,
    ) -> Result<bool> {
        // All events must be handled idempotently, as gossip messages can be seen
        // multiple times.
        let another_gossip_sent = match event {
            GossipEvent::Alive(node) if node.name != self.args.name => {
                self.nodes
                    .borrow_mut()
                    .entry(node.name.clone())
                    .or_insert_with(|| node.clone());
                let node_name = node.name.clone();
                self.add_shards_of_nodes(vec![node]);
                trace!(
                    "After alive of {}: holding {} nodes and {} shards",
                    node_name,
                    self.nodes.borrow().len(),
                    self.shards.borrow().len(),
                );

                notify_flow_event!(self, FlowEvent::AliveNodeGossip);

                let added = self
                    .shards
                    .borrow()
                    .iter()
                    .filter(|s| s.node_name == node_name)
                    .cloned()
                    .collect::<Vec<_>>();
                self.migrate_data_on_node_addition(&added);

                false
            }
            GossipEvent::Dead(node_name) => {
                if node_name == self.args.name {
                    // Whoops, someone marked us as dead, even though we are alive
                    // and well.
                    // Let's notify everyone that we are actually alive.
                    self.clone()
                        .gossip(GossipEvent::Alive(self.get_node_metadata()))
                        .await?;
                    true
                } else {
                    self.handle_dead_node(&node_name).await;
                    false
                }
            }
            GossipEvent::CreateCollection(name, replication_factor) => {
                match self.create_collection(name, replication_factor).await {
                    Ok(()) | Err(Error::CollectionAlreadyExists(_)) => {}
                    Err(e) => {
                        return Err(e);
                    }
                };
                false
            }
            GossipEvent::DropCollection(name) => {
                match self.drop_collection(&name) {
                    Ok(()) | Err(Error::CollectionNotFound(_)) => {}
                    Err(e) => {
                        return Err(e);
                    }
                };
                false
            }
            _ => false,
        };

        Ok(!another_gossip_sent)
    }

    #[cfg(feature = "flow-events")]
    pub fn subscribe_to_flow_event(
        &self,
        event: FlowEventKind,
    ) -> Receiver<()> {
        let (sender, receiver) = async_channel::bounded(1);
        self.flow_event_listeners
            .borrow_mut()
            .entry(event)
            .or_insert(vec![])
            .push(sender);
        receiver
    }

    #[cfg(feature = "flow-events")]
    pub async fn notify_flow_event(&self, event: FlowEventKind) {
        let maybe_removed =
            self.flow_event_listeners.borrow_mut().remove(&event);
        if let Some(senders) = maybe_removed {
            join_all(senders.iter().map(|s| s.send(()))).await;
        }
    }
}
