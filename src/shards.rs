use std::collections::HashSet;
use std::{cell::RefCell, collections::HashMap, path::PathBuf, rc::Rc};

use async_channel::{Receiver, Sender};
use futures::future::join_all;
use futures::stream::{FuturesUnordered, StreamExt};
use glommio::net::UdpSocket;
use glommio::spawn_local;
use log::{error, trace};
use murmur3::murmur3_32;
use rand::seq::IteratorRandom;
use rand::thread_rng;
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::gossip::{
    serialize_gossip_message, GossipEvent, GossipEventKind, GossipMessage,
};
use crate::messages::{NodeMetadata, ShardRequest, ShardResponse};
use crate::utils::get_first_capture;
use crate::{
    args::Args,
    cached_file_reader::FileId,
    error::{Error, Result},
    local_shard::LocalShardConnection,
    lsm_tree::LSMTree,
    messages::{ShardEvent, ShardMessage, ShardPacket},
    page_cache::{PageCache, PartitionPageCache},
    remote_shard_connection::RemoteShardConnection,
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
    pub replication_factor: u32,
}

#[derive(Debug)]
pub enum ShardConnection {
    Local(LocalShardConnection),
    Remote(RemoteShardConnection),
}

// A struct to hold other shards to communicate with.
#[derive(Debug)]
pub struct OtherShard {
    // The unique node name.
    pub node_name: String,

    // The unique shard name.
    pub name: String,

    // The hash of the unique name, used for consistent hashing.
    pub hash: u32,

    // Communicate with a shard by abstracting away if it's remote or local.
    pub connection: ShardConnection,
}

pub fn hash_string(s: &String) -> std::io::Result<u32> {
    murmur3_32(&mut std::io::Cursor::new(s), 0)
}

impl OtherShard {
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

/// Holder of state to the shard running on the current thread.
/// To share the state between all coroutines, async methods get Rc<Self>.
pub struct MyShard {
    /// Input config from the user.
    pub args: Args,

    /// Current shard's cpu id.
    pub id: usize,

    /// Shard unique name, if you want the node unique name, it's in args.name.
    pub shard_name: String,

    /// The shard's hash.
    pub hash: u32,

    /// The consistent hash ring (shards sorted by hash).
    /// Starts with the first hash that has a greater hash than our shard.
    pub shards: RefCell<Vec<OtherShard>>,

    /// All known nodes other than this node, key is node unique name.
    pub nodes: RefCell<HashMap<String, NodeMetadata>>,

    /// Holds the counts of gossip requests.
    pub gossip_requests: RefCell<HashMap<(String, GossipEventKind), u8>>,

    /// Collections to the lsm tree on disk.
    pub trees: RefCell<HashMap<String, Rc<LSMTree>>>,

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
        id: usize,
        shards: Vec<OtherShard>,
        cache: PageCache<FileId>,
        local_shards_packet_receiver: Receiver<ShardPacket>,
        stop_receiver: Receiver<()>,
        stop_sender: Sender<()>,
    ) -> Self {
        let shard_name = format!("{}-{}", args.name, id);
        let hash =
            hash_string(&format!("{}:{}", args.ip, args.port + id as u16))
                .unwrap();
        Self {
            args,
            id,
            shard_name,
            hash,
            shards: RefCell::new(shards),
            nodes: RefCell::new(HashMap::new()),
            gossip_requests: RefCell::new(HashMap::new()),
            trees: RefCell::new(HashMap::new()),
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

    pub fn get_collection(&self, name: &String) -> Result<Rc<LSMTree>> {
        self.trees
            .borrow()
            .get(name)
            .ok_or_else(|| Error::CollectionNotFound(name.to_string()))
            .map(|t| t.clone())
    }

    pub fn get_collection_names_from_disk(&self) -> Result<Vec<String>> {
        if !std::fs::metadata(&self.args.dir)
            .map(|m| m.is_dir())
            .unwrap_or(false)
        {
            return Ok(Vec::new());
        }

        let pattern = format!(r#"(.*)\-{}$"#, self.id);
        let regex = Regex::new(pattern.as_str())
            .map_err(|source| Error::RegexCreationError { source, pattern })?;
        let paths = std::fs::read_dir(&self.args.dir)?
            .filter_map(std::result::Result::ok)
            .filter_map(|entry| get_first_capture(&regex, &entry))
            .collect::<Vec<_>>();
        Ok(paths)
    }

    fn get_collection_dir(&self, name: &String) -> PathBuf {
        let mut dir = PathBuf::from(self.args.dir.clone());
        dir.push(format!("{}-{}", name, self.id));
        dir
    }

    async fn create_lsm_tree(&self, name: String) -> Result<Rc<LSMTree>> {
        let cache = self.cache.clone();
        let tree = Rc::new(
            LSMTree::open_or_create(
                self.get_collection_dir(&name),
                PartitionPageCache::new(name.clone(), cache),
            )
            .await?,
        );
        Ok(tree)
    }

    pub async fn create_collection(&self, name: String) -> Result<()> {
        if self.trees.borrow().contains_key(&name) {
            return Err(Error::CollectionAlreadyExists(name));
        }
        let tree = self.create_lsm_tree(name.clone()).await?;
        self.trees.borrow_mut().insert(name, tree);
        Ok(())
    }

    pub fn drop_collection(&self, name: &String) -> Result<()> {
        self.trees
            .borrow_mut()
            .remove(name)
            .ok_or_else(|| Error::CollectionNotFound(name.clone()))?
            .purge()?;
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

        for sender in senders {
            sender
                .send(ShardPacket::new(self.id, message.clone()))
                .await?;
        }

        Ok(())
    }

    pub async fn send_request_to_replicas<F, T>(
        self: Rc<Self>,
        request: ShardRequest,
        number_of_acks: usize,
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
                HashSet::with_capacity(my_shard.args.replication_factor as usize);

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
                .take(my_shard.args.replication_factor as usize)
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

    async fn handle_shard_event(&self, event: ShardEvent) -> Result<()> {
        // All events should be idempotent.
        match event {
            ShardEvent::CreateCollection(name) => {
                match self.create_collection(name).await {
                    Ok(()) | Err(Error::CollectionAlreadyExists(_)) => {}
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            ShardEvent::DropCollection(name) => {
                match self.drop_collection(&name) {
                    Ok(()) | Err(Error::CollectionNotFound(_)) => {}
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            ShardEvent::Gossip(event) => {
                self.handle_gossip_event(event).await?;
            }
        };

        Ok(())
    }

    pub fn get_node_metadata(&self) -> NodeMetadata {
        let shard_ports = self
            .shards
            .borrow()
            .iter()
            .flat_map(|shard| match &shard.connection {
                ShardConnection::Remote(_) => None,
                ShardConnection::Local(c) => {
                    Some(self.args.remote_shard_port + c.id as u16)
                }
            })
            .collect::<Vec<_>>();

        NodeMetadata {
            name: self.args.name.clone(),
            ip: self.args.ip.clone(),
            shard_ports,
            gossip_port: self.args.gossip_port,
            db_port: self.args.port,
        }
    }

    pub fn add_shards_of_nodes(&self, nodes: Vec<NodeMetadata>) {
        self.shards.borrow_mut().extend(
            nodes
                .into_iter()
                .flat_map(|node| {
                    node.shard_ports
                        .into_iter()
                        .map(|port| {
                            (node.name.clone(), format!("{}:{}", node.ip, port))
                        })
                        .collect::<Vec<_>>()
                })
                .map(|(node_name, address)| {
                    OtherShard::new(
                        node_name,
                        address.clone(),
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
            replication_factor: self.args.replication_factor,
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
            ShardRequest::Set(collection, key, value, timestamp) => {
                let existing_tree =
                    self.trees.borrow().get(&collection).cloned();
                if let Some(tree) = existing_tree {
                    tree.set_with_timestamp(key, value, timestamp).await?;
                } else {
                    let tree = self.create_lsm_tree(collection.clone()).await?;
                    tree.clone()
                        .set_with_timestamp(key, value, timestamp)
                        .await?;
                    self.trees.borrow_mut().insert(collection, tree);
                };

                notify_flow_event!(self, FlowEvent::ItemSetFromRemoteShard);

                ShardResponse::Set
            }
            ShardRequest::Delete(collection, key, timestamp) => {
                let existing_tree =
                    self.trees.borrow().get(&collection).cloned();
                if let Some(tree) = existing_tree {
                    tree.delete_with_timestamp(key, timestamp).await?;
                };
                ShardResponse::Delete
            }
            ShardRequest::Get(collection, key) => {
                let existing_tree =
                    self.trees.borrow().get(&collection).cloned();
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

    pub async fn handle_shard_message(
        &self,
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

    pub async fn gossip(&self, event: GossipEvent) -> Result<()> {
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

    pub async fn handle_dead_node(&self, node_name: &String) {
        if self.nodes.borrow_mut().remove(node_name).is_none() {
            return;
        }

        trace!(
            "After death: holding {} number of nodes",
            self.nodes.borrow().len()
        );
        self.shards
            .borrow_mut()
            .retain(|shard| &shard.node_name != node_name);
        notify_flow_event!(self, FlowEvent::DeadNodeRemoved);
    }

    pub async fn handle_gossip_event(
        &self,
        event: GossipEvent,
    ) -> Result<bool> {
        // All events must be handled idempotently, as gossip messages can be seen
        // multiple times.
        let sent_event = match event {
            GossipEvent::Alive(node) if node.name != self.args.name => {
                self.nodes
                    .borrow_mut()
                    .entry(node.name.clone())
                    .or_insert_with(|| node.clone());
                trace!(
                    "After alive: holding {} number of nodes",
                    self.nodes.borrow().len()
                );
                self.add_shards_of_nodes(vec![node]);

                notify_flow_event!(self, FlowEvent::AliveNodeGossip);

                false
            }
            GossipEvent::Dead(node_name) => {
                if node_name == self.args.name {
                    // Whoops, someone marked us as dead, even though we are alive
                    // and well.
                    // Let's notify everyone that we are actually alive.
                    self.gossip(GossipEvent::Alive(self.get_node_metadata()))
                        .await?;
                    true
                } else {
                    self.handle_dead_node(&node_name).await;
                    false
                }
            }
            _ => false,
        };

        Ok(sent_event)
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
