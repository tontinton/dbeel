use std::any::{Any, TypeId};
use std::time::Duration;
use std::{cell::RefCell, collections::HashMap, path::PathBuf, rc::Rc};

use caches::Cache;
use futures::future::join_all;
use glommio::net::UdpSocket;
use log::trace;
use murmur3::murmur3_32;
use rand::seq::IteratorRandom;
use rand::thread_rng;
use regex::Regex;

use crate::gossip::{serialize_gossip_message, GossipEvent, GossipMessage};
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

#[derive(Debug)]
pub enum ShardConnection {
    Local(LocalShardConnection),
    Remote(RemoteShardConnection),
}

// A struct to hold other shards to communicate with.
#[derive(Debug)]
pub struct OtherShard {
    // The unique shard name.
    pub name: String,

    // The hash of the unique name, used for consistent hashing.
    pub hash: u32,

    // Communicate with a shard by abstracting away if it's remote or local.
    pub connection: ShardConnection,
}

fn hash_string(s: &String) -> std::io::Result<u32> {
    murmur3_32(&mut std::io::Cursor::new(s), 0)
}

impl OtherShard {
    pub fn new(name: String, connection: ShardConnection) -> Self {
        let hash = hash_string(&name).unwrap();
        Self {
            name,
            hash,
            connection,
        }
    }
}

// Holder of state to the shard running on the current thread.
// To share the state between all coroutines, async methods get Rc<Self>.
pub struct MyShard {
    // Input config from the user.
    pub args: Args,

    // Current shard's cpu id.
    pub id: usize,

    // Shard unique name, if you want the node unique name, it's in args.name.
    pub shard_name: String,

    // The consistent hash ring (shards sorted by hash).
    pub shards: RefCell<Vec<OtherShard>>,

    // All known nodes other than this node, key is node unique name.
    pub nodes: RefCell<HashMap<String, NodeMetadata>>,

    // Holds the counts of gossip requests.
    pub gossip_requests: RefCell<HashMap<(String, TypeId), u8>>,

    // Collections to the lsm tree on disk.
    pub trees: RefCell<HashMap<String, Rc<LSMTree>>>,

    // The shard's page cache.
    cache: Rc<RefCell<PageCache<FileId>>>,
}

impl MyShard {
    pub fn new(
        args: Args,
        id: usize,
        shards: Vec<OtherShard>,
        cache: PageCache<FileId>,
    ) -> Self {
        let shard_name = format!("{}-{}", args.name, id);
        Self {
            args,
            id,
            shard_name,
            shards: RefCell::new(shards),
            nodes: RefCell::new(HashMap::new()),
            gossip_requests: RefCell::new(HashMap::new()),
            trees: RefCell::new(HashMap::new()),
            cache: Rc::new(RefCell::new(cache)),
        }
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

    pub async fn create_collection(self: Rc<Self>, name: String) -> Result<()> {
        if self.trees.borrow().contains_key(&name) {
            return Err(Error::CollectionAlreadyExists(name));
        }

        let cache = self.cache.clone();
        let tree = Rc::new(
            LSMTree::open_or_create(
                self.get_collection_dir(&name),
                PartitionPageCache::new(name.clone(), cache),
            )
            .await?,
        );
        self.trees.borrow_mut().insert(name, tree);
        Ok(())
    }

    pub fn drop_collection(self: Rc<Self>, name: String) -> Result<()> {
        self.trees
            .borrow_mut()
            .remove(&name)
            .ok_or(Error::CollectionNotFound(name))?
            .purge()?;

        // TODO: add set items to cache, then there will be no reason to ever delete
        // items from the cache.
        self.cache.borrow_mut().purge();

        Ok(())
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

    async fn handle_shard_event(
        self: Rc<Self>,
        event: ShardEvent,
    ) -> Result<()> {
        // All events should be idempotent.
        match event {
            ShardEvent::CreateCollection(name) => {
                if let Err(e) = self.create_collection(name).await {
                    if e.type_id() != Error::CollectionAlreadyExists.type_id() {
                        return Err(e);
                    }
                }
            }
            ShardEvent::DropCollection(name) => {
                if let Err(e) = self.drop_collection(name) {
                    if e.type_id() != Error::CollectionNotFound.type_id() {
                        return Err(e);
                    }
                }
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
        }
    }

    pub fn add_shards_of_nodes(&self, nodes: Vec<NodeMetadata>) {
        self.shards.borrow_mut().extend(
            nodes
                .into_iter()
                .flat_map(|node| {
                    node.shard_ports
                        .into_iter()
                        .map(|port| format!("{}:{}", node.ip, port))
                        .collect::<Vec<_>>()
                })
                .map(|address| {
                    OtherShard::new(
                        address.clone(),
                        ShardConnection::Remote(RemoteShardConnection::new(
                            address,
                            Duration::from_millis(
                                self.args.remote_shard_connect_timeout,
                            ),
                        )),
                    )
                }),
        );
    }

    fn handle_shard_request(
        &self,
        request: ShardRequest,
    ) -> Result<ShardResponse> {
        let response = match request {
            ShardRequest::Ping => ShardResponse::Pong,
            ShardRequest::GetMetadata => {
                let mut nodes = self
                    .nodes
                    .borrow()
                    .iter()
                    .map(|(_, n)| n.clone())
                    .collect::<Vec<_>>();
                nodes.push(self.get_node_metadata());

                ShardResponse::GetMetadata(nodes)
            }
        };

        Ok(response)
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
                Some(self.handle_shard_request(request)?)
            }
            ShardMessage::Response(_) => None,
        })
    }

    pub async fn gossip(self: Rc<Self>, event: GossipEvent) -> Result<()> {
        let message = GossipMessage::new(self.args.name.clone(), event);
        self.gossip_buffer(&serialize_gossip_message(&message)?)
            .await
    }

    pub async fn gossip_buffer(
        self: Rc<Self>,
        message_buffer: &[u8],
    ) -> Result<()> {
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

    pub fn handle_dead_node(&self, node_name: &String) {
        self.nodes.borrow_mut().remove(node_name);
        trace!(
            "After death: holding {} number of nodes",
            self.nodes.borrow().len()
        );
    }

    pub async fn handle_gossip_event(
        self: Rc<Self>,
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
                    self.nodes.borrow_mut().remove(&node_name);
                    trace!(
                        "After death: holding {} number of nodes",
                        self.nodes.borrow().len()
                    );
                    false
                }
            }
            _ => false,
        };

        Ok(sent_event)
    }
}
