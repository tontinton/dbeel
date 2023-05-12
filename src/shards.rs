use std::any::Any;
use std::{cell::RefCell, collections::HashMap, path::PathBuf, rc::Rc};

use caches::Cache;
use log::error;
use murmur3::murmur3_32;

use crate::messages::{ShardRequest, ShardResponse};
use crate::{
    args::Args,
    cached_file_reader::FileId,
    error::{Error, Result},
    local_shard::LocalShardConnection,
    lsm_tree::LSMTree,
    messages::{ShardEvent, ShardMessage, ShardPacket},
    page_cache::{PageCache, PartitionPageCache},
    remote_shard::RemoteShardConnection,
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

    // Shard unique name.
    pub name: String,

    // The consistent hash ring (shards sorted by hash).
    pub shards: RefCell<Vec<OtherShard>>,

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
        let name = format!("{}-{}", args.name, id);
        Self {
            args,
            id,
            name,
            shards: RefCell::new(shards),
            trees: RefCell::new(HashMap::new()),
            cache: Rc::new(RefCell::new(cache)),
        }
    }

    pub fn get_collection(&self, name: &String) -> Result<Rc<LSMTree>> {
        self.trees
            .borrow()
            .get(name)
            .ok_or(Error::CollectionNotFound(name.to_string()))
            .map(|t| t.clone())
    }

    pub async fn create_collection(self: Rc<Self>, name: String) -> Result<()> {
        if self.trees.borrow().contains_key(&name) {
            return Err(Error::CollectionAlreadyExists(name));
        }

        let mut dir = PathBuf::from(self.args.dir.clone());
        dir.push(format!("{}-{}", name, self.id));
        let cache = self.cache.clone();
        let tree = Rc::new(
            LSMTree::open_or_create(
                dir,
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

    async fn broadcast_message_to_remote_shards(
        self: Rc<Self>,
        message: &ShardMessage,
    ) -> Result<()> {
        let connections = self
            .shards
            .borrow()
            .iter()
            .flat_map(|p| match &p.connection {
                ShardConnection::Remote(c) => Some(c.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();

        for c in connections {
            if let Err(e) = c.send_message(message).await {
                error!(
                    "Error sending message to remote shard ({}): {}",
                    c.address, e
                );
            }
        }

        Ok(())
    }

    async fn broadcast_message_to_local_shards(
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

    pub async fn broadcast_message(
        self: Rc<Self>,
        message: &ShardMessage,
    ) -> Result<()> {
        self.clone()
            .broadcast_message_to_local_shards(message)
            .await?;
        self.broadcast_message_to_remote_shards(message).await?;
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

    fn handle_shard_request(
        &self,
        request: ShardRequest,
    ) -> Result<ShardResponse> {
        let response = match request {
            ShardRequest::GetShards => ShardResponse::GetShards(
                self.shards
                    .borrow()
                    .iter()
                    .flat_map(|shard| match &shard.connection {
                        ShardConnection::Remote(c) => {
                            Some((shard.name.clone(), c.address.clone()))
                        }
                        ShardConnection::Local(c) => Some((
                            shard.name.clone(),
                            format!(
                                "{}:{}",
                                self.args.ip,
                                self.args.remote_shard_port + c.id as u16
                            ),
                        )),
                    })
                    .collect::<Vec<_>>(),
            ),
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
}
