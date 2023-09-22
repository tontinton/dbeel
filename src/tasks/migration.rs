use std::{cmp::Ordering, net::Shutdown, rc::Rc};

use async_channel::Sender;
use glommio::{net::TcpStream, spawn_local};
use log::error;

use crate::{
    error::Result,
    messages::{ShardEvent, ShardMessage, ShardPacket},
    notify_flow_event,
    remote_shard_connection::send_message_to_stream,
    shards::{hash_bytes, MyShard, ShardConnection},
    storage_engine::{lsm_tree::LSMTree, Entry},
};

#[cfg(feature = "flow-events")]
use crate::flow_events::FlowEvent;

#[derive(Debug, Clone)]
pub enum MigrationAction {
    SendToShard(ShardConnection),
    Delete,
}

#[derive(Debug)]
pub struct RangeAndAction {
    start: u32,
    end: u32,
    action: MigrationAction,
}

impl RangeAndAction {
    pub fn new(start: u32, end: u32, action: MigrationAction) -> Self {
        Self { start, end, action }
    }
}

enum Action {
    Remote(TcpStream),
    Local(u16, Sender<ShardPacket>),
    Delete,
}

fn create_set_message(name: String, entry: Entry) -> ShardMessage {
    ShardMessage::Event(ShardEvent::Set(
        name,
        entry.key,
        entry.value.data,
        entry.value.timestamp,
    ))
}

fn between_cmp(hash: u32, start: &u32, end: &u32) -> bool {
    if end < start {
        hash.cmp(start) == Ordering::Less || hash.cmp(end) != Ordering::Less
    } else {
        hash.cmp(start) != Ordering::Less && hash.cmp(end) == Ordering::Less
    }
}

async fn migrate_actions(
    collection_name: String,
    tree: Rc<LSMTree>,
    ranges_and_actions: &[RangeAndAction],
) -> Result<()> {
    let mut actions = Vec::with_capacity(ranges_and_actions.len());
    for ra in ranges_and_actions {
        actions.push(match &ra.action {
            MigrationAction::SendToShard(ShardConnection::Remote(c)) => {
                Action::Remote(c.connect().await?)
            }
            MigrationAction::SendToShard(ShardConnection::Local(c)) => {
                Action::Local(c.id, c.sender.clone())
            }
            MigrationAction::Delete => Action::Delete,
        });
    }

    let ranges = ranges_and_actions
        .iter()
        .map(|ra| (ra.start, ra.end))
        .collect::<Vec<_>>();
    let ranges_clone = ranges.clone();

    let mut iter = tree.iter_filter(Box::new(move |key, _| {
        hash_bytes(key)
            .map(|hash| {
                ranges_clone
                    .iter()
                    .any(|(start, end)| between_cmp(hash, start, end))
            })
            .unwrap_or(false)
    }));

    while let Ok(Some(entry)) = iter.next().await {
        let index = ranges
            .iter()
            .position(|(start, end)| {
                hash_bytes(&entry.key)
                    .map(|hash| between_cmp(hash, start, end))
                    .unwrap_or(false)
            })
            .unwrap();

        let key = entry.key.clone();
        let msg = create_set_message(collection_name.clone(), entry);

        match &mut actions[index] {
            Action::Remote(ref mut stream) => {
                send_message_to_stream(stream, &msg).await?;
            }
            Action::Local(id, sender) => {
                sender.send(ShardPacket::new(*id, msg)).await?;
            }
            Action::Delete => {
                tree.clone().delete(key).await?;
            }
        }
    }

    for action in actions {
        if let Action::Remote(stream) = action {
            if let Err(e) = stream.shutdown(Shutdown::Both).await {
                error!("Error shutting down migration socket: {}", e);
            }
        }
    }

    Ok(())
}

pub fn spawn_migration_actions_tasks(
    my_shard: Rc<MyShard>,
    collections_to_ranges_and_actions: Vec<(String, Vec<RangeAndAction>)>,
) {
    for (collection_name, ranges_and_actions) in
        collections_to_ranges_and_actions
    {
        if let Some(tree) = my_shard
            .collections
            .borrow()
            .get(&collection_name)
            .map(|c| c.tree.clone())
        {
            #[cfg(feature = "flow-events")]
            let s = my_shard.clone();

            spawn_local(async move {
                let result =
                    migrate_actions(collection_name, tree, &ranges_and_actions)
                        .await;
                if let Err(e) = &result {
                    error!("Error migrating: {}", e);
                }
                notify_flow_event!(s, FlowEvent::DoneMigration);
                result
            })
            .detach();
        }
    }
}
