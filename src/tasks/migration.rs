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

async fn migrate(
    collection_name: String,
    tree: Rc<LSMTree>,
    start: u32,
    end: u32,
    connection: ShardConnection,
) -> Result<()> {
    let mut iter = tree.iter_filter(Box::new(move |key, _| {
        hash_bytes(key)
            .map(|hash| between_cmp(hash, &start, &end))
            .unwrap_or(false)
    }));

    match connection {
        ShardConnection::Remote(remote_connection) => {
            let mut stream = remote_connection.connect().await?;
            while let Ok(Some(entry)) = iter.next().await {
                send_message_to_stream(
                    &mut stream,
                    &create_set_message(collection_name.clone(), entry),
                )
                .await?;
            }
            if let Err(e) = stream.shutdown(Shutdown::Both).await {
                error!("Error shutting down migration socket: {}", e);
            }
        }
        ShardConnection::Local(local_connection) => {
            while let Ok(Some(entry)) = iter.next().await {
                local_connection
                    .sender
                    .send(ShardPacket::new(
                        local_connection.id,
                        create_set_message(collection_name.clone(), entry),
                    ))
                    .await?;
            }
        }
    };

    Ok(())
}

async fn migrate_actions(
    collection_name: String,
    tree: Rc<LSMTree>,
    ranges_and_actions: &[(u32, u32, MigrationAction)],
) -> Result<()> {
    let mut actions = Vec::with_capacity(ranges_and_actions.len());
    for (_, _, action) in ranges_and_actions {
        actions.push(match action {
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
        .map(|(start, end, _)| (*start, *end))
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

pub fn spawn_migration_tasks(
    my_shard: Rc<MyShard>,
    start: u32,
    end: u32,
    connection: ShardConnection,
) {
    let trees = my_shard
        .trees
        .borrow()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<Vec<_>>();
    for (collection_name, tree) in trees {
        let c = connection.clone();

        #[cfg(feature = "flow-events")]
        let s = my_shard.clone();

        spawn_local(async move {
            let result = migrate(collection_name, tree, start, end, c).await;
            if let Err(e) = &result {
                error!("Error migrating: {}", e);
            }
            notify_flow_event!(s, FlowEvent::DoneMigration);
            result
        })
        .detach();
    }
}

pub fn spawn_migration_actions_tasks(
    my_shard: Rc<MyShard>,
    ranges_and_actions: Vec<(u32, u32, MigrationAction)>,
) {
    let trees = my_shard
        .trees
        .borrow()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<Vec<_>>();
    for (collection_name, tree) in trees {
        let r = ranges_and_actions.clone();

        #[cfg(feature = "flow-events")]
        let s = my_shard.clone();

        spawn_local(async move {
            let result = migrate_actions(collection_name, tree, &r).await;
            if let Err(e) = &result {
                error!("Error migrating: {}", e);
            }
            notify_flow_event!(s, FlowEvent::DoneMigration);
            result
        })
        .detach();
    }
}
