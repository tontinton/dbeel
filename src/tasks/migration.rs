use std::{cmp::Ordering, net::Shutdown, rc::Rc};

use glommio::spawn_local;
use log::error;

use crate::{
    error::Result,
    lsm_tree::{Entry, LSMTree},
    messages::{ShardEvent, ShardMessage, ShardPacket},
    notify_flow_event,
    remote_shard_connection::send_message_to_stream,
    shards::{hash_bytes, MyShard, ShardConnection},
};

#[cfg(feature = "flow-events")]
use crate::flow_events::FlowEvent;

fn create_set_message(name: String, entry: Entry) -> ShardMessage {
    ShardMessage::Event(ShardEvent::Set(
        name,
        entry.key,
        entry.value.data,
        entry.value.timestamp,
    ))
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
            .map(|hash| {
                if end < start {
                    hash.cmp(&start) == Ordering::Less
                        || hash.cmp(&end) != Ordering::Less
                } else {
                    hash.cmp(&start) != Ordering::Less
                        && hash.cmp(&end) == Ordering::Less
                }
            })
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
            let _ = stream.shutdown(Shutdown::Both).await;
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
