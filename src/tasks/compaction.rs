use std::{pin::Pin, rc::Rc};

use event_listener::EventListener;
use futures::future::{join_all, select, select_all, Either};
use glommio::{spawn_local, Task};
use itertools::Itertools;
use log::error;

use crate::{
    error::Result, shards::MyShard, storage_engine::lsm_tree::LSMTree,
};

async fn get_trees_and_listeners(
    my_shard: &MyShard,
) -> (Vec<Rc<LSMTree>>, Vec<Pin<Box<EventListener<()>>>>) {
    while my_shard.collections.borrow().is_empty() {
        my_shard.collections_change_event.listen().await;
    }

    let trees = my_shard
        .collections
        .borrow()
        .values()
        .map(|c| c.tree.clone())
        .collect::<Vec<_>>();
    let listeners = trees
        .iter()
        .map(|tree| tree.get_flush_event_listener())
        .collect::<Vec<_>>();
    (trees, listeners)
}

async fn compact_tree(tree: Rc<LSMTree>, compaction_factor: usize) {
    loop {
        let indices_and_sizes = tree.sstable_indices_and_sizes();

        let mut index_to_compact = indices_and_sizes
            .iter()
            .map(|(i, _)| *i)
            .filter(|i| i % 2 != 0)
            .max()
            .map(|i| i + 2)
            .unwrap_or(1);

        let groups = indices_and_sizes
            .into_iter()
            .into_group_map_by(|(_, size)| size.leading_zeros())
            .into_values()
            .filter(|items| items.len() >= compaction_factor);

        let mut has_groups = false;
        for items in groups {
            has_groups = true;

            let indices = items.into_iter().map(|(i, _)| i).collect::<Vec<_>>();
            if let Err(e) = tree.compact(&indices, index_to_compact, true).await
            {
                error!("Failed to compact files: {}", e);
            }
            index_to_compact += 2;
        }

        if !has_groups {
            break;
        }
    }
}

async fn run_compaction_loop(my_shard: Rc<MyShard>) {
    let compaction_factor = my_shard.args.compaction_factor;

    let (mut trees, mut listeners) = get_trees_and_listeners(&my_shard).await;

    // Try to compact once, in case we return from a crash and want to compact
    // whatever files are currently saved.
    let futures = trees
        .iter()
        .cloned()
        .map(|tree| compact_tree(tree, compaction_factor));
    join_all(futures).await;

    loop {
        match select(
            my_shard.collections_change_event.listen(),
            select_all(&mut listeners),
        )
        .await
        {
            Either::Left(..) => {
                (trees, listeners) = get_trees_and_listeners(&my_shard).await;
            }
            Either::Right(((_, i, _), _)) => {
                let tree = &trees[i];
                listeners[i] = tree.get_flush_event_listener();
                compact_tree(tree.clone(), compaction_factor).await;
            }
        };
    }
}

pub fn spawn_compaction_task(my_shard: Rc<MyShard>) -> Task<Result<()>> {
    spawn_local(async move {
        run_compaction_loop(my_shard).await;
        Ok(())
    })
}
