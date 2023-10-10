use std::{pin::Pin, rc::Rc};

use event_listener::EventListener;
use futures::future::{join_all, select, select_all, Either};
use glommio::{spawn_local, Task};
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
    'current_tree_compaction: loop {
        let (even, mut odd): (Vec<usize>, Vec<usize>) =
            tree.sstable_indices().iter().partition(|i| *i % 2 == 0);

        if even.len() >= compaction_factor {
            let new_index = even[even.len() - 1] + 1;
            if let Err(e) = tree.compact(even, new_index, odd.is_empty()).await
            {
                error!("Failed to compact files: {}", e);
            }
            continue 'current_tree_compaction;
        }

        if odd.len() >= compaction_factor && !even.is_empty() {
            debug_assert!(even[0] > odd[odd.len() - 1]);

            odd.push(even[0]);

            let new_index = even[0] + 1;
            if let Err(e) = tree.compact(odd, new_index, true).await {
                error!("Failed to compact files: {}", e);
            }
            continue 'current_tree_compaction;
        }

        break 'current_tree_compaction;
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
