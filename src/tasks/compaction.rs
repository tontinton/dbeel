use std::{collections::HashMap, rc::Rc, time::Duration};

use futures::future::{join_all, select, select_all, Either};
use glommio::{executor, spawn_local_into, Latency, Shares, Task};
use itertools::Itertools;
use log::error;

use crate::{
    error::Result, shards::MyShard, storage_engine::lsm_tree::LSMTree,
    utils::local_event::LocalEventListener,
};

const MIN_COMPACTION_FACTOR: usize = 2;

async fn get_trees_and_listeners(
    my_shard: &MyShard,
) -> (Vec<Rc<LSMTree>>, Vec<LocalEventListener>) {
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
    let indices_and_sizes = tree.sstable_indices_and_sizes();

    let mut index_to_compact = indices_and_sizes
        .iter()
        .map(|(i, _)| *i)
        .filter(|i| i % 2 != 0)
        .max()
        .map(|i| i + 2)
        .unwrap_or(1);

    let mut groups = indices_and_sizes
        .into_iter()
        .into_group_map_by(|(_, size)| size.leading_zeros())
        .into_iter()
        .filter(|(_, items)| !items.is_empty())
        .collect::<Vec<_>>();

    groups.sort_unstable_by(|(a, _), (b, _)| b.cmp(a));

    // Possibly overshooting the amount of memory to limit allocations.
    let mut optimized_groups = HashMap::with_capacity(groups.len());

    for (size_order, mut items) in groups {
        if let Some(smaller_items) = optimized_groups.remove(&size_order) {
            items.extend(smaller_items);
        }

        // Doesn't currently take deletes into account.
        let estimated_size_order_after_compaction = items
            .iter()
            .map(|(_, size)| size)
            .sum::<u64>()
            .leading_zeros();

        let optimized_size_order =
            if estimated_size_order_after_compaction < size_order {
                estimated_size_order_after_compaction
            } else {
                size_order
            };

        optimized_groups
            .entry(optimized_size_order)
            .or_insert(vec![])
            .extend(items);
    }

    for items in optimized_groups.into_values() {
        if items.len() < MIN_COMPACTION_FACTOR
            || items.len() < compaction_factor
        {
            continue;
        }
        let indices = items.into_iter().map(|(i, _)| i).collect::<Vec<_>>();
        if let Err(e) = tree.compact(&indices, index_to_compact).await {
            error!("Failed to compact files: {}", e);
        }
        index_to_compact += 2;
    }
}

async fn run_compaction_loop(my_shard: Rc<MyShard>) {
    let compaction_factor = my_shard.args.compaction_factor;
    if compaction_factor < MIN_COMPACTION_FACTOR {
        return;
    }

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
    let shares = my_shard.args.foreground_tasks_shares.into();
    spawn_local_into(
        async move {
            run_compaction_loop(my_shard).await;
            Ok(())
        },
        executor().create_task_queue(
            Shares::Static(shares),
            Latency::Matters(Duration::from_millis(50)),
            "compaction",
        ),
    )
    .unwrap()
}
