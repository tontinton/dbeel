use std::{rc::Rc, time::Duration};

use glommio::{spawn_local, timer::sleep, Task};
use log::error;

use crate::{error::Result, shards::MyShard};

async fn run_compaction_loop(my_shard: Rc<MyShard>) {
    let compaction_factor = my_shard.args.compaction_factor;

    loop {
        let trees = my_shard
            .trees
            .borrow()
            .values()
            .map(|t| t.clone())
            .collect::<Vec<_>>();

        for tree in trees {
            'current_tree_compaction: loop {
                let (even, mut odd): (Vec<usize>, Vec<usize>) =
                    tree.sstable_indices().iter().partition(|i| *i % 2 == 0);

                if even.len() >= compaction_factor {
                    let new_index = even[even.len() - 1] + 1;
                    if let Err(e) =
                        tree.compact(even, new_index, odd.is_empty()).await
                    {
                        error!("Failed to compact files: {}", e);
                    }
                    continue 'current_tree_compaction;
                }

                if odd.len() >= compaction_factor && even.len() >= 1 {
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

        sleep(Duration::from_millis(1)).await;
    }
}

pub fn spawn_compaction_task(my_shard: Rc<MyShard>) -> Task<Result<()>> {
    spawn_local(async move {
        run_compaction_loop(my_shard).await;
        Ok(())
    })
}
