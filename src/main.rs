use dbil::lsm_tree::LSMTree;
use glommio::{LocalExecutorBuilder, Placement};
use rand::{seq::SliceRandom, thread_rng};
use std::time::Duration;
use std::{env::temp_dir, io::Result, path::PathBuf};

async fn write(dir: PathBuf) -> Result<()> {
    let mut tree = LSMTree::new(dir).await?;

    let mut rng = thread_rng();
    let mut nums: Vec<u32> = (0..10000).collect();
    nums.shuffle(&mut rng);
    for i in nums {
        tree.set(i.to_string(), i.to_string()).await?;
    }

    let lookup_key = "5000".to_string();
    println!("Querying for key '{}'", lookup_key);

    if let Ok(Some(v)) = tree.get(&lookup_key).await {
        println!("Found: {}", v);
    } else {
        println!("Key not found");
    }

    tree.compact(vec![0, 1, 2, 3, 4, 5, 6], 6).await?;

    Ok(())
}

async fn read(dir: PathBuf) -> Result<()> {
    let tree = LSMTree::new(dir).await?;

    let lookup_key = "1234".to_string();
    println!("Querying for key '{}'", lookup_key);

    if let Ok(Some(v)) = tree.get(&lookup_key).await {
        println!("Found: {}", v);
    } else {
        println!("Key not found");
    }

    Ok(())
}

fn main() -> Result<()> {
    let builder = LocalExecutorBuilder::new(Placement::Fixed(1))
        .spin_before_park(Duration::from_millis(10));
    let handle = builder.name("dbil").spawn(|| async move {
        let mut db_dir = temp_dir();
        db_dir.push("dbil");
        write(db_dir.clone()).await?;
        read(db_dir).await
    })?;

    handle.join()??;
    Ok(())
}
