use dbil::lsm_tree::LSMTree;
use glommio::{LocalExecutorBuilder, Placement};
use std::time::Duration;
use std::{env::temp_dir, io::Result, path::PathBuf};

macro_rules! hashmap {
    ($( $key: expr => $val: expr ),*) => {{
         let mut map = ::std::collections::HashMap::new();
         $( map.insert($key, $val); )*
         map
    }}
}

async fn run(dir: PathBuf) -> Result<()> {
    let mut tree = LSMTree::new(dir).await?;

    let entries = hashmap![
        "B".to_string() => "are".to_string(),
        "A".to_string() => "Trees".to_string(),
        "C".to_string() => "cool".to_string()
    ];
    for (key, value) in entries {
        tree.set(key, value).await?;
    }

    let lookup_key = "C".to_string();
    println!("Querying for key '{}'", lookup_key);

    if let Ok(Some(v)) = tree.get(&lookup_key).await {
        println!("Found: {}", v);
    } else {
        println!("Key not found");
    }

    tree.flush().await?;

    Ok(())
}

fn main() -> Result<()> {
    let builder = LocalExecutorBuilder::new(Placement::Fixed(1))
        .spin_before_park(Duration::from_millis(10));
    let handle = builder.name("dbil").spawn(|| async move {
        let mut db_dir = temp_dir();
        db_dir.push("dbil");
        run(db_dir).await
    })?;

    handle.join()??;
    Ok(())
}
