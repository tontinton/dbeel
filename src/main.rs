use dbil::lsm_tree::LSMTree;
use glommio::{LocalExecutorBuilder, Placement};
use rand::{seq::SliceRandom, thread_rng};
use rmpv::{decode::read_value_ref, encode::write_value_ref, ValueRef};
use std::time::Duration;
use std::{env::temp_dir, io::Result, path::PathBuf};

async fn write(dir: PathBuf) -> Result<()> {
    let mut tree = LSMTree::new(dir).await?;

    let mut rng = thread_rng();
    let mut nums: Vec<String> = (0..10000).map(|n| n.to_string()).collect();
    nums.shuffle(&mut rng);
    for i in nums {
        let msgpack_value = ValueRef::String(i.as_str().into());
        let mut encoded: Vec<u8> = Vec::new();
        write_value_ref(&mut encoded, &msgpack_value).unwrap();
        tree.set(encoded.clone(), encoded).await?;
    }

    let lookup_key = ValueRef::String("5000".into());
    let mut lookup_key_encoded: Vec<u8> = Vec::new();
    write_value_ref(&mut lookup_key_encoded, &lookup_key).unwrap();
    println!("Querying for key {}", lookup_key);

    if let Ok(Some(v)) = tree.get(&lookup_key_encoded).await {
        let msgpack_value = read_value_ref(&mut &v[..]).unwrap().to_owned();
        println!("Found: {}", msgpack_value);
    } else {
        println!("Key not found");
    }

    tree.compact(vec![0, 2, 4, 6, 8, 10, 12], 13).await?;

    Ok(())
}

async fn read(dir: PathBuf) -> Result<()> {
    let tree = LSMTree::new(dir).await?;

    let lookup_key = ValueRef::String("1234".into());
    let mut lookup_key_encoded: Vec<u8> = Vec::new();
    write_value_ref(&mut lookup_key_encoded, &lookup_key).unwrap();
    println!("Querying for key {}", lookup_key);

    if let Ok(Some(v)) = tree.get(&lookup_key_encoded).await {
        let msgpack_value = read_value_ref(&mut &v[..]).unwrap().to_owned();
        println!("Found: {}", msgpack_value);
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
