use bincode::{
    config::{
        FixintEncoding, RejectTrailing, WithOtherIntEncoding, WithOtherTrailing,
    },
    DefaultOptions, Options,
};
use futures_lite::AsyncWriteExt;
use glommio::{
    io::{DmaFile, DmaStreamWriterBuilder},
    LocalExecutorBuilder, Placement,
};
use redblacktree::RedBlackTree;
use serde::{Deserialize, Serialize};
use std::{env::temp_dir, io::Result, path::PathBuf};
use std::{fmt::Debug, time::Duration};

macro_rules! hashmap {
    ($( $key: expr => $val: expr ),*) => {{
         let mut map = ::std::collections::HashMap::new();
         $( map.insert($key, $val); )*
         map
    }}
}

#[derive(Debug, Serialize, Deserialize)]
struct Entry {
    key: String,
    value: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct EntryOffset {
    entry_offset: u64,
    entry_size: usize,
}

impl Default for EntryOffset {
    fn default() -> Self {
        Self {
            entry_offset: Default::default(),
            entry_size: Default::default(),
        }
    }
}

fn bincode_options() -> WithOtherIntEncoding<
    WithOtherTrailing<DefaultOptions, RejectTrailing>,
    FixintEncoding,
> {
    return DefaultOptions::new()
        .reject_trailing_bytes()
        .with_fixint_encoding();
}

async fn binary_search(
    data_file: &DmaFile,
    index_file: &DmaFile,
    key: &String,
) -> Result<Option<Entry>> {
    let item_size = bincode_options()
        .serialized_size(&EntryOffset::default())
        .unwrap();
    let length = index_file.file_size().await? / item_size;

    let mut half = length / 2;
    let mut hind = length - 1;
    let mut lind = 0;

    let mut current: EntryOffset = bincode_options()
        .deserialize(
            &index_file
                .read_at(half * item_size, item_size as usize)
                .await?,
        )
        .unwrap();

    while lind <= hind {
        let value: Entry = bincode_options()
            .deserialize(
                &data_file
                    .read_at(current.entry_offset as u64, current.entry_size)
                    .await?,
            )
            .unwrap();

        match value.key.cmp(&key) {
            std::cmp::Ordering::Equal => {
                return Ok(Some(value));
            }
            std::cmp::Ordering::Less => lind = half + 1,
            std::cmp::Ordering::Greater => hind = half - 1,
        }
        half = (hind + lind) / 2;
        current = bincode_options()
            .deserialize(
                &index_file
                    .read_at(half * item_size, item_size as usize)
                    .await?,
            )
            .unwrap();
    }

    Ok(None)
}

async fn run(dir: PathBuf) -> Result<()> {
    let mut data_filename = dir.clone();
    data_filename.push("data");
    let mut index_filename = dir.clone();
    index_filename.push("index");

    // Mimic writes to memtable.
    let entries = hashmap![
        "B".to_string() => "are".to_string(),
        "A".to_string() => "Trees".to_string(),
        "C".to_string() => "cool".to_string()
    ];

    let mut tree = RedBlackTree::with_capacity(entries.len());

    for (key, value) in entries {
        tree.insert(key, value).unwrap();
    }

    tree.pretty_print();
    println!("");

    // Mimic flush memtable to disk.
    let data_file = DmaFile::create(&data_filename).await?;
    let index_file = DmaFile::create(&index_filename).await?;

    let mut data_write_stream = DmaStreamWriterBuilder::new(data_file)
        .with_write_behind(10)
        .with_buffer_size(512)
        .build();
    let mut index_write_stream = DmaStreamWriterBuilder::new(index_file)
        .with_write_behind(10)
        .with_buffer_size(512)
        .build();
    for (key, value) in tree {
        let entry_offset = data_write_stream.current_pos();
        let entry = Entry { key, value };
        let entry_encoded = bincode_options().serialize(&entry).unwrap();
        let entry_size = entry_encoded.len();
        data_write_stream.write_all(&entry_encoded).await?;

        let entry_index = EntryOffset {
            entry_offset,
            entry_size,
        };
        let index_encoded = bincode_options().serialize(&entry_index).unwrap();
        index_write_stream.write_all(&index_encoded).await?;
    }
    data_write_stream.close().await?;
    index_write_stream.close().await?;

    // Mimic lookup from sstable.
    let data_file = DmaFile::open(&data_filename).await?;
    let index_file = DmaFile::open(&index_filename).await?;

    let lookup_key = "C".to_string();
    println!("Querying for key '{}'", lookup_key);

    let maybe_entry =
        binary_search(&data_file, &index_file, &lookup_key).await?;
    if let Some(entry) = maybe_entry {
        println!("Found: {}", entry.value);
    } else {
        println!("Key not found");
    }

    Ok(())
}

fn main() -> Result<()> {
    let builder = LocalExecutorBuilder::new(Placement::Fixed(1))
        .spin_before_park(Duration::from_millis(10));
    let handle = builder
        .name("dbil")
        .spawn(|| async move { run(temp_dir()).await })?;

    handle.join()??;
    Ok(())
}
