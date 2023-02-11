use bincode::{
    config::{
        FixintEncoding, RejectTrailing, WithOtherIntEncoding, WithOtherTrailing,
    },
    DefaultOptions, Options,
};
use redblacktree::RedBlackTree;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

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
    entry_offset: usize,
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

fn binary_search(
    index_buffer: &[u8],
    data_buffer: &[u8],
    key: &String,
) -> Option<Entry> {
    let item_size = bincode_options()
        .serialized_size(&EntryOffset::default())
        .unwrap() as usize;
    let length = index_buffer.len() / item_size;

    let mut half = length / 2;
    let mut hind = length - 1;
    let mut lind = 0;

    let mut current: EntryOffset = bincode_options()
        .deserialize(&index_buffer[half * item_size..(half + 1) * item_size])
        .unwrap();

    while lind <= hind {
        let value: Entry = bincode_options()
            .deserialize(
                &data_buffer[current.entry_offset
                    ..current.entry_offset + current.entry_size],
            )
            .unwrap();

        match value.key.cmp(&key) {
            std::cmp::Ordering::Equal => {
                return Some(value);
            }
            std::cmp::Ordering::Less => lind = half + 1,
            std::cmp::Ordering::Greater => hind = half - 1,
        }
        half = (hind + lind) / 2;
        current = bincode_options()
            .deserialize(
                &index_buffer[half * item_size..(half + 1) * item_size],
            )
            .unwrap();
    }

    return None;
}

fn main() {
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
    let mut data = Vec::new();
    let mut index = Vec::new();

    for (key, value) in tree {
        let entry_offset = data.len();
        let entry = Entry { key, value };
        let entry_encoded = bincode_options().serialize(&entry).unwrap();
        let entry_size = entry_encoded.len();
        data.extend(entry_encoded);

        let entry_index = EntryOffset {
            entry_offset,
            entry_size,
        };
        let index_encoded = bincode_options().serialize(&entry_index).unwrap();
        index.extend(index_encoded);
    }

    // Mimic lookup from sstable.
    let lookup_key = "C".to_string();
    println!("Querying for key '{}'", lookup_key);

    let value = binary_search(&index, &data, &lookup_key).unwrap();

    println!("Found: {:?}", value);
}
