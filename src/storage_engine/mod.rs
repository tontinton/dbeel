use std::cmp::Ordering;

use kinded::Kinded;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::utils::timestamp_nanos;

pub mod cached_file_reader;
pub mod entry_writer;
pub mod lsm_tree;
pub mod page_cache;

pub const TOMBSTONE: Vec<u8> = vec![];

const DMA_STREAM_NUMBER_OF_BUFFERS: usize = 16;

pub const DEFAULT_TREE_CAPACITY: usize = 4096;
pub const DEFAULT_SSTABLE_BLOOM_MIN_SIZE: u64 = 1_048_576;

const INDEX_PADDING: usize = 20; // Number of integers in max u64.

const MEMTABLE_FILE_EXT: &str = "memtable";
const DATA_FILE_EXT: &str = "data";
const INDEX_FILE_EXT: &str = "index";
const BLOOM_FILE_EXT: &str = "bloom";
const COMPACT_DATA_FILE_EXT: &str = "compact_data";
const COMPACT_INDEX_FILE_EXT: &str = "compact_index";
const COMPACT_BLOOM_FILE_EXT: &str = "compact_bloom";
const COMPACT_ACTION_FILE_EXT: &str = "compact_action";

/// An `EntryOffset` item size ater serialization with bincode.
const INDEX_ENTRY_SIZE: usize = 16;

#[derive(Kinded)]
#[kinded(derive(Hash))]
pub enum FileType {
    Memtable,
    Data,
    Index,
    Bloom,
}

// Remember to change the INDEX_ENTRY_SIZE const when you change this struct.
#[derive(Debug, Serialize, Deserialize, Default)]
struct EntryOffset {
    offset: u64,
    key_size: u32,
    full_size: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EntryValue {
    pub data: Vec<u8>,
    #[serde(with = "timestamp_nanos")]
    pub timestamp: OffsetDateTime,
}

impl EntryValue {
    fn new(data: Vec<u8>, timestamp: Option<OffsetDateTime>) -> Self {
        Self {
            data,
            timestamp: timestamp.unwrap_or_else(OffsetDateTime::now_utc),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Entry {
    // Key must be the first field (binary search assumes this).
    pub key: Vec<u8>,
    pub value: EntryValue,
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then(self.value.timestamp.cmp(&other.value.timestamp))
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for Entry {}
