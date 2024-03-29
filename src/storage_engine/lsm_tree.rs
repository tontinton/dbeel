use bincode::Options;
use bloomfilter::Bloom;
use futures::{try_join, AsyncReadExt, AsyncWriteExt};
use glommio::{
    enclose,
    io::{
        remove, rename, DmaBuffer, DmaFile, DmaStreamReaderBuilder,
        DmaStreamWriterBuilder, OpenOptions,
    },
    spawn_local,
    timer::sleep,
};
use log::{error, trace};
use rbtree_arena::RedBlackTree;
use regex::Regex;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use std::{
    cell::{Cell, RefCell},
    cmp::Ordering,
    collections::BinaryHeap,
    path::{Path, PathBuf},
    rc::Rc,
    time::Duration,
};

use super::{
    cached_file_reader::{CachedFileReader, FileId},
    entry_writer::EntryWriter,
    page_cache::{PartitionPageCache, PAGE_SIZE},
    Entry, EntryOffset, EntryValue, FileTypeKind, BLOOM_FILE_EXT,
    COMPACT_ACTION_FILE_EXT, COMPACT_BLOOM_FILE_EXT, COMPACT_DATA_FILE_EXT,
    COMPACT_INDEX_FILE_EXT, DATA_FILE_EXT, DEFAULT_SSTABLE_BLOOM_MIN_SIZE,
    DEFAULT_TREE_CAPACITY, DMA_STREAM_NUMBER_OF_BUFFERS, INDEX_ENTRY_SIZE,
    INDEX_FILE_EXT, INDEX_PADDING, MEMTABLE_FILE_EXT, TOMBSTONE,
};
use crate::{
    error::{Error, Result},
    utils::{
        bincode::bincode_options,
        get_first_capture,
        local_event::{LocalEvent, LocalEventListener},
    },
};

/// The acceptable error rate in the bloom filter value in (0, 1].
const BLOOM_MAX_ALLOWED_ERROR: f64 = 0.01;

type MemTable = RedBlackTree<Vec<u8>, EntryValue>;

#[derive(Eq, PartialEq)]
struct CompactionItem {
    entry: Entry,
    index: usize,
}

impl Ord for CompactionItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .entry
            .cmp(&self.entry)
            .then(other.index.cmp(&self.index))
    }
}

impl PartialOrd for CompactionItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Serialize, Deserialize)]
struct CompactionAction {
    renames: Vec<(PathBuf, PathBuf)>,
    deletes: Vec<PathBuf>,
}

#[derive(Clone)]
struct SSTable {
    index: usize,
    size: u64,
    data_file: Rc<DmaFile>,
    index_file: Rc<DmaFile>,
    bloom: Option<Rc<Bloom<Vec<u8>>>>,
}

impl SSTable {
    async fn new_with_bloom_read(
        dir: &Path,
        index: usize,
        size: u64,
    ) -> Result<Self> {
        let bloom_path = get_file_path(dir, index, BLOOM_FILE_EXT);
        let bloom = if bloom_path.exists() {
            let buf = read_file(&bloom_path).await?;
            let bloom: Bloom<Vec<u8>> = bincode_options().deserialize(&buf)?;
            Some(Rc::new(bloom))
        } else {
            None
        };

        Self::new(dir, index, size, bloom).await
    }

    async fn new(
        dir: &Path,
        index: usize,
        size: u64,
        bloom: Option<Rc<Bloom<Vec<u8>>>>,
    ) -> Result<Self> {
        let (data_path, index_path) = get_data_file_paths(dir, index);
        let data_file = Rc::new(DmaFile::open(&data_path).await?);
        let index_file = Rc::new(DmaFile::open(&index_path).await?);
        Ok(Self {
            index,
            size,
            data_file,
            index_file,
            bloom,
        })
    }

    async fn close(&self) -> Result<()> {
        try_join!(
            self.data_file.clone().close_rc(),
            self.index_file.clone().close_rc()
        )?;
        Ok(())
    }
}

enum IterState {
    UnreadSSTable(usize),
    ReadingSSTable(usize, CachedFileReader, CachedFileReader, u64, u64),
    Memtable,
}

type IterFilterFn = dyn FnMut(&[u8], &EntryValue) -> bool;

pub struct AsyncIter<'a> {
    tree: &'a LSMTree,
    sstables: Rc<Vec<SSTable>>,
    memtable: std::vec::IntoIter<Entry>,
    filter_fn: Box<IterFilterFn>,
    state: IterState,

    /// Optimization - read into this buffer when reading an entry from the
    /// index, instead of allocating a new buffer each time.
    index_buffer: [u8; INDEX_ENTRY_SIZE],
}

impl<'a> AsyncIter<'a> {
    fn new(tree: &'a LSMTree, mut filter_fn: Box<IterFilterFn>) -> Self {
        let mut memtable: Vec<Entry> = Vec::new();
        if let Some(tree) = tree.flush_memtable.borrow().as_ref() {
            memtable.extend(tree.iter().filter(|(k, v)| filter_fn(k, v)).map(
                |(k, v)| Entry {
                    key: k.clone(),
                    value: v.clone(),
                },
            ));
        }
        memtable.extend(
            tree.active_memtable
                .borrow()
                .iter()
                .filter(|(k, v)| filter_fn(k, v))
                .map(|(k, v)| Entry {
                    key: k.clone(),
                    value: v.clone(),
                }),
        );

        // The sstable files we operate on will not be deleted until this
        // object is dropped.
        let sstables = tree.sstables.borrow().clone();

        let state = if sstables.len() > 0 {
            IterState::UnreadSSTable(0)
        } else {
            IterState::Memtable
        };

        Self {
            tree,
            sstables,
            memtable: memtable.into_iter(),
            filter_fn: Box::new(filter_fn),
            state,
            index_buffer: [0; INDEX_ENTRY_SIZE],
        }
    }
}

enum NextEntryResult {
    Found(Option<Entry>),
    Continue,
}

impl<'a> AsyncIter<'a> {
    pub async fn next(&mut self) -> Result<Option<Entry>> {
        loop {
            if let NextEntryResult::Found(result) = self.read_one().await? {
                return Ok(result);
            }
        }
    }

    async fn read_one(&mut self) -> Result<NextEntryResult> {
        use NextEntryResult::{Continue, Found};

        Ok(match &mut self.state {
            IterState::UnreadSSTable(i) => {
                let i = *i;
                let sstable = &self.sstables[i];

                let data_file = CachedFileReader::new(
                    (FileTypeKind::Data, sstable.index),
                    sstable.data_file.clone(),
                    self.tree.page_cache.clone(),
                );

                // let index_file = DmaFile::open(&sstable.index_path).await?;
                let index_file_size = sstable.size * (INDEX_ENTRY_SIZE as u64);
                let index_file = CachedFileReader::new(
                    (FileTypeKind::Index, sstable.index),
                    sstable.index_file.clone(),
                    self.tree.page_cache.clone(),
                );

                self.state = IterState::ReadingSSTable(
                    i,
                    data_file,
                    index_file,
                    0,
                    index_file_size,
                );
                Continue
            }
            IterState::ReadingSSTable(
                i,
                data_file,
                index_file,
                index_offset,
                index_file_size,
            ) => {
                let i = *i;

                index_file
                    .read_at_into(*index_offset, &mut self.index_buffer)
                    .await?;
                let entry_offset: EntryOffset =
                    bincode_options().deserialize(&self.index_buffer)?;
                let entry: Entry = bincode_options().deserialize(
                    &data_file
                        .read_at(
                            entry_offset.offset,
                            entry_offset.full_size as usize,
                        )
                        .await?,
                )?;

                *index_offset += INDEX_ENTRY_SIZE as u64;
                if *index_offset >= *index_file_size {
                    self.state = if i == self.sstables.len() - 1 {
                        IterState::Memtable
                    } else {
                        IterState::UnreadSSTable(i + 1)
                    };
                };

                if (self.filter_fn)(&entry.key, &entry.value) {
                    Found(Some(entry))
                } else {
                    Continue
                }
            }
            IterState::Memtable => Found(self.memtable.next()),
        })
    }
}

fn get_file_path(dir: &Path, index: usize, ext: &str) -> PathBuf {
    let mut path = dir.to_path_buf();
    path.push(format!("{index:0INDEX_PADDING$}.{ext}"));
    path
}

fn get_data_file_paths(dir: &Path, index: usize) -> (PathBuf, PathBuf) {
    let data_path = get_file_path(dir, index, DATA_FILE_EXT);
    let index_path = get_file_path(dir, index, INDEX_FILE_EXT);
    (data_path, index_path)
}

fn get_compaction_file_paths(
    dir: &Path,
    index: usize,
) -> (PathBuf, PathBuf, PathBuf) {
    let data_path = get_file_path(dir, index, COMPACT_DATA_FILE_EXT);
    let index_path = get_file_path(dir, index, COMPACT_INDEX_FILE_EXT);
    let bloom_path = get_file_path(dir, index, COMPACT_BLOOM_FILE_EXT);
    (data_path, index_path, bloom_path)
}

fn create_file_path_regex(file_ext: &'static str) -> Result<Regex> {
    let pattern = format!(r#"^(\d+)\.{file_ext}$"#);
    Regex::new(pattern.as_str())
        .map_err(|source| Error::RegexCreationError { source, pattern })
}

async fn write_file(path: &Path, buf: &[u8]) -> Result<()> {
    let file = DmaFile::create(path).await?;
    let mut writer = DmaStreamWriterBuilder::new(file)
        .with_buffer_size(PAGE_SIZE)
        .with_write_behind(DMA_STREAM_NUMBER_OF_BUFFERS)
        .build();
    writer.write_all(buf).await?;
    writer.close().await?;
    Ok(())
}

async fn read_file(path: &Path) -> Result<Vec<u8>> {
    let file = DmaFile::open(path).await?;
    let mut reader = DmaStreamReaderBuilder::new(file)
        .with_buffer_size(PAGE_SIZE)
        .with_read_ahead(DMA_STREAM_NUMBER_OF_BUFFERS)
        .build();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    reader.close().await?;
    Ok(buf)
}

pub struct LSMTree {
    dir: PathBuf,

    /// The page cache to ensure skipping kernel code when reading / writing to
    /// disk.
    page_cache: Rc<PartitionPageCache<FileId>>,

    /// The memtable that is currently being written to.
    active_memtable: RefCell<MemTable>,

    /// The memtable that is currently being flushed to disk.
    flush_memtable: RefCell<Option<MemTable>>,

    /// Notified when a flush is started (active tree is empty + wal file updated).
    flush_start_event: LocalEvent,

    /// Notified when a flush is finished (sstables updated).
    flush_done_event: LocalEvent,

    /// The next sstable index that is going to be written.
    write_sstable_index: Cell<usize>,

    /// The sstables to query from.
    /// Rc tracks the number of sstable file reads are happening.
    /// The reason for tracking is that when ending a compaction, there are
    /// sstable files that should be removed / replaced, but there could be
    /// reads to the same files concurrently, so the compaction process will
    /// wait for the number of reads to reach 0.
    sstables: RefCell<Rc<Vec<SSTable>>>,

    /// The next memtable index.
    memtable_index: Cell<usize>,

    /// The memtable WAL for durability in case the process crashes without
    /// flushing the memtable to disk.
    wal_file: RefCell<Rc<DmaFile>>,

    /// The current end offset of the wal file.
    wal_offset: Cell<u64>,

    /// The fsync delay, for when throughput is more desirable than latency.
    /// When None, never sync after a write.
    wal_sync_delay: Option<Duration>,

    /// Used for waiting for a scheduled wal sync.
    wal_sync_event: RefCell<Option<Rc<LocalEvent>>>,

    ///The minimum size of an sstable (in bytes) to calculate and store its bloom filter.
    sstable_bloom_min_size: u64,
}

impl LSMTree {
    pub async fn open_or_create(
        dir: PathBuf,
        page_cache: PartitionPageCache<FileId>,
    ) -> Result<Self> {
        Self::open_or_create_ex(
            dir,
            page_cache,
            DEFAULT_TREE_CAPACITY,
            None,
            DEFAULT_SSTABLE_BLOOM_MIN_SIZE,
        )
        .await
    }

    pub async fn open_or_create_ex(
        dir: PathBuf,
        page_cache: PartitionPageCache<FileId>,
        tree_capacity: usize,
        wal_sync_delay: Option<Duration>,
        sstable_bloom_min_size: u64,
    ) -> Result<Self> {
        assert_eq!(
            bincode_options()
                .serialized_size(&EntryOffset::default())
                .unwrap(),
            INDEX_ENTRY_SIZE as u64
        );

        if !dir.is_dir() {
            trace!("Creating new tree in: {:?}", dir);
            std::fs::create_dir_all(&dir)?;
        } else {
            trace!("Opening existing tree from: {:?}", dir);
        }

        let page_cache = Rc::new(page_cache);

        let pattern = create_file_path_regex(COMPACT_ACTION_FILE_EXT)?;
        let compact_action_paths: Vec<PathBuf> = std::fs::read_dir(&dir)?
            .filter_map(std::result::Result::ok)
            .filter(|entry| get_first_capture(&pattern, entry).is_some())
            .map(|entry| entry.path())
            .collect();
        for compact_action_path in &compact_action_paths {
            let buf = read_file(compact_action_path).await?;
            while let Ok(action) = bincode_options()
                .deserialize_from::<_, CompactionAction>(&mut &buf[..])
            {
                Self::run_compaction_action(&action).await?;
            }
            Self::remove_file_log_on_err(compact_action_path).await;
        }

        let pattern = create_file_path_regex(DATA_FILE_EXT)?;
        let sstables: Vec<SSTable> = {
            let mut indices = std::fs::read_dir(&dir)?
                .filter_map(std::result::Result::ok)
                .filter_map(|entry| get_first_capture(&pattern, &entry))
                .filter_map(|n| n.parse::<usize>().ok())
                .collect::<Vec<_>>();
            indices.sort_unstable();

            let mut sstables = Vec::with_capacity(indices.len());
            for index in indices {
                let path = get_file_path(&dir, index, INDEX_FILE_EXT);
                let size =
                    std::fs::metadata(path)?.len() / INDEX_ENTRY_SIZE as u64;
                sstables.push(
                    SSTable::new_with_bloom_read(&dir, index, size).await?,
                );
            }
            sstables
        };

        let write_file_index = sstables
            .iter()
            .map(|t| t.index)
            .max()
            .map_or(0, |i| (i + 2 - (i & 1)));

        let pattern = create_file_path_regex(MEMTABLE_FILE_EXT)?;
        let wal_indices: Vec<usize> = {
            let mut vec = std::fs::read_dir(&dir)?
                .filter_map(std::result::Result::ok)
                .filter_map(|entry| get_first_capture(&pattern, &entry))
                .filter_map(|n| n.parse::<usize>().ok())
                .collect::<Vec<_>>();
            vec.sort_unstable();
            vec
        };

        let wal_file_index = match wal_indices.len() {
            0 => 0,
            1 => wal_indices[0],
            2 => {
                // A flush did not finish for some reason, do it now.
                let wal_file_index = wal_indices[1];
                let unflashed_file_index = wal_indices[0];
                let unflashed_file_path = get_file_path(
                    &dir,
                    unflashed_file_index,
                    MEMTABLE_FILE_EXT,
                );
                let (data_file_path, index_file_path) =
                    get_data_file_paths(&dir, wal_file_index);
                let memtable = Self::read_memtable_from_wal_file(
                    &unflashed_file_path,
                    tree_capacity,
                )
                .await?;
                let (data_file, index_file) = try_join!(
                    DmaFile::create(&data_file_path),
                    DmaFile::create(&index_file_path)
                )?;
                Self::flush_memtable_to_disk(
                    memtable.into_iter().collect(),
                    data_file,
                    index_file,
                    wal_file_index,
                    page_cache.clone(),
                )
                .await?;
                remove(&unflashed_file_path).await?;
                wal_file_index
            }
            _ => panic!("Cannot have more than 2 WAL files"),
        };

        let wal_path = get_file_path(&dir, wal_file_index, MEMTABLE_FILE_EXT);
        let wal_file = OpenOptions::new()
            .write(true)
            .create(true)
            .dma_open(&wal_path)
            .await?;
        let wal_offset = wal_file.file_size().await?;

        let active_memtable = if wal_path.exists() {
            Self::read_memtable_from_wal_file(&wal_path, tree_capacity).await?
        } else {
            RedBlackTree::with_capacity(tree_capacity)
        };

        Ok(Self {
            dir,
            page_cache,
            active_memtable: RefCell::new(active_memtable),
            flush_memtable: RefCell::new(None),
            flush_start_event: LocalEvent::new(),
            flush_done_event: LocalEvent::new(),
            write_sstable_index: Cell::new(write_file_index),
            sstables: RefCell::new(Rc::new(sstables)),
            memtable_index: Cell::new(wal_file_index),
            wal_file: RefCell::new(Rc::new(wal_file)),
            wal_offset: Cell::new(wal_offset),
            wal_sync_delay,
            wal_sync_event: RefCell::new(None),
            sstable_bloom_min_size,
        })
    }

    pub fn purge(&self) -> Result<()> {
        trace!("Deleting tree in: {:?}", self.dir);
        Ok(std::fs::remove_dir_all(&self.dir)?)
    }

    async fn read_memtable_from_wal_file(
        wal_path: &Path,
        tree_capacity: usize,
    ) -> Result<MemTable> {
        let wal_buf = read_file(wal_path).await?;
        let mut cursor = std::io::Cursor::new(&wal_buf[..]);

        let mut memtable = RedBlackTree::with_capacity(tree_capacity);

        while cursor.position() < wal_buf.len() as u64 {
            if let Ok(entry) =
                bincode_options().deserialize_from::<_, Entry>(&mut cursor)
            {
                memtable.set(entry.key, entry.value)?;
            }
            let pos = cursor.position();
            cursor.set_position(
                pos + (PAGE_SIZE as u64) - pos % (PAGE_SIZE as u64),
            );
        }

        Ok(memtable)
    }

    async fn run_compaction_action(action: &CompactionAction) -> Result<()> {
        for path_to_delete in &action.deletes {
            if path_to_delete.exists() {
                Self::remove_file_log_on_err(path_to_delete).await;
            }
        }

        for (source_path, destination_path) in &action.renames {
            if source_path.exists() {
                rename(source_path, destination_path).await?;
            }
        }

        Ok(())
    }

    pub fn sstable_indices_and_sizes(&self) -> Vec<(usize, u64)> {
        self.sstables
            .borrow()
            .iter()
            .map(|sstable| (sstable.index, sstable.size))
            .collect()
    }

    fn active_memtable_full(&self) -> bool {
        self.active_memtable.borrow().capacity()
            == self.active_memtable.borrow().len()
    }

    async fn binary_search(
        key: &Vec<u8>,
        data_file: &CachedFileReader,
        index_file: &CachedFileReader,
        index_offset_start: u64,
        index_offset_length: u64,
    ) -> Result<Option<(Entry, u64)>> {
        let mut half = index_offset_length / 2;
        let mut high_index = index_offset_length - 1;
        let mut low_index = 0;

        let mut index_buf = [0; INDEX_ENTRY_SIZE];

        // do:
        loop {
            let current_index_offset =
                index_offset_start + half * (INDEX_ENTRY_SIZE as u64);
            index_file
                .read_at_into(current_index_offset, &mut index_buf)
                .await?;
            let current: EntryOffset =
                bincode_options().deserialize(&index_buf)?;

            let raw_key: Vec<u8> = bincode_options().deserialize(
                &data_file
                    .read_at(current.offset, current.key_size as usize)
                    .await?,
            )?;

            match raw_key.cmp(key) {
                Ordering::Equal => {
                    let entry_value: EntryValue = bincode_options()
                        .deserialize(
                            &data_file
                                .read_at(
                                    current.offset
                                        + u64::from(current.key_size),
                                    (current.full_size - current.key_size)
                                        as usize,
                                )
                                .await?,
                        )?;
                    let entry = Entry {
                        key: raw_key,
                        value: entry_value,
                    };
                    return Ok(Some((entry, current_index_offset)));
                }
                Ordering::Less => low_index = half + 1,
                Ordering::Greater => high_index = std::cmp::max(half, 1) - 1,
            }

            if half == 0 || half == index_offset_length {
                break;
            }

            half = (high_index + low_index) / 2;

            // while not:
            if low_index > high_index {
                break;
            }
        }

        Ok(None)
    }

    /// Get the value together with the metadata saved for a key.
    /// If you only want the raw value, use get().
    pub async fn get_entry(&self, key: &Vec<u8>) -> Result<Option<EntryValue>> {
        // Query the active tree first.
        if let Some(result) = self.active_memtable.borrow().get(key) {
            return Ok(Some(result.clone()));
        }

        // Key not found in active tree, query the flushed tree.
        if let Some(tree) = self.flush_memtable.borrow().as_ref() {
            if let Some(result) = tree.get(key) {
                return Ok(Some(result.clone()));
            }
        }

        // Key not found in memory, query all files from the newest to the
        // oldest.
        let sstables = self.sstables.borrow().clone();
        for sstable in sstables.iter().rev() {
            // Skip keys that are not in the sstable.
            if let Some(bloom) = &sstable.bloom {
                if !bloom.check(key) {
                    continue;
                }
            }

            let data_file = CachedFileReader::new(
                (FileTypeKind::Data, sstable.index),
                sstable.data_file.clone(),
                self.page_cache.clone(),
            );
            let index_file = CachedFileReader::new(
                (FileTypeKind::Index, sstable.index),
                sstable.index_file.clone(),
                self.page_cache.clone(),
            );

            if let Some((entry, _)) = Self::binary_search(
                key,
                &data_file,
                &index_file,
                0,
                sstable.size,
            )
            .await?
            {
                return Ok(Some(entry.value));
            }
        }

        Ok(None)
    }

    /// Get the raw value saved for a key.
    /// If you prefer to also get metadata of the value, use `get_entry`().
    pub async fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>> {
        Ok(self.get_entry(key).await?.map(|v| v.data))
    }

    async fn set_ex(
        self: Rc<Self>,
        key: Vec<u8>,
        value: Vec<u8>,
        timestamp: Option<OffsetDateTime>,
    ) -> Result<Option<EntryValue>> {
        let value = EntryValue::new(value, timestamp);
        let entry = Entry { key, value };

        let entry_size = bincode_options().serialized_size(&entry)? as usize;
        let size_padded = entry_size + PAGE_SIZE - (entry_size % PAGE_SIZE);
        let mut dma_buffer =
            self.wal_file.borrow().alloc_dma_buffer(size_padded);
        bincode_options().serialize_into(dma_buffer.as_bytes_mut(), &entry)?;

        // Wait until the active tree has space to fill.
        while self.active_memtable_full() {
            self.flush_start_event.listen().await;
        }

        // Write to memtable in memory.
        let result = self
            .active_memtable
            .borrow_mut()
            .set(entry.key, entry.value)?;

        if self.active_memtable_full() {
            // Capacity is full, flush memtable to disk in background.
            spawn_local(enclose!((self.clone() => tree) async move {
                if let Err(e) = tree.flush().await {
                    error!("Failed to flush memtable: {}", e);
                }
            }))
            .detach();
        }

        // Write to WAL for persistance.
        self.write_to_wal(dma_buffer).await?;

        Ok(result)
    }

    pub async fn set(
        self: Rc<Self>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<Option<EntryValue>> {
        self.set_ex(key, value, None).await
    }

    pub async fn set_with_timestamp(
        self: Rc<Self>,
        key: Vec<u8>,
        value: Vec<u8>,
        timestamp: OffsetDateTime,
    ) -> Result<Option<EntryValue>> {
        self.set_ex(key, value, Some(timestamp)).await
    }

    pub async fn delete(
        self: Rc<Self>,
        key: Vec<u8>,
    ) -> Result<Option<EntryValue>> {
        self.set(key, TOMBSTONE).await
    }

    pub async fn delete_with_timestamp(
        self: Rc<Self>,
        key: Vec<u8>,
        timestamp: OffsetDateTime,
    ) -> Result<Option<EntryValue>> {
        self.set_with_timestamp(key, TOMBSTONE, timestamp).await
    }

    async fn write_to_wal(&self, buffer: DmaBuffer) -> Result<()> {
        let file = self.wal_file.borrow().clone();

        let offset = self.wal_offset.get();
        self.wal_offset.set(offset + buffer.len() as u64);

        file.write_at(buffer, offset).await?;

        match self.wal_sync_delay {
            Some(delay) if delay.is_zero() => {
                file.fdatasync().await?;
            }
            Some(delay) => {
                let maybe_event =
                    self.wal_sync_event.borrow().as_ref().cloned();
                if let Some(event) = maybe_event {
                    event.listen().await;
                } else {
                    let event = Rc::new(LocalEvent::new());

                    self.wal_sync_event.replace(Some(event.clone()));
                    sleep(delay).await;
                    self.wal_sync_event.replace(None);

                    file.fdatasync().await?;
                    event.notify();
                }
            }
            None => {}
        }

        Ok(())
    }

    /// Wait until a flush occures.
    pub fn get_flush_event_listener(&self) -> LocalEventListener {
        self.flush_done_event.listen()
    }

    pub async fn flush(&self) -> Result<()> {
        // Wait until the previous flush is finished.
        while self.flush_memtable.borrow().is_some() {
            self.get_flush_event_listener().await;
        }

        if self.active_memtable.borrow().is_empty() {
            return Ok(());
        }

        let memtable_to_flush = RedBlackTree::with_capacity(
            self.active_memtable.borrow().capacity(),
        );
        self.flush_memtable
            .replace(Some(self.active_memtable.replace(memtable_to_flush)));

        self.memtable_index.set(self.memtable_index.get() + 2);

        let next_wal_path = get_file_path(
            &self.dir,
            self.memtable_index.get(),
            MEMTABLE_FILE_EXT,
        );
        let old_wal_file = self
            .wal_file
            .replace(Rc::new(DmaFile::create(&next_wal_path).await?));
        self.wal_offset.set(0);
        self.wal_sync_event.replace(None);

        self.flush_start_event.notify();

        let (data_filename, index_filename) =
            get_data_file_paths(&self.dir, self.write_sstable_index.get());
        let (data_file, index_file) = try_join!(
            DmaFile::create(&data_filename),
            DmaFile::create(&index_filename)
        )?;

        let vec = self
            .flush_memtable
            .borrow()
            .as_ref()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let items_written = Self::flush_memtable_to_disk(
            vec,
            data_file,
            index_file,
            self.write_sstable_index.get(),
            self.page_cache.clone(),
        )
        .await?;

        self.flush_memtable.replace(None);

        // Replace sstables with new list containing the flushed sstable.
        {
            let mut sstables: Vec<SSTable> =
                self.sstables.borrow().iter().cloned().collect();

            let index = self.write_sstable_index.get();
            sstables.push(
                SSTable::new(&self.dir, index, items_written as u64, None)
                    .await?,
            );

            self.sstables.replace(Rc::new(sstables));

            self.write_sstable_index.set(index + 2);
        }

        self.flush_done_event.notify();

        old_wal_file.clone().close_rc().await?;
        old_wal_file.remove().await?;

        Ok(())
    }

    async fn flush_memtable_to_disk(
        memtable: Vec<(Vec<u8>, EntryValue)>,
        data_file: DmaFile,
        index_file: DmaFile,
        files_index: usize,
        page_cache: Rc<PartitionPageCache<FileId>>,
    ) -> Result<usize> {
        let table_length = memtable.len();

        let mut entry_writer = EntryWriter::new_from_dma(
            data_file,
            index_file,
            files_index,
            page_cache,
        );
        for (key, value) in memtable {
            entry_writer.write(&Entry { key, value }).await?;
        }
        entry_writer.close().await?;

        Ok(table_length)
    }

    /// Compact all sstables in the given list of sstable files, write the result
    /// to the output file given.
    pub async fn compact(
        &self,
        indices_to_compact: &[usize],
        output_index: usize,
        keep_tombstones: bool,
    ) -> Result<()> {
        let sstable_paths: Vec<(PathBuf, PathBuf, PathBuf)> =
            indices_to_compact
                .iter()
                .map(|i| {
                    let (data_path, index_path) =
                        get_data_file_paths(&self.dir, *i);
                    let bloom_path =
                        get_file_path(&self.dir, *i, BLOOM_FILE_EXT);
                    (data_path, index_path, bloom_path)
                })
                .collect();

        // No stable AsyncIterator yet...
        // If there was, itertools::kmerge would probably solve it all.
        let mut sstable_readers = Vec::with_capacity(sstable_paths.len());
        let mut max_items_after_compaction = 0usize;
        let mut max_data_size_after_compaction = 0u64;

        for (data_path, index_path, _) in &sstable_paths {
            let (data_file, index_file) =
                try_join!(DmaFile::open(data_path), DmaFile::open(index_path))?;

            let index_file_size = index_file.file_size().await?;
            let num_entries = index_file_size / INDEX_ENTRY_SIZE as u64;
            max_items_after_compaction += num_entries as usize;

            max_data_size_after_compaction += data_file.file_size().await?;

            let data_reader = DmaStreamReaderBuilder::new(data_file)
                .with_buffer_size(PAGE_SIZE)
                .with_read_ahead(DMA_STREAM_NUMBER_OF_BUFFERS)
                .build();
            let index_reader = DmaStreamReaderBuilder::new(index_file)
                .with_buffer_size(PAGE_SIZE)
                .with_read_ahead(DMA_STREAM_NUMBER_OF_BUFFERS)
                .build();
            sstable_readers.push((data_reader, index_reader));
        }

        let (compact_data_path, compact_index_path, compact_bloom_path) =
            get_compaction_file_paths(&self.dir, output_index);
        let (compact_data_file, compact_index_file) = try_join!(
            DmaFile::create(&compact_data_path),
            DmaFile::create(&compact_index_path)
        )?;

        let mut offset_bytes = vec![0; INDEX_ENTRY_SIZE];
        let mut heap = BinaryHeap::new();

        for (index, (data_reader, index_reader)) in
            sstable_readers.iter_mut().enumerate()
        {
            let entry_result = Self::read_next_entry(
                data_reader,
                index_reader,
                &mut offset_bytes,
            )
            .await;
            if let Ok(entry) = entry_result {
                heap.push(CompactionItem { entry, index });
            }
        }

        let mut entry_writer = EntryWriter::new_from_dma(
            compact_data_file,
            compact_index_file,
            output_index,
            self.page_cache.clone(),
        );

        let mut maybe_bloom =
            if max_data_size_after_compaction > self.sstable_bloom_min_size {
                Some(Bloom::new_for_fp_rate(
                    max_items_after_compaction,
                    BLOOM_MAX_ALLOWED_ERROR,
                ))
            } else {
                None
            };

        let mut items_written = 0;

        while let Some(current) = heap.pop() {
            let index = current.index;

            let mut should_write_current = true;
            if let Some(next) = heap.peek() {
                should_write_current &= next.entry.key != current.entry.key;
            }
            should_write_current &=
                keep_tombstones || current.entry.value.data != TOMBSTONE;

            if should_write_current {
                if let Some(ref mut bloom) = maybe_bloom {
                    bloom.set(&current.entry.key);
                }
                entry_writer.write(&current.entry).await?;
                items_written += 1;
            }

            let (data_reader, index_reader) = &mut sstable_readers[index];
            let entry_result = Self::read_next_entry(
                data_reader,
                index_reader,
                &mut offset_bytes,
            )
            .await;
            if let Ok(entry) = entry_result {
                heap.push(CompactionItem { entry, index });
            }
        }

        entry_writer.close().await?;

        if let Some(ref bloom) = maybe_bloom {
            write_file(
                &compact_bloom_path,
                &bincode_options().serialize(&bloom)?,
            )
            .await?;
        }

        let mut files_to_delete = Vec::with_capacity(sstable_paths.len() * 3);
        for (data_path, index_path, bloom_path) in sstable_paths {
            files_to_delete.push(data_path);
            files_to_delete.push(index_path);
            files_to_delete.push(bloom_path);
        }

        let (output_data_path, output_index_path) =
            get_data_file_paths(&self.dir, output_index);
        let output_bloom_path =
            get_file_path(&self.dir, output_index, BLOOM_FILE_EXT);

        let action = CompactionAction {
            renames: vec![
                (compact_data_path, output_data_path),
                (compact_index_path, output_index_path),
                (compact_bloom_path, output_bloom_path),
            ],
            deletes: files_to_delete,
        };

        let compact_action_path =
            get_file_path(&self.dir, output_index, COMPACT_ACTION_FILE_EXT);
        write_file(
            &compact_action_path,
            &bincode_options().serialize(&action)?,
        )
        .await?;

        for (source_path, destination_path) in &action.renames {
            if source_path.exists() {
                rename(source_path, destination_path).await?;
            }
        }

        let old_sstables = self.sstables.borrow().clone();

        {
            let (sstables_to_close, mut sstables): (
                Vec<SSTable>,
                Vec<SSTable>,
            ) = old_sstables
                .iter()
                .cloned()
                .partition(|x| indices_to_compact.contains(&x.index));

            for x in sstables_to_close {
                x.close().await?;
            }
            sstables.push(
                SSTable::new(
                    &self.dir,
                    output_index,
                    items_written,
                    maybe_bloom.map(Rc::new),
                )
                .await?,
            );
            sstables.sort_unstable_by_key(|t| t.index);

            self.sstables.replace(Rc::new(sstables));
        }

        // Block the current execution task until all currently running read
        // tasks finish, to make sure we don't delete files that are being read.
        while Rc::strong_count(&old_sstables) > 1 {
            futures_lite::future::yield_now().await;
        }

        for path_to_delete in &action.deletes {
            if path_to_delete.exists() {
                Self::remove_file_log_on_err(path_to_delete).await;
            }
        }

        Self::remove_file_log_on_err(&compact_action_path).await;

        Ok(())
    }

    async fn read_next_entry(
        data_reader: &mut (impl AsyncReadExt + Unpin),
        index_reader: &mut (impl AsyncReadExt + Unpin),
        offset_bytes: &mut [u8],
    ) -> Result<Entry> {
        index_reader.read_exact(offset_bytes).await?;
        let entry_offset: EntryOffset =
            bincode_options().deserialize(offset_bytes)?;
        let mut data_bytes = vec![0; entry_offset.full_size as usize];
        data_reader.read_exact(&mut data_bytes).await?;
        let entry: Entry = bincode_options().deserialize(&data_bytes)?;
        Ok(entry)
    }

    async fn remove_file_log_on_err(file_path: &PathBuf) {
        if let Err(e) = remove(file_path).await {
            error!(
                "Failed to remove file '{}', that is irrelevant after \
                 compaction: {}",
                file_path.display(),
                e
            );
        }
    }

    pub fn iter(&self) -> AsyncIter {
        AsyncIter::new(self, Box::new(|_, _| true))
    }

    pub fn iter_filter(&self, filter_fn: Box<IterFilterFn>) -> AsyncIter {
        AsyncIter::new(self, filter_fn)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    use ctor::ctor;
    use futures::{io::Cursor, AsyncWrite, Future};
    use glommio::{LocalExecutorBuilder, Placement};
    use tempfile::tempdir;

    use crate::storage_engine::page_cache::PageCache;

    use super::*;

    const TEST_TREE_CAPACITY: usize = 32;

    #[ctor]
    fn init_color_backtrace() {
        color_backtrace::install();
    }

    type GlobalCache = Rc<RefCell<PageCache<FileId>>>;

    async fn test_lsm_tree(
        dir: PathBuf,
        page_cache: PartitionPageCache<FileId>,
    ) -> Result<LSMTree> {
        LSMTree::open_or_create_ex(
            dir,
            page_cache,
            TEST_TREE_CAPACITY,
            None,
            DEFAULT_SSTABLE_BLOOM_MIN_SIZE,
        )
        .await
    }

    fn partitioned_cache(cache: &GlobalCache) -> PartitionPageCache<FileId> {
        PartitionPageCache::new_named("test", cache.clone()).unwrap()
    }

    fn run_with_glommio<G, F, T>(fut_gen: G) -> Result<()>
    where
        G: FnOnce(PathBuf, GlobalCache) -> F + Send + 'static,
        F: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let builder = LocalExecutorBuilder::new(Placement::Unbound);
        let handle = builder.name("test").spawn(|| async move {
            let dir = tempdir().unwrap().into_path();
            let cache = Rc::new(RefCell::new(PageCache::new(1024, 1024)));
            let result = fut_gen(dir.clone(), cache).await;
            std::fs::remove_dir_all(dir).unwrap();
            result
        })?;
        handle.join()?;
        Ok(())
    }

    async fn _set_and_get_memtable(
        dir: PathBuf,
        cache: GlobalCache,
    ) -> Result<()> {
        // New tree.
        {
            let tree = Rc::new(
                test_lsm_tree(dir.clone(), partitioned_cache(&cache)).await?,
            );
            tree.clone().set(vec![100], vec![200]).await?;
            assert_eq!(tree.get(&vec![100]).await?, Some(vec![200]));
            assert_eq!(tree.get(&vec![0]).await?, None);
        }

        // Reopening the tree.
        {
            let tree = test_lsm_tree(dir, partitioned_cache(&cache)).await?;
            assert_eq!(tree.get(&vec![100]).await?, Some(vec![200]));
            assert_eq!(tree.get(&vec![0]).await?, None);
        }

        Ok(())
    }

    #[test]
    fn set_and_get_memtable() -> Result<()> {
        run_with_glommio(_set_and_get_memtable)
    }

    async fn _set_and_get_sstable(
        dir: PathBuf,
        cache: GlobalCache,
    ) -> Result<()> {
        // New tree.
        {
            let tree = Rc::new(
                test_lsm_tree(dir.clone(), partitioned_cache(&cache)).await?,
            );
            assert_eq!(tree.write_sstable_index.get(), 0);

            let values: Vec<Vec<u8>> = (0..TEST_TREE_CAPACITY as u16)
                .map(|n| n.to_le_bytes().to_vec())
                .collect();

            for v in values {
                tree.clone().set(v.clone(), v).await?;
            }
            tree.clone().flush().await?;

            assert_eq!(tree.active_memtable.borrow().len(), 0);
            assert_eq!(tree.write_sstable_index.get(), 2);
            assert_eq!(tree.get(&vec![0, 0]).await?, Some(vec![0, 0]));
            assert_eq!(tree.get(&vec![1, 0]).await?, Some(vec![1, 0]));
            assert_eq!(tree.get(&vec![10, 0]).await?, Some(vec![10, 0]));
        }

        // Reopening the tree.
        {
            let tree =
                test_lsm_tree(dir.clone(), partitioned_cache(&cache)).await?;
            assert_eq!(tree.active_memtable.borrow().len(), 0);
            assert_eq!(tree.write_sstable_index.get(), 2);
            assert_eq!(tree.get(&vec![0, 0]).await?, Some(vec![0, 0]));
            assert_eq!(tree.get(&vec![1, 0]).await?, Some(vec![1, 0]));
            assert_eq!(tree.get(&vec![10, 0]).await?, Some(vec![10, 0]));
        }

        Ok(())
    }

    #[test]
    fn set_and_get_sstable() -> Result<()> {
        run_with_glommio(_set_and_get_sstable)
    }

    async fn _get_after_compaction(
        dir: PathBuf,
        cache: GlobalCache,
    ) -> Result<()> {
        async fn validate_tree_iter_range_pre_delete(
            tree: &LSMTree,
        ) -> Result<()> {
            validate_tree_iter_range(
                tree,
                vec![1, 0],
                vec![5, 0],
                vec![vec![1, 0], vec![2, 0], vec![3, 0], vec![4, 0]],
            )
            .await
        }

        async fn validate_tree_iter_range_post_delete(
            tree: &LSMTree,
        ) -> Result<()> {
            validate_tree_iter_range(
                tree,
                vec![1, 0],
                vec![5, 0],
                vec![
                    vec![1, 0],
                    vec![2, 0],
                    vec![3, 0],
                    vec![4, 0],
                    vec![],
                    vec![],
                ],
            )
            .await
        }

        async fn validate_tree_iter_range(
            tree: &LSMTree,
            start: Vec<u8>,
            end: Vec<u8>,
            expected: Vec<Vec<u8>>,
        ) -> Result<()> {
            let mut items = Vec::new();
            let mut iter = tree.iter_filter(Box::new(move |k, _| {
                k.cmp(&start) != Ordering::Less && k.cmp(&end) == Ordering::Less
            }));
            while let Some(entry) = iter.next().await? {
                items.push(entry.value.data);
            }
            assert_eq!(items, expected);
            Ok(())
        }

        async fn validate_tree_after_compaction(tree: &LSMTree) -> Result<()> {
            assert_eq!(
                *tree.sstable_indices_and_sizes(),
                vec![(5, (TEST_TREE_CAPACITY as u64) * 3 - 4)]
            );
            assert_eq!(tree.write_sstable_index.get(), 6);
            assert_eq!(tree.get(&vec![0, 0]).await?, Some(vec![0, 0]));
            assert_eq!(tree.get(&vec![2, 0]).await?, Some(vec![2, 0]));
            assert_eq!(tree.get(&vec![10, 0]).await?, Some(vec![10, 0]));
            assert_eq!(tree.get(&vec![1, 0]).await?, None);
            assert_eq!(tree.get(&vec![4, 0]).await?, None);
            validate_tree_iter_range(
                tree,
                vec![1, 0],
                vec![5, 0],
                vec![vec![2, 0], vec![3, 0]],
            )
            .await
        }

        // New tree.
        {
            let tree = Rc::new(
                test_lsm_tree(dir.clone(), partitioned_cache(&cache)).await?,
            );
            assert_eq!(tree.write_sstable_index.get(), 0);
            assert_eq!(*tree.sstable_indices_and_sizes(), vec![]);

            let values: Vec<Vec<u8>> = (0..((TEST_TREE_CAPACITY as u16) * 3)
                - 2)
                .map(|n| n.to_le_bytes().to_vec())
                .collect();

            for v in values {
                tree.clone().set(v.clone(), v).await?;
            }
            validate_tree_iter_range_pre_delete(&tree).await?;

            tree.clone().delete(vec![1, 0]).await?;
            tree.clone().delete(vec![4, 0]).await?;
            validate_tree_iter_range_post_delete(&tree).await?;

            tree.clone().flush().await?;
            validate_tree_iter_range_post_delete(&tree).await?;

            assert_eq!(
                *tree.sstable_indices_and_sizes(),
                vec![
                    (0, TEST_TREE_CAPACITY as u64),
                    (2, TEST_TREE_CAPACITY as u64),
                    (4, TEST_TREE_CAPACITY as u64)
                ]
            );

            tree.compact(&[0, 2, 4], 5, false).await?;
            validate_tree_after_compaction(&tree).await?;
        }

        // Reopening the tree.
        {
            let tree =
                test_lsm_tree(dir.clone(), partitioned_cache(&cache)).await?;
            validate_tree_after_compaction(&tree).await?;
        }

        Ok(())
    }

    #[test]
    fn get_after_compaction() -> Result<()> {
        run_with_glommio(_get_after_compaction)
    }

    #[derive(Clone)]
    struct RcCursorBuffer(Rc<RefCell<Cursor<Vec<u8>>>>);

    impl RcCursorBuffer {
        fn new() -> Self {
            Self(Rc::new(RefCell::new(Cursor::new(vec![]))))
        }
    }

    impl AsyncWrite for RcCursorBuffer {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::result::Result<usize, std::io::Error>> {
            let rc_cursor = &mut *self.0.borrow_mut();
            Pin::new(rc_cursor).poll_write(cx, buf)
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), std::io::Error>> {
            let rc_cursor = &mut *self.0.borrow_mut();
            Pin::new(rc_cursor).poll_flush(cx)
        }

        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let rc_cursor = &mut *self.0.borrow_mut();
            Pin::new(rc_cursor).poll_close(cx)
        }
    }

    async fn _entry_writer_cache_equals_disk(
        _dir: PathBuf,
        cache: GlobalCache,
    ) -> Result<()> {
        let test_partition_cache = Rc::new(partitioned_cache(&cache));

        let data_cursor = RcCursorBuffer::new();
        let index_cursor = RcCursorBuffer::new();

        let mut entry_writer = EntryWriter::new(
            Box::new(data_cursor.clone()),
            Box::new(index_cursor.clone()),
            0,
            test_partition_cache.clone(),
        );

        let entries = (0..TEST_TREE_CAPACITY)
            .map(|x| x.to_le_bytes().to_vec())
            .map(|x| Entry {
                key: x.clone(),
                value: EntryValue::new(x, None),
            })
            .collect::<Vec<_>>();

        let mut data_written = 0;
        let mut index_written = 0;
        for entry in entries {
            let (d, i) = entry_writer.write(&entry).await?;
            data_written += d;
            index_written += i;
        }
        entry_writer.close().await?;

        assert_eq!(data_cursor.0.borrow().get_ref().len(), data_written);
        assert_eq!(index_cursor.0.borrow().get_ref().len(), index_written);

        let cache_pages = (0..data_written).step_by(PAGE_SIZE).map(|address| {
            test_partition_cache
                .get_copied((FileTypeKind::Data, 0), address as u64)
                .unwrap_or_else(|| panic!("No cache on address: {address}"))
        });

        for (cache_page, chunk) in
            cache_pages.zip(data_cursor.0.borrow().get_ref().chunks(PAGE_SIZE))
        {
            assert_eq!(&cache_page[..chunk.len()], chunk);
        }

        let cache_pages =
            (0..index_written).step_by(PAGE_SIZE).map(|address| {
                test_partition_cache
                    .get_copied((FileTypeKind::Index, 0), address as u64)
                    .unwrap_or_else(|| panic!("No cache on address: {address}"))
            });

        for (cache_page, chunk) in
            cache_pages.zip(index_cursor.0.borrow().get_ref().chunks(PAGE_SIZE))
        {
            assert_eq!(&cache_page[..chunk.len()], chunk);
        }

        Ok(())
    }

    #[test]
    fn entry_writer_cache_equals_disk() -> Result<()> {
        run_with_glommio(_entry_writer_cache_equals_disk)
    }
}
