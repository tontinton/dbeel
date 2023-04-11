use crate::error::{Error, Result};
use bincode::{
    config::{
        FixintEncoding, RejectTrailing, WithOtherIntEncoding, WithOtherTrailing,
    },
    DefaultOptions, Options,
};
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::io::{
    BufferedFile, DmaFile, DmaStreamWriterBuilder, OpenOptions, StreamReader,
    StreamReaderBuilder, StreamWriterBuilder,
};
use once_cell::sync::Lazy;
use redblacktree::RedBlackTree;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{
    cell::{Cell, RefCell},
    cmp::Ordering,
    collections::BinaryHeap,
    fs::DirEntry,
    os::unix::prelude::MetadataExt,
    path::PathBuf,
    rc::Rc,
};

pub const TOMBSTONE: Vec<u8> = vec![];

const TREE_CAPACITY: usize = 1024;
const INDEX_PADDING: usize = 20; // Number of integers in max u64.

const MEMTABLE_FILE_EXT: &str = "memtable";
const DATA_FILE_EXT: &str = "data";
const INDEX_FILE_EXT: &str = "index";
const COMPACT_DATA_FILE_EXT: &str = "compact_data";
const COMPACT_INDEX_FILE_EXT: &str = "compact_index";
const COMPACT_ACTION_FILE_EXT: &str = "compact_action";

#[derive(Debug, Serialize, Deserialize)]
struct Entry {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
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

static INDEX_ENTRY_SIZE: Lazy<u64> = Lazy::new(|| {
    bincode_options()
        .serialized_size(&EntryOffset::default())
        .unwrap()
});

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
}

fn bincode_options() -> WithOtherIntEncoding<
    WithOtherTrailing<DefaultOptions, RejectTrailing>,
    FixintEncoding,
> {
    return DefaultOptions::new()
        .reject_trailing_bytes()
        .with_fixint_encoding();
}

fn get_file_path(dir: &PathBuf, index: usize, ext: &str) -> PathBuf {
    let mut path = dir.clone();
    path.push(format!("{0:01$}.{2}", index, INDEX_PADDING, ext));
    path
}

fn create_file_path_regex(file_ext: &'static str) -> Result<Regex> {
    let pattern = format!(r#"^(\d+)\.{}"#, file_ext);
    Regex::new(pattern.as_str())
        .map_err(|source| Error::RegexCreationError { source, pattern })
}

pub struct LSMTree {
    dir: PathBuf,

    // The memtable that is currently being written to.
    active_memtable: RefCell<RedBlackTree<Vec<u8>, Vec<u8>>>,

    // The memtable that is currently being flushed to disk.
    flush_memtable: RefCell<Option<RedBlackTree<Vec<u8>, Vec<u8>>>>,

    // The next sstable index that is going to be written.
    write_sstable_index: Cell<usize>,

    // The sstables to query from.
    // Rc tracks the number of sstable file reads are happening.
    // The reason for tracking is that when ending a compaction, there are
    // sstable files that should be removed / replaced, but there could be
    // reads to the same files concurrently, so the compaction process will
    // wait for the number of reads to reach 0.
    sstables: RefCell<Rc<Vec<SSTable>>>,

    // The next memtable index.
    memtable_index: Cell<usize>,

    // The memtable WAL for durability in case the process crashes without
    // flushing the memtable to disk.
    wal_file: RefCell<Rc<BufferedFile>>,

    // The current end offset of the wal file.
    wal_offset: Cell<u64>,

    // The filesystem block size of where our data is written to.
    block_size: u64,
}

impl LSMTree {
    pub async fn open_or_create(dir: PathBuf) -> Result<Self> {
        if !dir.is_dir() {
            std::fs::create_dir_all(&dir)?;
        }

        let pattern = create_file_path_regex(COMPACT_ACTION_FILE_EXT)?;
        let compact_action_paths: Vec<PathBuf> = std::fs::read_dir(&dir)?
            .filter_map(std::result::Result::ok)
            .filter(|entry| Self::get_first_capture(&pattern, &entry).is_some())
            .map(|entry| entry.path())
            .collect();
        for compact_action_path in &compact_action_paths {
            let file = BufferedFile::open(compact_action_path).await?;
            let mut reader = StreamReaderBuilder::new(file).build();

            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await?;
            let mut cursor = std::io::Cursor::new(&buf[..]);
            while let Ok(action) = bincode_options()
                .deserialize_from::<_, CompactionAction>(&mut cursor)
            {
                Self::run_compaction_action(&action)?;
            }
            reader.close().await?;
            Self::remove_file_log_on_err(compact_action_path);
        }

        let pattern = create_file_path_regex(DATA_FILE_EXT)?;
        let sstables: Vec<SSTable> = {
            let mut indices: Vec<usize> = std::fs::read_dir(&dir)?
                .filter_map(std::result::Result::ok)
                .filter_map(|entry| Self::get_first_capture(&pattern, &entry))
                .collect();
            indices.sort();

            let mut sstables = Vec::with_capacity(indices.len());
            for index in indices {
                let path = get_file_path(&dir, index, INDEX_FILE_EXT);
                let size = std::fs::metadata(path)?.len() / *INDEX_ENTRY_SIZE;
                sstables.push(SSTable { index, size });
            }
            sstables
        };

        let write_file_index = sstables
            .iter()
            .map(|t| t.index)
            .max()
            .map(|i| (i + 2 - (i & 1)))
            .unwrap_or(0);

        let pattern = create_file_path_regex(MEMTABLE_FILE_EXT)?;
        let wal_indices: Vec<usize> = {
            let mut vec: Vec<usize> = std::fs::read_dir(&dir)?
                .filter_map(std::result::Result::ok)
                .filter_map(|entry| Self::get_first_capture(&pattern, &entry))
                .collect();
            vec.sort();
            vec
        };

        let block_size = std::fs::metadata(dir.clone())?.blksize();

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
                    Self::get_data_file_paths(&dir, unflashed_file_index);
                let memtable = Self::read_memtable_from_wal_file(
                    &unflashed_file_path,
                    block_size,
                )
                .await?;
                let data_file = DmaFile::open(&data_file_path).await?;
                let index_file = DmaFile::open(&index_file_path).await?;
                Self::flush_memtable_to_disk(&memtable, data_file, index_file)
                    .await?;
                std::fs::remove_file(&unflashed_file_path)?;
                wal_file_index
            }
            _ => panic!("Cannot have more than 2 WAL files"),
        };

        let wal_path = get_file_path(&dir, wal_file_index, MEMTABLE_FILE_EXT);
        let wal_file = OpenOptions::new()
            .write(true)
            .create(true)
            .buffered_open(&wal_path)
            .await?;
        let wal_offset = wal_file.file_size().await?;

        let active_memtable = if wal_path.exists() {
            Self::read_memtable_from_wal_file(&wal_path, block_size).await?
        } else {
            RedBlackTree::with_capacity(TREE_CAPACITY)
        };

        Ok(Self {
            dir,
            active_memtable: RefCell::new(active_memtable),
            flush_memtable: RefCell::new(None),
            write_sstable_index: Cell::new(write_file_index),
            sstables: RefCell::new(Rc::new(sstables)),
            memtable_index: Cell::new(wal_file_index),
            wal_file: RefCell::new(Rc::new(wal_file)),
            wal_offset: Cell::new(wal_offset),
            block_size,
        })
    }

    pub fn purge(&self) -> Result<()> {
        Ok(std::fs::remove_dir_all(&self.dir)?)
    }

    fn get_first_capture(pattern: &Regex, entry: &DirEntry) -> Option<usize> {
        let file_name = entry.file_name();
        file_name.to_str().and_then(|file_str| {
            pattern.captures(&file_str).and_then(|captures| {
                captures.get(1).and_then(|number_capture| {
                    number_capture.as_str().parse::<usize>().ok()
                })
            })
        })
    }

    async fn read_memtable_from_wal_file(
        wal_path: &PathBuf,
        block_size: u64,
    ) -> Result<RedBlackTree<Vec<u8>, Vec<u8>>> {
        let mut memtable = RedBlackTree::with_capacity(TREE_CAPACITY);
        let wal_file = BufferedFile::open(&wal_path).await?;
        let mut reader = StreamReaderBuilder::new(wal_file).build();

        let mut wal_buf = Vec::new();
        reader.read_to_end(&mut wal_buf).await?;
        let mut cursor = std::io::Cursor::new(&wal_buf[..]);
        while cursor.position() < wal_buf.len() as u64 {
            if let Ok(entry) =
                bincode_options().deserialize_from::<_, Entry>(&mut cursor)
            {
                memtable.set(entry.key, entry.value)?;
            }
            let pos = cursor.position();
            cursor.set_position(pos + block_size - pos % block_size);
        }
        reader.close().await?;
        Ok(memtable)
    }

    fn run_compaction_action(action: &CompactionAction) -> Result<()> {
        for path_to_delete in &action.deletes {
            if path_to_delete.exists() {
                Self::remove_file_log_on_err(path_to_delete);
            }
        }

        for (source_path, destination_path) in &action.renames {
            if source_path.exists() {
                std::fs::rename(source_path, destination_path)?;
            }
        }

        Ok(())
    }

    fn get_data_file_paths(dir: &PathBuf, index: usize) -> (PathBuf, PathBuf) {
        let data_path = get_file_path(dir, index, DATA_FILE_EXT);
        let index_path = get_file_path(dir, index, INDEX_FILE_EXT);
        (data_path, index_path)
    }

    fn get_compaction_file_paths(
        dir: &PathBuf,
        index: usize,
    ) -> (PathBuf, PathBuf) {
        let data_path = get_file_path(dir, index, COMPACT_DATA_FILE_EXT);
        let index_path = get_file_path(dir, index, COMPACT_INDEX_FILE_EXT);
        (data_path, index_path)
    }

    pub fn sstable_indices(&self) -> Vec<usize> {
        self.sstables.borrow().iter().map(|t| t.index).collect()
    }

    pub fn memtable_full(&self) -> bool {
        self.active_memtable.borrow().capacity()
            == self.active_memtable.borrow().len()
    }

    async fn binary_search(
        key: &Vec<u8>,
        data_file: &BufferedFile,
        index_file: &BufferedFile,
        index_offset_start: u64,
        index_offset_length: u64,
    ) -> Result<Option<Entry>> {
        let mut half = index_offset_length / 2;
        let mut hind = index_offset_length - 1;
        let mut lind = 0;

        let mut current: EntryOffset = bincode_options().deserialize(
            &index_file
                .read_at(
                    index_offset_start + half * *INDEX_ENTRY_SIZE,
                    *INDEX_ENTRY_SIZE as usize,
                )
                .await?,
        )?;

        while lind <= hind {
            let value: Entry = bincode_options().deserialize(
                &data_file
                    .read_at(current.entry_offset as u64, current.entry_size)
                    .await?,
            )?;

            match value.key.cmp(&key) {
                std::cmp::Ordering::Equal => {
                    return Ok(Some(value));
                }
                std::cmp::Ordering::Less => lind = half + 1,
                std::cmp::Ordering::Greater => hind = half - 1,
            }

            if half == 0 || half == index_offset_length {
                break;
            }

            half = (hind + lind) / 2;
            current = bincode_options().deserialize(
                &index_file
                    .read_at(
                        index_offset_start + half * *INDEX_ENTRY_SIZE,
                        *INDEX_ENTRY_SIZE as usize,
                    )
                    .await?,
            )?;
        }

        Ok(None)
    }

    pub async fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>> {
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
            let (data_filename, index_filename) =
                Self::get_data_file_paths(&self.dir, sstable.index);

            let data_file = BufferedFile::open(&data_filename).await?;
            let index_file = BufferedFile::open(&index_filename).await?;

            if let Some(result) = Self::binary_search(
                key,
                &data_file,
                &index_file,
                0,
                sstable.size,
            )
            .await?
            {
                return Ok(Some(result.value));
            }
        }

        Ok(None)
    }

    pub async fn set(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<Option<Vec<u8>>> {
        // Write to memtable in memory.
        let result = self
            .active_memtable
            .borrow_mut()
            .set(key.clone(), value.clone())?;

        // Write to WAL for persistance.
        self.write_to_wal(&Entry { key, value }).await?;

        Ok(result)
    }

    pub async fn delete(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        self.set(key, TOMBSTONE).await
    }

    async fn write_to_wal(&self, entry: &Entry) -> Result<()> {
        let buf = bincode_options().serialize(entry)?;
        let size = buf.len() as u64;

        let size_padded = size + self.block_size - size % self.block_size;
        let offset = self.wal_offset.get();
        self.wal_offset.set(offset + size_padded);

        let file = self.wal_file.borrow().clone();
        file.write_at(buf, offset).await?;
        // TODO: fdatasync to ensure durability

        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        // Wait until the previous flush is finished.
        while self.flush_memtable.borrow().is_some() {
            futures_lite::future::yield_now().await;
        }

        if self.active_memtable.borrow().len() == 0 {
            return Ok(());
        }

        let memtable_to_flush = RedBlackTree::with_capacity(
            self.active_memtable.borrow().capacity(),
        );
        self.flush_memtable
            .replace(Some(self.active_memtable.replace(memtable_to_flush)));

        let flush_wal_path = get_file_path(
            &self.dir,
            self.memtable_index.get(),
            MEMTABLE_FILE_EXT,
        );

        self.memtable_index.set(self.memtable_index.get() + 2);

        let next_wal_path = get_file_path(
            &self.dir,
            self.memtable_index.get(),
            MEMTABLE_FILE_EXT,
        );
        self.wal_file
            .replace(Rc::new(BufferedFile::create(&next_wal_path).await?));
        self.wal_offset.set(0);

        let (data_filename, index_filename) = Self::get_data_file_paths(
            &self.dir,
            self.write_sstable_index.get(),
        );
        let data_file = DmaFile::create(&data_filename).await?;
        let index_file = DmaFile::create(&index_filename).await?;

        let items_written = Self::flush_memtable_to_disk(
            self.flush_memtable.borrow().as_ref().unwrap(),
            data_file,
            index_file,
        )
        .await?;

        self.flush_memtable.replace(None);

        // Replace sstables with new list containing the flushed sstable.
        {
            let mut sstables: Vec<SSTable> =
                self.sstables.borrow().iter().map(|t| t.clone()).collect();
            sstables.push(SSTable {
                index: self.write_sstable_index.get(),
                size: items_written as u64,
            });
            self.sstables.replace(Rc::new(sstables));
        }
        self.write_sstable_index
            .set(self.write_sstable_index.get() + 2);

        std::fs::remove_file(&flush_wal_path)?;

        Ok(())
    }

    async fn write_entry(
        entry: &Entry,
        offset: u64,
        data_writer: &mut (impl AsyncWriteExt + std::marker::Unpin),
        index_writer: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<usize> {
        let data_encoded = bincode_options().serialize(&entry)?;
        let entry_size = data_encoded.len();
        let entry_index = EntryOffset {
            entry_offset: offset,
            entry_size,
        };
        let index_encoded = bincode_options().serialize(&entry_index)?;

        data_writer.write_all(&data_encoded).await?;
        index_writer.write_all(&index_encoded).await?;
        Ok(entry_size)
    }

    async fn flush_memtable_to_disk(
        memtable: &RedBlackTree<Vec<u8>, Vec<u8>>,
        data_file: DmaFile,
        index_file: DmaFile,
    ) -> Result<usize> {
        let mut data_write_stream = DmaStreamWriterBuilder::new(data_file)
            .with_write_behind(10)
            .with_buffer_size(512)
            .build();
        let mut index_write_stream = DmaStreamWriterBuilder::new(index_file)
            .with_write_behind(10)
            .with_buffer_size(512)
            .build();

        let mut written = 0;
        for (key, value) in memtable.iter() {
            let entry_offset = data_write_stream.current_pos();
            let entry = Entry {
                key: key.clone(),
                value: value.clone(),
            };
            Self::write_entry(
                &entry,
                entry_offset,
                &mut data_write_stream,
                &mut index_write_stream,
            )
            .await?;
            written += 1;
        }
        data_write_stream.close().await?;
        index_write_stream.close().await?;

        Ok(written)
    }

    // Compact all sstables in the given list of sstable files, write the result
    // to the output file given.
    pub async fn compact(
        &self,
        indices_to_compact: Vec<usize>,
        output_index: usize,
        remove_tombstones: bool,
    ) -> Result<()> {
        let sstable_paths: Vec<(PathBuf, PathBuf)> = indices_to_compact
            .iter()
            .map(|i| Self::get_data_file_paths(&self.dir, *i))
            .collect();

        // No stable AsyncIterator yet...
        // If there was, itertools::kmerge would probably solve it all.
        let mut sstable_readers = Vec::with_capacity(sstable_paths.len());
        for (data_path, index_path) in &sstable_paths {
            let data_file = BufferedFile::open(data_path).await?;
            let index_file = BufferedFile::open(index_path).await?;
            let data_reader = StreamReaderBuilder::new(data_file).build();
            let index_reader = StreamReaderBuilder::new(index_file).build();
            sstable_readers.push((data_reader, index_reader));
        }

        let (compact_data_path, compact_index_path) =
            Self::get_compaction_file_paths(&self.dir, output_index);
        let compact_data_file =
            BufferedFile::create(&compact_data_path).await?;
        let compact_index_file =
            BufferedFile::create(&compact_index_path).await?;
        let mut compact_data_writer =
            StreamWriterBuilder::new(compact_data_file).build();
        let mut compact_index_writer =
            StreamWriterBuilder::new(compact_index_file).build();

        let mut offset_bytes = vec![0; *INDEX_ENTRY_SIZE as usize];
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

        let mut entry_offset = 0u64;
        let mut items_written = 0;

        while let Some(current) = heap.pop() {
            let index = current.index;

            let mut should_write_current = true;
            if let Some(next) = heap.peek() {
                should_write_current &= next.entry.key != current.entry.key;
            }
            should_write_current &=
                !remove_tombstones || current.entry.value != TOMBSTONE;

            if should_write_current {
                let written = Self::write_entry(
                    &current.entry,
                    entry_offset,
                    &mut compact_data_writer,
                    &mut compact_index_writer,
                )
                .await?;
                entry_offset += written as u64;
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

        compact_data_writer.close().await?;
        compact_index_writer.close().await?;

        let mut files_to_delete = Vec::with_capacity(sstable_paths.len() * 2);
        for (data_path, index_path) in sstable_paths {
            files_to_delete.push(data_path);
            files_to_delete.push(index_path);
        }

        let (output_data_path, output_index_path) =
            Self::get_data_file_paths(&self.dir, output_index);

        let action = CompactionAction {
            renames: vec![
                (compact_data_path, output_data_path),
                (compact_index_path, output_index_path),
            ],
            deletes: files_to_delete,
        };
        let action_encoded = bincode_options().serialize(&action)?;

        let compact_action_path =
            get_file_path(&self.dir, output_index, COMPACT_ACTION_FILE_EXT);
        let compact_action_file =
            BufferedFile::create(&compact_action_path).await?;
        let mut compact_action_writer =
            StreamWriterBuilder::new(compact_action_file).build();
        compact_action_writer.write_all(&action_encoded).await?;
        compact_action_writer.close().await?;

        let old_sstables = self.sstables.borrow().clone();

        {
            let mut sstables: Vec<SSTable> =
                old_sstables.iter().map(|t| t.clone()).collect();
            sstables.retain(|x| !indices_to_compact.contains(&x.index));
            sstables.push(SSTable {
                index: output_index,
                size: items_written,
            });
            sstables.sort_unstable_by_key(|t| t.index);
            self.sstables.replace(Rc::new(sstables));
        }

        for (source_path, destination_path) in &action.renames {
            std::fs::rename(source_path, destination_path)?;
        }

        // Block the current execution task until all currently running read
        // tasks finish, to make sure we don't delete files that are being read.
        while Rc::strong_count(&old_sstables) > 1 {
            futures_lite::future::yield_now().await;
        }

        for path_to_delete in &action.deletes {
            if path_to_delete.exists() {
                Self::remove_file_log_on_err(path_to_delete);
            }
        }

        Self::remove_file_log_on_err(&compact_action_path);

        Ok(())
    }

    async fn read_next_entry(
        data_reader: &mut StreamReader,
        index_reader: &mut StreamReader,
        offset_bytes: &mut Vec<u8>,
    ) -> Result<Entry> {
        index_reader.read_exact(offset_bytes).await?;
        let entry_offset: EntryOffset =
            bincode_options().deserialize(&offset_bytes)?;
        let mut data_bytes = vec![0; entry_offset.entry_size];
        data_reader.read_exact(&mut data_bytes).await?;
        let entry: Entry = bincode_options().deserialize(&data_bytes)?;
        Ok(entry)
    }

    fn remove_file_log_on_err(file_path: &PathBuf) {
        if let Err(e) = std::fs::remove_file(file_path) {
            eprintln!(
                "Failed to remove file '{}', that is irrelevant after \
                 compaction: {}",
                file_path.display(),
                e
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_lite::Future;
    use glommio::{LocalExecutorBuilder, Placement};
    use tempfile::tempdir;

    use super::*;

    fn run_with_glommio<G, F, T>(fut_gen: G) -> Result<()>
    where
        G: FnOnce(PathBuf) -> F + Send + 'static,
        F: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let builder = LocalExecutorBuilder::new(Placement::Fixed(1))
            .spin_before_park(Duration::from_millis(10));
        let handle = builder.name("test").spawn(|| async move {
            let dir = tempdir().unwrap().into_path();
            let result = fut_gen(dir.clone()).await;
            std::fs::remove_dir_all(dir).unwrap();
            result
        })?;
        handle.join()?;
        Ok(())
    }

    async fn _set_and_get_memtable(dir: PathBuf) -> Result<()> {
        // New tree.
        {
            let tree = LSMTree::open_or_create(dir.clone()).await?;
            tree.set(vec![100], vec![200]).await?;
            assert_eq!(tree.get(&vec![100]).await?, Some(vec![200]));
            assert_eq!(tree.get(&vec![0]).await?, None);
        }

        // Reopening the tree.
        {
            let tree = LSMTree::open_or_create(dir).await?;
            assert_eq!(tree.get(&vec![100]).await?, Some(vec![200]));
            assert_eq!(tree.get(&vec![0]).await?, None);
        }

        Ok(())
    }

    #[test]
    fn set_and_get_memtable() -> Result<()> {
        run_with_glommio(_set_and_get_memtable)
    }

    async fn _set_and_get_sstable(dir: PathBuf) -> Result<()> {
        // New tree.
        {
            let tree = LSMTree::open_or_create(dir.clone()).await?;
            assert_eq!(tree.write_sstable_index.get(), 0);

            let values: Vec<Vec<u8>> = (0..TREE_CAPACITY as u16)
                .map(|n| n.to_le_bytes().to_vec())
                .collect();

            for v in values {
                tree.set(v.clone(), v).await?;
            }
            tree.flush().await?;

            assert_eq!(tree.active_memtable.borrow().len(), 0);
            assert_eq!(tree.write_sstable_index.get(), 2);
            assert_eq!(tree.get(&vec![0, 0]).await?, Some(vec![0, 0]));
            assert_eq!(tree.get(&vec![100, 1]).await?, Some(vec![100, 1]));
            assert_eq!(tree.get(&vec![200, 2]).await?, Some(vec![200, 2]));
        }

        // Reopening the tree.
        {
            let tree = LSMTree::open_or_create(dir).await?;
            assert_eq!(tree.active_memtable.borrow().len(), 0);
            assert_eq!(tree.write_sstable_index.get(), 2);
            assert_eq!(tree.get(&vec![0, 0]).await?, Some(vec![0, 0]));
            assert_eq!(tree.get(&vec![100, 1]).await?, Some(vec![100, 1]));
            assert_eq!(tree.get(&vec![200, 2]).await?, Some(vec![200, 2]));
        }

        Ok(())
    }

    #[test]
    fn set_and_get_sstable() -> Result<()> {
        run_with_glommio(_set_and_get_sstable)
    }

    async fn _get_after_compaction(dir: PathBuf) -> Result<()> {
        // New tree.
        {
            let tree = LSMTree::open_or_create(dir.clone()).await?;
            assert_eq!(tree.write_sstable_index.get(), 0);
            assert_eq!(*tree.sstable_indices(), vec![]);

            let values: Vec<Vec<u8>> = (0..((TREE_CAPACITY as u16) * 3) - 2)
                .map(|n| n.to_le_bytes().to_vec())
                .collect();

            for v in values {
                tree.set(v.clone(), v).await?;
                if tree.memtable_full() {
                    tree.flush().await?;
                }
            }
            tree.delete(vec![0, 1]).await?;
            tree.delete(vec![100, 2]).await?;
            tree.flush().await?;

            assert_eq!(*tree.sstable_indices(), vec![0, 2, 4]);

            tree.compact(vec![0, 2, 4], 5, true).await?;

            assert_eq!(*tree.sstable_indices(), vec![5]);
            assert_eq!(tree.write_sstable_index.get(), 6);
            assert_eq!(tree.get(&vec![0, 0]).await?, Some(vec![0, 0]));
            assert_eq!(tree.get(&vec![100, 1]).await?, Some(vec![100, 1]));
            assert_eq!(tree.get(&vec![200, 2]).await?, Some(vec![200, 2]));
            assert_eq!(tree.get(&vec![0, 1]).await?, None);
            assert_eq!(tree.get(&vec![100, 2]).await?, None);
        }

        // Reopening the tree.
        {
            let tree = LSMTree::open_or_create(dir).await?;
            assert_eq!(*tree.sstable_indices(), vec![5]);
            assert_eq!(tree.write_sstable_index.get(), 6);
            assert_eq!(tree.get(&vec![0, 0]).await?, Some(vec![0, 0]));
            assert_eq!(tree.get(&vec![100, 1]).await?, Some(vec![100, 1]));
            assert_eq!(tree.get(&vec![200, 2]).await?, Some(vec![200, 2]));
            assert_eq!(tree.get(&vec![0, 1]).await?, None);
            assert_eq!(tree.get(&vec![100, 2]).await?, None);
        }

        Ok(())
    }

    #[test]
    fn get_after_compaction() -> Result<()> {
        run_with_glommio(_get_after_compaction)
    }
}
