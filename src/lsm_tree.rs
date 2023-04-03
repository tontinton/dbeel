use std::{
    cmp::Ordering, collections::BinaryHeap, fs::DirEntry, marker::PhantomData,
    os::unix::prelude::MetadataExt, path::PathBuf, rc::Rc,
};

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
use redblacktree::RedBlackTree;
use regex::Regex;
use serde::{Deserialize, Serialize};

const TREE_CAPACITY: usize = 1024;
const INDEX_PADDING: usize = 20; // Number of integers in max u64.

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
            .then(self.index.cmp(&other.index))
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

fn bincode_options() -> WithOtherIntEncoding<
    WithOtherTrailing<DefaultOptions, RejectTrailing>,
    FixintEncoding,
> {
    return DefaultOptions::new()
        .reject_trailing_bytes()
        .with_fixint_encoding();
}

async fn binary_search(
    data_file: &BufferedFile,
    index_file: &BufferedFile,
    key: &Vec<u8>,
) -> glommio::Result<Option<Entry>, ()> {
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

        if half == 0 || half == length {
            break;
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

pub struct LSMTree {
    dir: PathBuf,
    // The memtable that is currently being written to.
    active_memtable: RedBlackTree<Vec<u8>, Vec<u8>>,
    // The memtable that is currently being flushed to disk.
    flush_memtable: Option<RedBlackTree<Vec<u8>, Vec<u8>>>,
    // The next sstable index that is going to be written.
    write_sstable_index: usize,
    // The sstable indices to query from.
    read_sstable_indices: Vec<usize>,
    // Track the number of sstable file reads are happening.
    // The reason for tracking is that when ending a compaction, there are
    // sstable files that should be removed / replaced, but there could be
    // reads to the same files concurrently, so the compaction process will
    // wait for the number of reads to reach 0.
    number_of_sstable_reads: Rc<PhantomData<usize>>,
    // The next memtable index.
    memtable_index: usize,
    // The memtable WAL for durability in case the process crashes without
    // flushing the memtable to disk.
    wal_file: BufferedFile,
    // The current end offset of the wal file.
    wal_offset: u64,
    // The filesystem block size of where our data is written to.
    block_size: u64,
}

impl LSMTree {
    pub async fn open_or_create(dir: PathBuf) -> std::io::Result<Self> {
        if !dir.is_dir() {
            std::fs::create_dir_all(&dir)?;
        }

        let pattern = Regex::new(r#"^(\d+)\.compact_action"#).unwrap();
        let compact_action_paths: Vec<PathBuf> = std::fs::read_dir(&dir)?
            .filter_map(Result::ok)
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

        let pattern = Regex::new(r#"^(\d+)\.data"#).unwrap();
        let data_file_indices = {
            let mut vec: Vec<usize> = std::fs::read_dir(&dir)?
                .filter_map(Result::ok)
                .filter_map(|entry| Self::get_first_capture(&pattern, &entry))
                .collect();
            vec.sort();
            vec
        };

        let write_file_index = data_file_indices
            .iter()
            .max()
            .map(|i| (*i + 2 - (*i & 1)))
            .unwrap_or(0);

        let pattern = Regex::new(r#"^(\d+)\.memtable"#).unwrap();
        let wal_indices: Vec<usize> = {
            let mut vec: Vec<usize> = std::fs::read_dir(&dir)?
                .filter_map(Result::ok)
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
                let mut unflashed_file_path = dir.clone();
                unflashed_file_path.push(format!(
                    "{:01$}.memtable",
                    unflashed_file_index, INDEX_PADDING
                ));
                let (data_file_path, index_file_path) =
                    Self::get_data_file_paths(
                        dir.clone(),
                        unflashed_file_index,
                    );
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

        let mut wal_path = dir.clone();
        wal_path
            .push(format!("{:01$}.memtable", wal_file_index, INDEX_PADDING));
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
            active_memtable,
            flush_memtable: None,
            write_sstable_index: write_file_index,
            read_sstable_indices: data_file_indices,
            number_of_sstable_reads: Rc::new(PhantomData::<usize>),
            memtable_index: wal_file_index,
            wal_file,
            wal_offset,
            block_size,
        })
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
    ) -> std::io::Result<RedBlackTree<Vec<u8>, Vec<u8>>> {
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
                memtable.set(entry.key, entry.value).unwrap();
            }
            let pos = cursor.position();
            cursor.set_position(pos + block_size - pos % block_size);
        }
        reader.close().await?;
        Ok(memtable)
    }

    fn run_compaction_action(action: &CompactionAction) -> std::io::Result<()> {
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

    fn get_data_file_paths(dir: PathBuf, index: usize) -> (PathBuf, PathBuf) {
        let mut data_filename = dir.clone();
        data_filename.push(format!("{:01$}.data", index, INDEX_PADDING));
        let mut index_filename = dir.clone();
        index_filename.push(format!("{:01$}.index", index, INDEX_PADDING));
        (data_filename, index_filename)
    }

    fn get_compaction_file_paths(
        dir: PathBuf,
        index: usize,
    ) -> (PathBuf, PathBuf) {
        let mut data_filename = dir.clone();
        data_filename
            .push(format!("{:01$}.compact_data", index, INDEX_PADDING));
        let mut index_filename = dir.clone();
        index_filename
            .push(format!("{:01$}.compact_index", index, INDEX_PADDING));
        (data_filename, index_filename)
    }

    pub fn sstable_indices(&self) -> &Vec<usize> {
        &self.read_sstable_indices
    }

    pub fn memtable_full(&self) -> bool {
        self.active_memtable.capacity() == self.active_memtable.len()
    }

    pub async fn get(
        &self,
        key: &Vec<u8>,
    ) -> glommio::Result<Option<Vec<u8>>, ()> {
        // Query the active tree first.
        let result = self.active_memtable.get(key);
        if result.is_some() {
            return Ok(result.map(|s| s.clone()));
        }

        // Key not found in active tree, query the flushed tree.
        if let Some(tree) = &self.flush_memtable {
            let result = tree.get(key);
            if result.is_some() {
                return Ok(result.map(|s| s.clone()));
            }
        }

        // Key not found in memory, query all files from the newest to the
        // oldest.
        let _counter = self.number_of_sstable_reads.clone();

        for i in self.read_sstable_indices.iter().rev() {
            let (data_filename, index_filename) =
                Self::get_data_file_paths(self.dir.clone(), *i);

            let data_file = BufferedFile::open(&data_filename).await?;
            let index_file = BufferedFile::open(&index_filename).await?;

            if let Some(result) =
                binary_search(&data_file, &index_file, key).await?
            {
                return Ok(Some(result.value));
            }
        }

        Ok(None)
    }

    pub async fn set(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> glommio::Result<Option<Vec<u8>>, ()> {
        // Write to memtable in memory.
        let result = self
            .active_memtable
            .set(key.clone(), value.clone())
            .unwrap();

        // Write to WAL for persistance.
        self.write_to_wal(&Entry { key, value }).await?;

        Ok(result)
    }

    async fn write_to_wal(&mut self, entry: &Entry) -> glommio::Result<(), ()> {
        let buf = bincode_options().serialize(entry).unwrap();
        let size = buf.len() as u64;

        let size_padded = size + self.block_size - size % self.block_size;
        let offset = self.wal_offset;
        self.wal_offset += size_padded;

        self.wal_file.write_at(buf, offset).await?;
        // TODO: fdatasync to ensure durability

        Ok(())
    }

    pub async fn flush(&mut self) -> glommio::Result<(), ()> {
        // Wait until the previous flush is finished.
        while self.flush_memtable.is_some() {
            futures_lite::future::yield_now().await;
        }

        if self.active_memtable.len() == 0 {
            return Ok(());
        }

        let mut memtable_to_flush =
            RedBlackTree::with_capacity(self.active_memtable.capacity());
        std::mem::swap(&mut memtable_to_flush, &mut self.active_memtable);
        self.flush_memtable = Some(memtable_to_flush);

        let mut flush_wal_path = self.dir.clone();
        flush_wal_path.push(format!(
            "{:01$}.memtable",
            self.memtable_index, INDEX_PADDING
        ));

        self.memtable_index += 2;

        let mut next_wal_path = self.dir.clone();
        next_wal_path.push(format!(
            "{:01$}.memtable",
            self.memtable_index, INDEX_PADDING
        ));
        self.wal_file = BufferedFile::create(&next_wal_path).await?;
        self.wal_offset = 0;

        let (data_filename, index_filename) = Self::get_data_file_paths(
            self.dir.clone(),
            self.write_sstable_index,
        );
        let data_file = DmaFile::create(&data_filename).await?;
        let index_file = DmaFile::create(&index_filename).await?;

        Self::flush_memtable_to_disk(
            self.flush_memtable.as_ref().unwrap(),
            data_file,
            index_file,
        )
        .await?;

        self.flush_memtable = None;
        self.read_sstable_indices.push(self.write_sstable_index);
        self.write_sstable_index += 2;

        std::fs::remove_file(&flush_wal_path)?;

        Ok(())
    }

    async fn flush_memtable_to_disk(
        memtable: &RedBlackTree<Vec<u8>, Vec<u8>>,
        data_file: DmaFile,
        index_file: DmaFile,
    ) -> glommio::Result<(), ()> {
        let mut data_write_stream = DmaStreamWriterBuilder::new(data_file)
            .with_write_behind(10)
            .with_buffer_size(512)
            .build();
        let mut index_write_stream = DmaStreamWriterBuilder::new(index_file)
            .with_write_behind(10)
            .with_buffer_size(512)
            .build();

        for (key, value) in memtable.iter() {
            let entry_offset = data_write_stream.current_pos();
            let entry = Entry {
                key: key.clone(),
                value: value.clone(),
            };
            let entry_encoded = bincode_options().serialize(&entry).unwrap();
            let entry_size = entry_encoded.len();
            data_write_stream.write_all(&entry_encoded).await?;

            let entry_index = EntryOffset {
                entry_offset,
                entry_size,
            };
            let index_encoded =
                bincode_options().serialize(&entry_index).unwrap();
            index_write_stream.write_all(&index_encoded).await?;
        }
        data_write_stream.close().await?;
        index_write_stream.close().await?;

        Ok(())
    }

    // Compact all sstables in the given list of sstable files, write the result
    // to the output file given.
    pub async fn compact(
        &mut self,
        indices_to_compact: Vec<usize>,
        output_index: usize,
    ) -> std::io::Result<()> {
        let sstable_paths: Vec<(PathBuf, PathBuf)> = indices_to_compact
            .iter()
            .map(|i| Self::get_data_file_paths(self.dir.clone(), *i))
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
            Self::get_compaction_file_paths(self.dir.clone(), output_index);
        let compact_data_file =
            BufferedFile::create(&compact_data_path).await?;
        let compact_index_file =
            BufferedFile::create(&compact_index_path).await?;
        let mut compact_data_writer =
            StreamWriterBuilder::new(compact_data_file).build();
        let mut compact_index_writer =
            StreamWriterBuilder::new(compact_index_file).build();

        let item_size = bincode_options()
            .serialized_size(&EntryOffset::default())
            .unwrap();

        let mut offset_bytes = vec![0; item_size as usize];
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

        while let Some(next) = heap.pop() {
            let index = next.index;

            let next_data_encoded =
                bincode_options().serialize(&next.entry).unwrap();
            let entry_size = next_data_encoded.len();
            let entry_index = EntryOffset {
                entry_offset,
                entry_size,
            };
            entry_offset += entry_size as u64;
            let next_index_encoded =
                bincode_options().serialize(&entry_index).unwrap();

            compact_data_writer.write(&next_data_encoded).await?;
            compact_index_writer.write(&next_index_encoded).await?;

            let (data_reader, index_reader): &mut (StreamReader, StreamReader) =
                sstable_readers.get_mut(index).unwrap();

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
            Self::get_data_file_paths(self.dir.clone(), output_index);

        let action = CompactionAction {
            renames: vec![
                (compact_data_path, output_data_path),
                (compact_index_path, output_index_path),
            ],
            deletes: files_to_delete,
        };
        let action_encoded = bincode_options().serialize(&action).unwrap();

        let mut compact_action_path = self.dir.clone();
        compact_action_path.push(format!(
            "{:01$}.compact_action",
            output_index, INDEX_PADDING
        ));
        let compact_action_file =
            BufferedFile::create(&compact_action_path).await?;
        let mut compact_action_writer =
            StreamWriterBuilder::new(compact_action_file).build();
        compact_action_writer.write_all(&action_encoded).await?;
        compact_action_writer.close().await?;

        let counter = self.number_of_sstable_reads.clone();
        self.number_of_sstable_reads = Rc::new(PhantomData::<usize>);

        self.read_sstable_indices
            .retain(|x| !indices_to_compact.contains(x));
        self.read_sstable_indices.push(output_index);
        self.read_sstable_indices.sort();

        for (source_path, destination_path) in &action.renames {
            std::fs::rename(source_path, destination_path)?;
        }

        // Block the current execution task until all currently running read
        // tasks finish, to make sure we don't delete files that are being read.
        while Rc::strong_count(&counter) > 1 {
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
    ) -> std::io::Result<Entry> {
        index_reader.read_exact(offset_bytes).await?;
        let entry_offset: EntryOffset =
            bincode_options().deserialize(&offset_bytes).unwrap();
        let mut data_bytes = vec![0; entry_offset.entry_size];
        data_reader.read_exact(&mut data_bytes).await?;
        let entry: Entry = bincode_options().deserialize(&data_bytes).unwrap();
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

    fn run_with_glommio<G, F, T>(fut_gen: G) -> std::io::Result<()>
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

    async fn _set_and_get_memtable(dir: PathBuf) -> std::io::Result<()> {
        // New tree.
        {
            let mut tree = LSMTree::open_or_create(dir.clone()).await?;
            tree.set(vec![100], vec![200]).await?;
            assert_eq!(tree.get(&vec![100]).await?, Some(vec![200]));
        }

        // Reopening the tree.
        {
            let tree = LSMTree::open_or_create(dir).await?;
            assert_eq!(tree.get(&vec![100]).await?, Some(vec![200]));
        }

        Ok(())
    }

    #[test]
    fn set_and_get_memtable() -> std::io::Result<()> {
        run_with_glommio(_set_and_get_memtable)
    }

    async fn _set_and_get_sstable(dir: PathBuf) -> std::io::Result<()> {
        // New tree.
        {
            let mut tree = LSMTree::open_or_create(dir.clone()).await?;
            assert_eq!(tree.write_sstable_index, 0);

            let values: Vec<Vec<u8>> = (0..TREE_CAPACITY as u16)
                .map(|n| n.to_le_bytes().to_vec())
                .collect();

            for v in values {
                tree.set(v.clone(), v).await?;
            }
            tree.flush().await?;

            assert_eq!(tree.active_memtable.len(), 0);
            assert_eq!(tree.write_sstable_index, 2);
            assert_eq!(tree.get(&vec![0, 0]).await?, Some(vec![0, 0]));
            assert_eq!(tree.get(&vec![100, 1]).await?, Some(vec![100, 1]));
            assert_eq!(tree.get(&vec![200, 2]).await?, Some(vec![200, 2]));
        }

        // Reopening the tree.
        {
            let tree = LSMTree::open_or_create(dir).await?;
            assert_eq!(tree.active_memtable.len(), 0);
            assert_eq!(tree.write_sstable_index, 2);
            assert_eq!(tree.get(&vec![0, 0]).await?, Some(vec![0, 0]));
            assert_eq!(tree.get(&vec![100, 1]).await?, Some(vec![100, 1]));
            assert_eq!(tree.get(&vec![200, 2]).await?, Some(vec![200, 2]));
        }

        Ok(())
    }

    #[test]
    fn set_and_get_sstable() -> std::io::Result<()> {
        run_with_glommio(_set_and_get_sstable)
    }

    async fn _get_after_compaction(dir: PathBuf) -> std::io::Result<()> {
        // New tree.
        {
            let mut tree = LSMTree::open_or_create(dir.clone()).await?;
            assert_eq!(tree.write_sstable_index, 0);
            assert_eq!(tree.read_sstable_indices, vec![]);

            let values: Vec<Vec<u8>> = (0..(TREE_CAPACITY as u16) * 3)
                .map(|n| n.to_le_bytes().to_vec())
                .collect();

            for v in values {
                tree.set(v.clone(), v).await?;
                if tree.memtable_full() {
                    tree.flush().await?;
                }
            }

            assert_eq!(tree.read_sstable_indices, vec![0, 2, 4]);

            tree.compact(vec![0, 2, 4], 5).await?;

            assert_eq!(tree.read_sstable_indices, vec![5]);
            assert_eq!(tree.write_sstable_index, 6);
            assert_eq!(tree.get(&vec![0, 0]).await?, Some(vec![0, 0]));
            assert_eq!(tree.get(&vec![100, 1]).await?, Some(vec![100, 1]));
            assert_eq!(tree.get(&vec![200, 2]).await?, Some(vec![200, 2]));
        }

        // Reopening the tree.
        {
            let tree = LSMTree::open_or_create(dir).await?;
            assert_eq!(tree.read_sstable_indices, vec![5]);
            assert_eq!(tree.write_sstable_index, 6);
            assert_eq!(tree.get(&vec![0, 0]).await?, Some(vec![0, 0]));
            assert_eq!(tree.get(&vec![100, 1]).await?, Some(vec![100, 1]));
            assert_eq!(tree.get(&vec![200, 2]).await?, Some(vec![200, 2]));
        }

        Ok(())
    }

    #[test]
    fn get_after_compaction() -> std::io::Result<()> {
        run_with_glommio(_get_after_compaction)
    }
}
