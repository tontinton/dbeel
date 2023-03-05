use std::{fs::DirEntry, path::PathBuf};

use bincode::{
    config::{
        FixintEncoding, RejectTrailing, WithOtherIntEncoding, WithOtherTrailing,
    },
    DefaultOptions, Options,
};
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::io::{
    BufferedFile, DmaFile, DmaStreamWriterBuilder, OpenOptions,
    StreamReaderBuilder, StreamWriter, StreamWriterBuilder,
};
use redblacktree::RedBlackTree;
use regex::Regex;
use serde::{Deserialize, Serialize};

const TREE_CAPACITY: usize = 8096;

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
    active_memtable: RedBlackTree<String, String>,
    // The memtable that is currently being flushed to disk.
    flush_memtable: Option<RedBlackTree<String, String>>,
    // The next sstable index that is going to be written.
    write_sstable_index: usize,
    // The sstable indices to query from.
    read_sstable_indices: Vec<usize>,
    // The next memtable index.
    memtable_index: usize,
    // The memtable WAL for durability in case the process crashes without
    // flushing the memtable to disk.
    wal_writer: StreamWriter,
}

impl LSMTree {
    pub async fn new(dir: PathBuf) -> std::io::Result<Self> {
        let pattern = Regex::new(r#"^(\d+)\.data"#).unwrap();
        let data_file_indices = if dir.is_dir() {
            let mut vec: Vec<usize> = std::fs::read_dir(&dir)?
                .filter_map(Result::ok)
                .filter_map(|entry| Self::get_first_capture(&pattern, &entry))
                .collect();
            vec.sort();
            vec
        } else {
            std::fs::create_dir_all(&dir)?;
            Vec::new()
        };

        let write_file_index =
            data_file_indices.iter().max().map(|i| *i + 1).unwrap_or(0);

        let pattern = Regex::new(r#"^(\d+)\.memtable"#).unwrap();
        let wal_indexes: Vec<usize> = std::fs::read_dir(&dir)?
            .filter_map(Result::ok)
            .filter_map(|entry| Self::get_first_capture(&pattern, &entry))
            .collect();

        let wal_file_index = match wal_indexes.len() {
            0 => 0,
            1 => wal_indexes[0],
            2 => {
                // A flush did not finish for some reason, do it now.
                let wal_file_index = wal_indexes[1];
                let unflashed_file_index = wal_indexes[0];
                let mut unflashed_file_path = dir.clone();
                unflashed_file_path
                    .push(format!("{:01$}.memtable", unflashed_file_index, 9));
                let (data_file_path, index_file_path) =
                    Self::get_data_file_paths(
                        dir.clone(),
                        unflashed_file_index,
                    );
                let memtable =
                    Self::read_memtable_from_wal_file(&unflashed_file_path)
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
        wal_path.push(format!("{:01$}.memtable", wal_file_index, 9));

        let (wal_writer, active_memtable) = if wal_path.exists() {
            let memtable = Self::read_memtable_from_wal_file(&wal_path).await?;
            let file = OpenOptions::new()
                .append(true)
                .buffered_open(&wal_path)
                .await?;
            let wal_writer = StreamWriterBuilder::new(file).build();
            (wal_writer, memtable)
        } else {
            let memtable = RedBlackTree::with_capacity(TREE_CAPACITY);
            let wal_writer = StreamWriterBuilder::new(
                BufferedFile::create(&wal_path).await?,
            )
            .build();
            (wal_writer, memtable)
        };

        Ok(Self {
            dir,
            active_memtable,
            flush_memtable: None,
            write_sstable_index: write_file_index,
            read_sstable_indices: data_file_indices,
            memtable_index: wal_file_index,
            wal_writer,
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
    ) -> std::io::Result<RedBlackTree<String, String>> {
        let mut memtable = RedBlackTree::with_capacity(TREE_CAPACITY);
        let wal_file = BufferedFile::open(&wal_path).await?;
        let mut reader = StreamReaderBuilder::new(wal_file).build();

        let mut wal_buf = Vec::new();
        reader.read_to_end(&mut wal_buf).await?;
        let mut cursor = std::io::Cursor::new(&wal_buf[..]);
        while let Ok(entry) =
            bincode_options().deserialize_from::<_, Entry>(&mut cursor)
        {
            memtable.set(entry.key, entry.value).unwrap();
        }
        reader.close().await?;
        Ok(memtable)
    }

    fn get_data_file_paths(dir: PathBuf, index: usize) -> (PathBuf, PathBuf) {
        let mut data_filename = dir.clone();
        data_filename.push(format!("{:01$}.data", index, 9));
        let mut index_filename = dir.clone();
        index_filename.push(format!("{:01$}.index", index, 9));
        (data_filename, index_filename)
    }

    pub async fn get(
        &self,
        key: &String,
    ) -> glommio::Result<Option<String>, ()> {
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
        for i in self.read_sstable_indices.iter().rev() {
            let (data_filename, index_filename) =
                Self::get_data_file_paths(self.dir.clone(), *i);

            let data_file = DmaFile::open(&data_filename).await?;
            let index_file = DmaFile::open(&index_filename).await?;

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
        key: String,
        value: String,
    ) -> glommio::Result<Option<String>, ()> {
        // Write to memtable in memory.
        let result = self
            .active_memtable
            .set(key.clone(), value.clone())
            .unwrap();

        // Write to WAL for persistance.
        let entry = Entry { key, value };
        let entry_encoded = bincode_options().serialize(&entry).unwrap();
        self.wal_writer.write_all(&entry_encoded).await?;
        self.wal_writer.flush().await?;

        if self.active_memtable.capacity() == self.active_memtable.len() {
            // Capacity is full, flush the active tree to disk.
            self.flush().await?;
        }

        Ok(result)
    }

    async fn flush(&mut self) -> glommio::Result<(), ()> {
        if self.active_memtable.len() == 0 {
            return Ok(());
        }

        // TODO: do not panic here.
        if self.flush_memtable.is_some() {
            panic!("Flush memtable has not yet been flushed");
        }

        let mut flush_wal_path = self.dir.clone();
        flush_wal_path.push(format!("{:01$}.memtable", self.memtable_index, 9));

        self.memtable_index += 1;

        let mut next_wal_path = self.dir.clone();
        next_wal_path.push(format!("{:01$}.memtable", self.memtable_index, 9));

        self.wal_writer = StreamWriterBuilder::new(
            BufferedFile::create(&next_wal_path).await?,
        )
        .build();

        let (data_filename, index_filename) = Self::get_data_file_paths(
            self.dir.clone(),
            self.write_sstable_index,
        );
        let data_file = DmaFile::create(&data_filename).await?;
        let index_file = DmaFile::create(&index_filename).await?;

        let mut memtable_to_flush =
            RedBlackTree::with_capacity(self.active_memtable.capacity());
        std::mem::swap(&mut memtable_to_flush, &mut self.active_memtable);
        self.flush_memtable = Some(memtable_to_flush);

        Self::flush_memtable_to_disk(
            self.flush_memtable.as_ref().unwrap(),
            data_file,
            index_file,
        )
        .await?;

        self.flush_memtable = None;
        self.read_sstable_indices.push(self.write_sstable_index);
        self.write_sstable_index += 1;

        std::fs::remove_file(&flush_wal_path)?;

        Ok(())
    }

    async fn flush_memtable_to_disk(
        memtable: &RedBlackTree<String, String>,
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
                key: key.to_string(),
                value: value.to_string(),
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
}
