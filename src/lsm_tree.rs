use std::path::PathBuf;

use bincode::{
    config::{
        FixintEncoding, RejectTrailing, WithOtherIntEncoding, WithOtherTrailing,
    },
    DefaultOptions, Options,
};
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::io::{
    BufferedFile, DmaFile, DmaStreamWriter, DmaStreamWriterBuilder,
    StreamReaderBuilder,
};
use redblacktree::RedBlackTree;
use regex::Regex;
use serde::{Deserialize, Serialize};

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
    // The next sstable index.
    index: usize,
    // The memtable WAL for durability in case the process crashes without
    // flushing the memtable to disk.
    wal_writer: DmaStreamWriter,
}

impl LSMTree {
    pub async fn new(dir: PathBuf) -> std::io::Result<Self> {
        let pattern = Regex::new(r#"^(\d+)\.data"#).unwrap();
        let index = if dir.is_dir() {
            // TODO: check for missing files.
            std::fs::read_dir(dir.clone())?
                .filter_map(Result::ok)
                .filter_map(|entry| {
                    let file_name = entry.file_name();
                    file_name.to_str().map(str::to_owned).and_then(|file_str| {
                        pattern.captures(&file_str).and_then(|captures| {
                            captures.get(1).and_then(|number_capture| {
                                number_capture
                                    .as_str()
                                    .parse::<usize>()
                                    .ok()
                                    .map(|n| n + 1)
                            })
                        })
                    })
                })
                .max()
                .unwrap_or(0)
        } else {
            std::fs::create_dir_all(dir.clone()).unwrap();
            0
        };

        let mut active_memtable = RedBlackTree::with_capacity(1024);

        let mut wal_path = dir.clone();
        wal_path.push("memtable.log");
        if wal_path.exists() {
            let wal_file = BufferedFile::open(wal_path.clone()).await?;
            let mut reader = StreamReaderBuilder::new(wal_file).build();

            let mut wal_buf = Vec::new();
            reader.read_to_end(&mut wal_buf).await?;
            let mut cursor = std::io::Cursor::new(&wal_buf[..]);
            while let Ok(entry) =
                bincode_options().deserialize_from::<_, Entry>(&mut cursor)
            {
                // TODO: not unwrap.
                active_memtable.set(entry.key, entry.value).unwrap();
            }
            reader.close().await?;
        }

        let wal_writer =
            DmaStreamWriterBuilder::new(DmaFile::create(wal_path).await?)
                .with_buffer_size(512)
                .with_write_behind(10)
                .build();

        Ok(Self {
            dir,
            active_memtable,
            flush_memtable: None,
            index,
            wal_writer,
        })
    }

    fn get_data_file_names(&self, index: usize) -> (PathBuf, PathBuf) {
        let mut data_filename = self.dir.clone();
        data_filename.push(format!("{:01$}.data", index, 9));
        let mut index_filename = self.dir.clone();
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
        for i in (0..self.index).rev() {
            let (data_filename, index_filename) = self.get_data_file_names(i);

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
    ) -> glommio::Result<(), ()> {
        // Write to memtable in memory.
        self.active_memtable
            .set(key.clone(), value.clone())
            .unwrap();

        // Write to WAL for persistance.
        let entry = Entry { key, value };
        let entry_encoded = bincode_options().serialize(&entry).unwrap();
        self.wal_writer.write_all(&entry_encoded).await?;
        self.wal_writer.sync().await?;

        if self.active_memtable.capacity() == self.active_memtable.len() {
            // Capacity is full, flush the active tree to disk.
            self.flush().await?;
        }

        Ok(())
    }

    async fn flush(&mut self) -> glommio::Result<(), ()> {
        if self.active_memtable.len() == 0 {
            return Ok(());
        }

        // TODO: do not panic here.
        if self.flush_memtable.is_some() {
            panic!("Flush memtable has not yet been flushed");
        }

        let (data_filename, index_filename) =
            self.get_data_file_names(self.index);
        let data_file = DmaFile::create(&data_filename).await?;
        let index_file = DmaFile::create(&index_filename).await?;

        let mut memtable_to_flush =
            RedBlackTree::with_capacity(self.active_memtable.capacity());
        std::mem::swap(&mut memtable_to_flush, &mut self.active_memtable);
        self.flush_memtable = Some(memtable_to_flush);

        let mut data_write_stream = DmaStreamWriterBuilder::new(data_file)
            .with_write_behind(10)
            .with_buffer_size(512)
            .build();
        let mut index_write_stream = DmaStreamWriterBuilder::new(index_file)
            .with_write_behind(10)
            .with_buffer_size(512)
            .build();

        for (key, value) in self.flush_memtable.as_ref().unwrap().iter() {
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

        // TODO: data added to memtable between std::mem::swap and here, will
        // get destoryed, by creating a file as truncated.
        let mut wal_path = self.dir.clone();
        wal_path.push("memtable.log");
        self.wal_writer =
            DmaStreamWriterBuilder::new(DmaFile::create(wal_path).await?)
                .with_buffer_size(512)
                .with_write_behind(10)
                .build();

        self.flush_memtable = None;
        self.index += 1;

        Ok(())
    }
}
