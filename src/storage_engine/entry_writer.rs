use std::rc::Rc;

use bincode::Options;
use futures::{try_join, AsyncWrite, AsyncWriteExt};
use glommio::io::{DmaFile, DmaStreamWriterBuilder};

use super::{
    bincode_options,
    cached_file_reader::FileId,
    page_cache::{Page, PartitionPageCache, PAGE_SIZE},
    Entry, EntryOffset, DATA_FILE_EXT, DMA_STREAM_NUMBER_OF_BUFFERS,
    INDEX_ENTRY_SIZE, INDEX_FILE_EXT,
};
use crate::error::Result;

pub struct EntryWriter {
    data_writer: Box<(dyn AsyncWrite + std::marker::Unpin)>,
    index_writer: Box<(dyn AsyncWrite + std::marker::Unpin)>,
    files_index: usize,
    page_cache: Rc<PartitionPageCache<FileId>>,
    data_buf: [u8; PAGE_SIZE],
    data_written: usize,
    index_buf: [u8; PAGE_SIZE],
    index_written: usize,
}

impl EntryWriter {
    pub fn new_from_dma(
        data_file: DmaFile,
        index_file: DmaFile,
        files_index: usize,
        page_cache: Rc<PartitionPageCache<FileId>>,
    ) -> Self {
        let data_writer = Box::new(
            DmaStreamWriterBuilder::new(data_file)
                .with_write_behind(DMA_STREAM_NUMBER_OF_BUFFERS)
                .with_buffer_size(PAGE_SIZE)
                .build(),
        );
        let index_writer = Box::new(
            DmaStreamWriterBuilder::new(index_file)
                .with_write_behind(1)
                .with_buffer_size(INDEX_ENTRY_SIZE)
                .build(),
        );

        Self::new(data_writer, index_writer, files_index, page_cache)
    }

    pub fn new(
        data_writer: Box<(dyn AsyncWrite + std::marker::Unpin)>,
        index_writer: Box<(dyn AsyncWrite + std::marker::Unpin)>,
        files_index: usize,
        page_cache: Rc<PartitionPageCache<FileId>>,
    ) -> Self {
        Self {
            data_writer,
            index_writer,
            files_index,
            page_cache,
            data_buf: [0; PAGE_SIZE],
            data_written: 0,
            index_buf: [0; PAGE_SIZE],
            index_written: 0,
        }
    }

    pub async fn write(&mut self, entry: &Entry) -> Result<(usize, usize)> {
        let data_encoded = bincode_options().serialize(entry)?;
        let data_size = data_encoded.len();

        let entry_index = EntryOffset {
            offset: self.data_written as u64,
            size: data_size,
        };
        let index_encoded = bincode_options().serialize(&entry_index)?;
        let index_size = index_encoded.len();

        try_join!(
            self.data_writer.write_all(&data_encoded),
            self.index_writer.write_all(&index_encoded)
        )?;

        self.write_to_cache(data_encoded, true);
        self.write_to_cache(index_encoded, false);

        Ok((data_size, index_size))
    }

    fn write_to_cache(&mut self, bytes: Vec<u8>, is_data_file: bool) {
        let (buf, written, ext) = if is_data_file {
            (&mut self.data_buf, &mut self.data_written, DATA_FILE_EXT)
        } else {
            (&mut self.index_buf, &mut self.index_written, INDEX_FILE_EXT)
        };

        for chunk in bytes.chunks(PAGE_SIZE) {
            let data_buf_offset = *written % PAGE_SIZE;
            let end = std::cmp::min(data_buf_offset + chunk.len(), PAGE_SIZE);
            buf[data_buf_offset..end]
                .copy_from_slice(&chunk[..end - data_buf_offset]);

            let written_first_copy = end - data_buf_offset;
            *written += written_first_copy;

            if *written % PAGE_SIZE == 0 {
                // Filled a page, write it to cache.
                self.page_cache.set(
                    (ext, self.files_index),
                    *written as u64 - PAGE_SIZE as u64,
                    Page::new(std::mem::replace(buf, [0; PAGE_SIZE])),
                );

                // Write whatever is left in the chunk.
                let left = chunk.len() - written_first_copy;
                buf[..left].copy_from_slice(&chunk[written_first_copy..]);
                *written += left;
            }
        }
    }

    pub async fn close(&mut self) -> Result<()> {
        let data_left = self.data_written % PAGE_SIZE;
        if data_left != 0 {
            self.page_cache.set(
                (DATA_FILE_EXT, self.files_index),
                (self.data_written - data_left) as u64,
                Page::new(self.data_buf),
            );
        }
        let index_left = self.index_written % PAGE_SIZE;
        if self.index_written % PAGE_SIZE != 0 {
            self.page_cache.set(
                (INDEX_FILE_EXT, self.files_index),
                (self.index_written - index_left) as u64,
                Page::new(self.index_buf),
            );
        }

        try_join!(self.data_writer.close(), self.index_writer.close())?;
        Ok(())
    }
}
