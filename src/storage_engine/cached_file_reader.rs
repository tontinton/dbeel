use std::rc::Rc;

use glommio::io::DmaFile;

use super::{
    page_cache::{align_down, align_up, PartitionPageCache, PAGE_SIZE},
    FileTypeKind,
};
use crate::error::Result;

pub type FileId = (FileTypeKind, usize);

pub struct CachedFileReader {
    id: FileId,
    file: Rc<DmaFile>,
    cache: Rc<PartitionPageCache<FileId>>,
}

impl CachedFileReader {
    pub fn new(
        id: FileId,
        file: Rc<DmaFile>,
        cache: Rc<PartitionPageCache<FileId>>,
    ) -> Self {
        Self { id, file, cache }
    }

    pub async fn read_at_into(
        &self,
        pos: u64,
        output_buf: &mut [u8],
    ) -> Result<()> {
        let size = output_buf.len();

        // The address to the first page.
        let low_page = align_down(pos);

        // The address to the last page, we don't read it.
        let high_page = align_up(pos + size as u64);

        let mut written = 0;

        for address in (low_page..high_page).step_by(PAGE_SIZE) {
            let start = if address == low_page {
                (pos - low_page) as usize
            } else {
                0
            };
            let end = std::cmp::min(PAGE_SIZE, size - written + start);
            let write_size = end - start;

            // Try to read from cache.
            if let Some(page) = self
                .cache
                .borrow_mut()
                .get(&self.cache.full_key(self.id, address))
            {
                output_buf[written..written + write_size]
                    .copy_from_slice(&page[start..end]);
                written += write_size;
                continue;
            }

            // Not found in cache, read from disk.
            let page = self.file.read_at_aligned(address, PAGE_SIZE).await?;

            output_buf[written..written + write_size]
                .copy_from_slice(&page[start..end]);
            written += write_size;

            let mut page_buf = [0; PAGE_SIZE];
            page_buf[..page.len()].copy_from_slice(&page);

            self.cache.set(self.id, address, page_buf);
        }

        assert_eq!(written, size);
        Ok(())
    }

    pub async fn read_at(&self, pos: u64, size: usize) -> Result<Vec<u8>> {
        assert_ne!(size, 0);

        let mut output_buf = vec![0; size];
        self.read_at_into(pos, &mut output_buf).await?;

        Ok(output_buf)
    }
}
