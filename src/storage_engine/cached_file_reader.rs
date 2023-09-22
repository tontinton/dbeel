use std::rc::Rc;

use glommio::io::DmaFile;

use super::page_cache::{align_down, align_up, PartitionPageCache, PAGE_SIZE};
use crate::error::Result;

pub type FileId = (&'static str, usize);

pub struct CachedFileReader {
    id: FileId,
    file: DmaFile,
    cache: Rc<PartitionPageCache<FileId>>,
}

impl CachedFileReader {
    pub fn new(
        id: FileId,
        file: DmaFile,
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

            match &self.cache.get(self.id, address) {
                Some(page) => {
                    output_buf[written..written + write_size]
                        .copy_from_slice(&page[start..end]);
                    written += write_size;
                }
                None => {
                    let page =
                        self.file.read_at_aligned(address, PAGE_SIZE).await?;

                    output_buf[written..written + write_size]
                        .copy_from_slice(&page[start..end]);
                    written += write_size;

                    let mut page_buf = [0; PAGE_SIZE];
                    page_buf[..page.len()].copy_from_slice(&page);

                    self.cache.set(self.id, address, page_buf);
                }
            }
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
