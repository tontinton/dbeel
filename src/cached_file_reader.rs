use crate::error::Result;
use crate::page_cache::{
    align_down, align_up, Page, PartitionPageCache, PAGE_SIZE,
};
use glommio::io::DmaFile;
use std::rc::Rc;

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

    pub async fn read_at(&self, pos: u64, size: usize) -> Result<Vec<u8>> {
        assert_ne!(size, 0);

        // The address to the first page.
        let low_page = align_down(pos);

        // The address to the last page, we don't read it.
        let high_page = align_up(pos + size as u64);

        let mut output_buf = Vec::with_capacity(size);

        for address in (low_page..high_page).step_by(PAGE_SIZE) {
            let start = if address == low_page {
                (pos - low_page) as usize
            } else {
                0
            };
            let end = std::cmp::min(PAGE_SIZE, size - output_buf.len() + start);

            'wait_for_fault: loop {
                match self.cache.get(self.id.clone(), address) {
                    Some(page) => {
                        output_buf
                            .extend_from_slice(&page.as_slice()[start..end]);
                    }
                    None => {
                        let page = self
                            .file
                            .read_at_aligned(address, PAGE_SIZE)
                            .await?;

                        output_buf.extend_from_slice(&page[start..end]);

                        let mut page_buf = [0; PAGE_SIZE];
                        page_buf[..page.len()].copy_from_slice(&page);

                        self.cache.set(
                            self.id.clone(),
                            address,
                            Page::new(page_buf),
                        );
                    }
                }

                break 'wait_for_fault;
            }
        }

        assert_eq!(output_buf.len(), size);

        Ok(output_buf)
    }
}
