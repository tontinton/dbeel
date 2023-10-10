use std::{cell::RefCell, collections::HashMap, rc::Rc};

use event_listener::Event;
use glommio::io::DmaFile;

use super::page_cache::{align_down, align_up, PartitionPageCache, PAGE_SIZE};
use crate::error::Result;

/// Number of times to try to get from the cache right after waiting for the
/// page to be read.
const RETRIES: usize = 2;

pub type FileId = (&'static str, usize);

pub struct CachedFileReader {
    id: FileId,
    file: DmaFile,
    cache: Rc<PartitionPageCache<FileId>>,
    read_events: RefCell<HashMap<u64, Rc<Event>>>,
}

impl CachedFileReader {
    pub fn new(
        id: FileId,
        file: DmaFile,
        cache: Rc<PartitionPageCache<FileId>>,
    ) -> Self {
        Self {
            id,
            file,
            cache,
            read_events: RefCell::new(HashMap::new()),
        }
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

            for i in 0..RETRIES {
                if let Some(page) = self
                    .cache
                    .borrow_mut()
                    .get(&self.cache.full_key(self.id, address))
                {
                    output_buf[written..written + write_size]
                        .copy_from_slice(&page[start..end]);
                    written += write_size;
                    break;
                }

                // Read from disk / wait for someone to read from disk.

                let mut first_reader = false;
                if i < RETRIES - 1 {
                    let maybe_event =
                        self.read_events.borrow().get(&address).cloned();
                    if let Some(event) = maybe_event {
                        event.listen().await;
                        continue;
                    } else {
                        first_reader = true;
                    }
                }

                if first_reader {
                    self.read_events
                        .borrow_mut()
                        .insert(address, Rc::new(Event::new()));
                }

                let page =
                    self.file.read_at_aligned(address, PAGE_SIZE).await?;

                if first_reader {
                    if let Some(event) =
                        self.read_events.borrow_mut().remove(&address)
                    {
                        event.notify(usize::MAX);
                    }
                }

                output_buf[written..written + write_size]
                    .copy_from_slice(&page[start..end]);
                written += write_size;

                let mut page_buf = [0; PAGE_SIZE];
                page_buf[..page.len()].copy_from_slice(&page);

                self.cache.set(self.id, address, page_buf);

                break;
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
