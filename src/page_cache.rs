use core::hash::Hash;
use std::{cell::RefCell, rc::Rc};

use caches::{Cache, WTinyLFUCache};

pub const PAGE_SIZE: usize = 4096;

pub type Page = Rc<[u8; PAGE_SIZE]>;

type CacheKey<K> = (String, K, u64);
pub type PageCache<K> = WTinyLFUCache<CacheKey<K>, Page>;

pub fn align_up(address: u64) -> u64 {
    (address + (PAGE_SIZE as u64) - 1) & !((PAGE_SIZE as u64) - 1)
}

pub fn align_down(address: u64) -> u64 {
    address & !((PAGE_SIZE as u64) - 1)
}

pub struct PartitionPageCache<K: Hash + Eq> {
    name: String,
    cache: Rc<RefCell<PageCache<K>>>,
}

impl<K: Hash + Eq> PartitionPageCache<K> {
    pub fn new(name: String, cache: Rc<RefCell<PageCache<K>>>) -> Self {
        Self { name, cache }
    }

    #[inline]
    fn full_key(&self, partial_key: K, address: u64) -> CacheKey<K> {
        (self.name.clone(), partial_key, address)
    }

    pub fn get(&self, key: K, address: u64) -> Option<Page> {
        self.cache
            .borrow_mut()
            .get(&self.full_key(key, address))
            .map(|p| p.clone())
    }

    pub fn set(&self, key: K, address: u64, value: Page) {
        self.cache
            .borrow_mut()
            .put(self.full_key(key, address), value);
    }
}
