use core::hash::Hash;
use std::{
    cell::{RefCell, RefMut},
    rc::Rc,
};

use murmur3::murmur3_32;
use wtinylfu::WTinyLfuCache;

pub const PAGE_SIZE: usize = 4096;

pub type Page = [u8; PAGE_SIZE];

type CacheKey<K> = (u32, K, u64);
pub type PageCache<K> = WTinyLfuCache<CacheKey<K>, Page>;

pub fn align_up(address: u64) -> u64 {
    (address + (PAGE_SIZE as u64) - 1) & !((PAGE_SIZE as u64) - 1)
}

pub fn align_down(address: u64) -> u64 {
    address & !((PAGE_SIZE as u64) - 1)
}

pub struct PartitionPageCache<K: Hash + Eq> {
    name_hash: u32,
    cache: Rc<RefCell<PageCache<K>>>,
}

impl<K: Hash + Eq> PartitionPageCache<K> {
    pub fn new(name_hash: u32, cache: Rc<RefCell<PageCache<K>>>) -> Self {
        Self { name_hash, cache }
    }

    pub fn new_named(
        name: &str,
        cache: Rc<RefCell<PageCache<K>>>,
    ) -> std::io::Result<Self> {
        let name_hash = murmur3_32(&mut std::io::Cursor::new(name), 0)?;
        Ok(Self::new(name_hash, cache))
    }

    pub fn full_key(&self, partial_key: K, address: u64) -> CacheKey<K> {
        (self.name_hash, partial_key, address)
    }

    pub fn get_copied(&self, key: K, address: u64) -> Option<Page> {
        self.cache
            .borrow_mut()
            .get(&self.full_key(key, address))
            .cloned()
    }

    pub fn borrow_mut(&self) -> RefMut<PageCache<K>> {
        self.cache.borrow_mut()
    }

    pub fn set(&self, key: K, address: u64, value: Page) {
        self.cache
            .borrow_mut()
            .put(self.full_key(key, address), value);
    }
}
