use crate::lsm_tree::LSMTree;

// TODO: learn compile time rust to generate this automatically!
pub struct LSMTreeMut {
    ptr: *mut LSMTree,
}

impl LSMTreeMut {
    pub fn new(tree: LSMTree) -> Self {
        Self {
            ptr: Box::into_raw(Box::new(tree)),
        }
    }

    pub fn sstable_indices(&self) -> &Vec<usize> {
        unsafe { (*self.ptr).sstable_indices() }
    }

    pub fn memtable_full(&self) -> bool {
        unsafe { (*self.ptr).memtable_full() }
    }

    pub async fn get(
        &self,
        key: &Vec<u8>,
    ) -> glommio::Result<Option<Vec<u8>>, ()> {
        unsafe { (*self.ptr).get(key).await }
    }

    pub async fn set(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> glommio::Result<Option<Vec<u8>>, ()> {
        unsafe { (*self.ptr).set(key, value).await }
    }

    pub async fn delete(
        &self,
        key: Vec<u8>,
    ) -> glommio::Result<Option<Vec<u8>>, ()> {
        unsafe { (*self.ptr).delete(key).await }
    }

    pub async fn flush(&self) -> glommio::Result<(), ()> {
        unsafe { (*self.ptr).flush().await }
    }

    pub async fn compact(
        &self,
        indices_to_compact: Vec<usize>,
        output_index: usize,
        remove_tombstones: bool,
    ) -> std::io::Result<()> {
        unsafe {
            (*self.ptr)
                .compact(indices_to_compact, output_index, remove_tombstones)
                .await
        }
    }
}

impl Drop for LSMTreeMut {
    fn drop(&mut self) {
        unsafe {
            drop(Box::from_raw(self.ptr));
        }
    }
}
