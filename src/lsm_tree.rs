use std::io;

use crate::avl_tree::AVLtree;

pub struct LSMtree<K> {
    memtable: AVLtree<K>,
    memtable_threshold: u32,
    path: String,
    flush_count: u32,
}

impl<K: Ord + Clone> LSMtree<K> {
    fn new(path: String, memtable_threshold: u32) -> Self {
        Self {
            memtable: AVLtree::<K>::new(),
            memtable_threshold,
            path,
            flush_count: 0,
        }
    }

    fn insert(&mut self, key: K, value: Vec<u8>) -> io::Result<()> {
        if self.memtable.size() % self.memtable_threshold == 0 {
            let flush_path = format!("{0}/flush_{1}", self.path, self.flush_count);
            self.memtable.flush(flush_path)?;
        }

        self.memtable.insert(key, value);
        Ok(())
    }

    fn get(&self, key: K) -> io::Result<Vec<u8>> {
        self.memtable.get(&key)
    }

    fn contains(&self, key: K) -> bool {
        self.memtable.contains(&key)
    }
}
