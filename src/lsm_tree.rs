use std::{
    fs::File,
    io,
    path::{Path, PathBuf},
};

use crate::avl_tree::AVLtree;

pub struct LSMtree<K> {
    memtable: AVLtree<K>,
    memtable_threshold: u32,
    path: PathBuf,
    flush_count: u32,
}

impl<K: Ord> LSMtree<K> {
    pub fn new(path: &Path, memtable_threshold: u32) -> Self {
        Self {
            memtable: AVLtree::<K>::new(),
            memtable_threshold,
            path: path.to_path_buf(),
            flush_count: 0,
        }
    }

    pub fn insert(&mut self, key: K, value: Vec<u8>) -> io::Result<()> {
        if self.memtable.size() != 0 && self.memtable.size() % self.memtable_threshold == 0 {
            let flush_path = self.path.join(format!("flust_{0}", self.flush_count));
            File::create(flush_path.as_path())?;
            self.memtable.flush(flush_path.as_path())?;
            self.flush_count += 1;
        }

        self.memtable.insert(key, value);
        Ok(())
    }

    pub fn get(&self, key: K) -> io::Result<Vec<u8>> {
        self.memtable.get(&key)
    }

    pub fn contains(&self, key: K) -> bool {
        self.memtable.contains(&key)
    }

    pub fn get_flush_count(&self) -> u32 {
        self.flush_count
    }
}

#[cfg(test)]
mod tests {
    use super::LSMtree;
    use std::io;
    use tempfile::TempDir;

    #[test]
    pub fn insert_above_threshold() -> io::Result<()> {
        let tempdir = &TempDir::new().unwrap();
        let mut lsm = LSMtree::new(tempdir.path(), 3);

        lsm.insert(1, b"value1".to_vec())?;
        lsm.insert(2, b"value2".to_vec())?;
        lsm.insert(3, b"value3".to_vec())?;
        lsm.insert(4, b"value4".to_vec())?;

        assert_eq!(lsm.flush_count, 1);

        assert_eq!(lsm.get(1).unwrap(), b"value1".to_vec());
        assert_eq!(lsm.get(2).unwrap(), b"value2".to_vec());
        assert_eq!(lsm.get(3).unwrap(), b"value3".to_vec());
        assert_eq!(lsm.get(4).unwrap(), b"value4".to_vec());

        Ok(())
    }
}
