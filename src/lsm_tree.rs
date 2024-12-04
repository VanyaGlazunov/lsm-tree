use crate::avl_tree::AVLtree;
use chunkfs::{Data, DataContainer, Database};
use std::{
    fs::File,
    io,
    path::{Path, PathBuf},
};

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
        let prev_size = self.memtable.size();

        self.memtable.insert(key, value);

        if self.memtable.size() > prev_size && self.memtable.size() % self.memtable_threshold == 0 {
            let flush_path = self.path.join(format!("flush_{0}", self.flush_count));
            File::create(flush_path.as_path())?;
            self.memtable.flush(flush_path.as_path())?;
            self.flush_count += 1;
        }

        Ok(())
    }

    pub fn get(&self, key: &K) -> io::Result<Vec<u8>> {
        self.memtable.get(key)
    }

    pub fn contains(&self, key: &K) -> bool {
        self.memtable.contains(key)
    }

    pub fn get_flush_count(&self) -> u32 {
        self.flush_count
    }
}

impl<K: Ord> Database<K, DataContainer<()>> for LSMtree<K> {
    fn insert(&mut self, key: K, value: DataContainer<()>) -> io::Result<()> {
        match value.extract() {
            Data::Chunk(chunk) => self.insert(key, chunk.clone()),
            Data::TargetChunk(_target_chunk) => unimplemented!(),
        }
    }

    fn get(&self, key: &K) -> io::Result<DataContainer<()>> {
        match self.get(key) {
            Ok(chunk) => Ok(DataContainer::from(chunk)),
            Err(e) => Err(e),
        }
    }

    fn remove(&mut self, _key: &K) {
        unimplemented!()
    }

    fn contains(&self, key: &K) -> bool {
        self.contains(key)
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

        assert_eq!(lsm.get(&1).unwrap(), b"value1".to_vec());
        assert_eq!(lsm.get(&2).unwrap(), b"value2".to_vec());
        assert_eq!(lsm.get(&3).unwrap(), b"value3".to_vec());
        assert_eq!(lsm.get(&4).unwrap(), b"value4".to_vec());

        Ok(())
    }
}
