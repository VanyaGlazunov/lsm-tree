use crate::avl_tree::{AVLtree, NodeData};
use chunkfs::{Data, DataContainer, Database};
use std::{
    fs::File,
    io::{self, Read, Seek, Write},
    path::{Path, PathBuf},
};

pub struct LSMtree<K> {
    memtable: AVLtree<K>,
    memtable_threshold: usize,
    path: PathBuf,
    flush_count: usize,
    tables: Vec<PathBuf>,
}

impl<K: Ord> LSMtree<K> {
    pub fn new(path: &Path, memtable_threshold: usize) -> Self {
        Self {
            memtable: AVLtree::<K>::new(),
            memtable_threshold,
            path: path.to_path_buf(),
            flush_count: 0,
            tables: Vec::new(),
        }
    }

    pub fn insert(&mut self, key: K, value: Vec<u8>) -> io::Result<()> {
        self.memtable.insert(key, value);

        if self.memtable.size() >= self.memtable_threshold {
            let flush_path = self.path.join(format!("flush_{0}", self.flush_count));
            match self.memtable.flush(self.flush_count) {
                None => return Ok(()),
                Some(flushed) => {
                    let mut file = File::create(flush_path.as_path())?;
                    self.tables.push(flush_path);
                    file.write_all(&flushed)?;
                }
            }
            self.flush_count += 1;
        }

        Ok(())
    }

    pub fn get(&self, key: &K) -> io::Result<Vec<u8>> {
        match self.memtable.get(key) {
            Ok(data) => match data {
                NodeData::Pointer(table, position, size) => {
                    let mut file = File::open(self.tables[table].clone())?;

                    file.seek(io::SeekFrom::Start(position as u64))?;
                    let mut value = vec![0; size];
                    file.read_exact(&mut value)?;
                    Ok(value)
                }
                NodeData::Value(chunk) => Ok(chunk),
            },
            Err(e) => Err(e),
        }
    }

    pub fn contains(&self, key: &K) -> bool {
        self.memtable.contains(key)
    }

    pub fn get_flush_count(&self) -> usize {
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

    fn contains(&self, key: &K) -> bool {
        self.contains(key)
    }
}

#[cfg(test)]
mod tests {
    use super::LSMtree;
    use std::{env::temp_dir, io};

    #[test]
    pub fn insert_above_threshold() -> io::Result<()> {
        let path = temp_dir();
        let value_size = 6;
        let mut lsm = LSMtree::new(&path, value_size);

        lsm.insert(1, b"value1".to_vec())?;
        lsm.insert(2, b"value2".to_vec())?;
        lsm.insert(3, b"value3".to_vec())?;
        lsm.insert(4, b"value4".to_vec())?;

        assert_eq!(lsm.flush_count, 4);

        assert_eq!(lsm.get(&1).unwrap(), b"value1".to_vec());
        assert_eq!(lsm.get(&2).unwrap(), b"value2".to_vec());
        assert_eq!(lsm.get(&3).unwrap(), b"value3".to_vec());
        assert_eq!(lsm.get(&4).unwrap(), b"value4".to_vec());

        Ok(())
    }
}
