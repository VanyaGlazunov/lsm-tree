use std::collections::BTreeMap;

use bytes::Bytes;

pub trait Memtable {
    fn new(id: usize) -> Self;
    fn get(&self, key: &[u8]) -> Option<Bytes>;
    fn set(&mut self, key: Bytes, value: Bytes);
    fn get_id(&self) -> usize;
    fn size_estimate(&self) -> usize;
    fn iter(&self) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + '_>;
}

#[derive(Clone, Debug)]
pub struct BtreeMapMemtable {
    id: usize,
    container: BTreeMap<Bytes, Bytes>,
}

impl Memtable for BtreeMapMemtable {
    fn new(id: usize) -> Self {
        Self {
            id,
            container: BTreeMap::new(),
        }
    }

    fn get_id(&self) -> usize {
        self.id
    }

    fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.container.get(key).cloned()
    }

    fn set(&mut self, key: Bytes, value: Bytes) {
        self.container.insert(key, value);
    }

    fn size_estimate(&self) -> usize {
        self.iter().fold(0, |acc, e| acc + e.0.len() + e.1.len())
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + '_> {
        Box::new(self.container.iter().map(|(k, v)| (k.clone(), v.clone())))
    }
}
