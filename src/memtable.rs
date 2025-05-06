use std::collections::BTreeMap;

use bytes::Bytes;

use crate::lsm_storage::Record;

pub trait Memtable {
    fn new(id: usize) -> Self;
    fn get(&self, key: &[u8]) -> Option<Record>;
    fn set(&mut self, key: Bytes, value: Record);
    fn get_id(&self) -> usize;
    fn size_estimate(&self) -> usize;
    fn iter(&self) -> Box<dyn Iterator<Item = (Bytes, Record)> + '_>;
}

#[derive(Clone, Debug)]
pub struct BtreeMapMemtable {
    id: usize,
    size: usize,
    container: BTreeMap<Bytes, Record>,
}

impl Memtable for BtreeMapMemtable {
    fn new(id: usize) -> Self {
        Self {
            id,
            size: 0,
            container: BTreeMap::new(),
        }
    }

    fn get_id(&self) -> usize {
        self.id
    }

    fn get(&self, key: &[u8]) -> Option<Record> {
        self.container.get(key).cloned()
    }

    fn set(&mut self, key: Bytes, value: Record) {
        self.size += key.len() + value.value_len();
        self.container.insert(key, value);
    }

    fn size_estimate(&self) -> usize {
        self.size
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (Bytes, Record)> + '_> {
        Box::new(self.container.iter().map(|(k, v)| (k.clone(), v.clone())))
    }
}
