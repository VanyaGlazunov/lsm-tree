use std::{collections::BTreeMap, sync::atomic::AtomicUsize};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

use crate::lsm_storage::Record;

/// Trait represents in-memmory buffer of Log-Structured Merge Tree.
pub trait Memtable {
    /// Creates new instance with given ID.
    ///
    /// [crate::lsm_storage::LSMStorage] associates monotonically increasing ID for each memtable and keeps a number of created memtables.
    fn new(id: usize) -> Self;
    /// Returns for given key if it exists, None otherwise.
    fn get(&self, key: &[u8]) -> Option<Record>;
    /// Inserts/Updates key-value pair.
    fn set(&mut self, key: Bytes, value: Record);
    /// Returns memtable ID.
    fn get_id(&self) -> usize;
    /// Estimates the total size of stored data.
    ///
    /// It is advised to implement it by adding size of keys and values inside [Memtable::set] method.
    /// Estimations doesn't need to be very accurate, speed matters more.
    fn size_estimate(&self) -> usize;

    /// Creates iterator overy sorted entries.
    fn iter(&self) -> Box<dyn Iterator<Item = (Bytes, Record)> + '_>;
}

/// Memtable implementation based on [BTreeMap]
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

/// Memtable implementation based on [SkipMap]
pub struct SkipListMemtable {
    id: usize,
    size: AtomicUsize,
    container: SkipMap<Bytes, Record>,
}

impl Memtable for SkipListMemtable {
    fn new(id: usize) -> Self {
        Self {
            id,
            size: AtomicUsize::new(0),
            container: SkipMap::new(),
        }
    }

    fn get(&self, key: &[u8]) -> Option<Record> {
        self.container.get(key).map(|e| e.value().clone())
    }

    fn set(&mut self, key: Bytes, value: Record) {
        let add = key.len() + value.value_len();
        self.size
            .fetch_add(add, std::sync::atomic::Ordering::Relaxed);
        self.container.insert(key, value);
    }

    fn get_id(&self) -> usize {
        self.id
    }

    fn size_estimate(&self) -> usize {
        self.size.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (Bytes, Record)> + '_> {
        Box::new(
            self.container
                .iter()
                .map(|e| (e.key().clone(), e.value().clone())),
        )
    }
}
