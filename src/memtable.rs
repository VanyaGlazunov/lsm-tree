use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicUsize, Ordering},
};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::sync::RwLock;

use crate::{lsm_storage::Record, sstable::builder::SSTableBuilder};

/// Trait represents in-memmory buffer of Log-Structured Merge Tree.
pub trait Memtable {
    /// Creates new instance with given ID.
    ///
    /// [crate::lsm_storage::LSMStorage] associates monotonically increasing ID for each memtable and keeps a number of created memtables.
    fn new(id: usize) -> Self;
    /// Returns for given key if it exists, None otherwise.
    fn get(&self, key: &[u8]) -> Option<Record>;
    /// Inserts/Updates key-value pair.
    fn set(&self, key: Bytes, value: Record);
    /// Returns memtable ID.
    fn get_id(&self) -> usize;
    /// Estimates the total size of stored data.
    ///
    /// It is advised to implement it by adding size of keys and values inside [Memtable::set] method.
    /// Estimations doesn't need to be very accurate, speed matters more.
    fn size_estimate(&self) -> usize;

    /// Provides currently stored data.
    fn snapshot(&self) -> Vec<(Bytes, Record)>;

    /// Builds SSTable from memtable via SSTableBuilder.
    /// [crate::lsm_storage::LSMStorage] gurantees to use this method on immutable memtables only.
    /// Default uses snapshot, override for effective way to sink everything from memtable to [SSTableBuilder]
    fn write_to_sst(&self, builder: &mut SSTableBuilder) {
        for (k, v) in self.snapshot() {
            builder.add(k, v);
        }
    }
}

/// Memtable implementation based on [BTreeMap]
#[derive(Debug)]
pub struct BtreeMapMemtable {
    id: usize,
    size: AtomicUsize,
    container: RwLock<BTreeMap<Bytes, Record>>,
}

impl Memtable for BtreeMapMemtable {
    fn new(id: usize) -> Self {
        Self {
            id,
            size: 0.into(),
            container: BTreeMap::new().into(),
        }
    }

    fn get_id(&self) -> usize {
        self.id
    }

    fn get(&self, key: &[u8]) -> Option<Record> {
        self.container
            .read()
            .map_or(None, |btree| btree.get(key).cloned())
    }

    fn set(&self, key: Bytes, value: Record) {
        self.size
            .fetch_add(key.len() + value.value_len(), Ordering::Relaxed);
        let mut btree = self.container.write().unwrap();
        btree.insert(key, value);
    }

    fn size_estimate(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    fn snapshot(&self) -> Vec<(Bytes, Record)> {
        self.container
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn write_to_sst(&self, builder: &mut SSTableBuilder) {
        let btree = self.container.read().unwrap();
        for (key, value) in &*btree {
            builder.add(key.clone(), value.clone());
        }
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

    fn set(&self, key: Bytes, value: Record) {
        self.size
            .fetch_add(key.len() + value.value_len(), Ordering::Relaxed);
        self.container.insert(key, value);
    }

    fn get_id(&self) -> usize {
        self.id
    }

    fn size_estimate(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    fn snapshot(&self) -> Vec<(Bytes, Record)> {
        self.container
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }

    fn write_to_sst(&self, builder: &mut SSTableBuilder) {
        for entry in &self.container {
            builder.add(entry.key().clone(), entry.value().clone());
        }
    }
}
