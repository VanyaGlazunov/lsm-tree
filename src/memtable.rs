//! In-memory buffer implementations for LSM-tree writes.
//!
//! Memtables temporarily hold recent writes before being flushed to SSTables.
//! Two implementations are provided:
//! - [`BtreeMapMemtable`] - Based on std BTreeMap (uses RwLock)
//! - [`SkipListMemtable`] - Based on crossbeam SkipMap (lock-free)

use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicUsize, Ordering},
};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::sync::RwLock;

use crate::{lsm_storage::Record, sstable::builder::SSTableBuilder};

/// In-memory buffer for recent writes in an LSM-tree.
///
/// Memtables provide fast reads and writes by keeping data in memory.
/// When a memtable reaches its size limit, it becomes immutable and is flushed
/// to disk as an SSTable.
pub trait Memtable {
    /// Creates a new memtable with the given ID.
    fn new(id: usize) -> Self;

    /// Retrieves the record for a key, if it exists.
    fn get(&self, key: &[u8]) -> Option<Record>;

    /// Inserts or updates a key-value pair.
    fn set(&self, key: Bytes, value: Record);

    /// Returns the unique ID of this memtable.
    fn get_id(&self) -> usize;

    /// Returns an estimate of the total data size in bytes.
    ///
    /// Used to determine when to flush. Doesn't need to be exact.
    fn size_estimate(&self) -> usize;

    /// Returns a snapshot of all entries in sorted order.
    fn snapshot(&self) -> Vec<(Bytes, Record)>;

    /// Writes all entries to an SSTable builder.
    ///
    /// Override this for more efficient implementations than using [`snapshot()`](Memtable::snapshot).
    fn write_to_sst(&self, builder: &mut SSTableBuilder) {
        for (k, v) in self.snapshot() {
            builder.add(k, v);
        }
    }
}

/// Memtable implementation using [`std::collections::BTreeMap`].
///
/// Uses [`RwLock`] for synchronization - allows multiple concurrent readers
/// or a single writer.
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

/// Memtable implementation using [`crossbeam_skiplist::SkipMap`].
///
/// Lock-free implementation with better concurrent write performance
/// than [`BtreeMapMemtable`].
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
