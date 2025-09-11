use std::path::Path;

use crate::{lsm_storage::LSMStorage, memtable::Memtable};
use anyhow::Result;

const DEFAULT_BLOCK_SIZE: usize = 1 << 12;
const DEFAULT_MEMTABLE_SIZE: usize = 1 << 23;
const DEFAULT_NUM_FLUSH_WORKERS: usize = 2;
const DEFAULT_MAX_L0_SSTS: usize = 4;
const DEFAULT_DURABLE_WAL: bool = false;

/// Configuration for LSMStorage
#[derive(Clone)]
pub struct LSMStorageOptions {
    pub block_size: usize,
    pub memtables_size: usize,
    pub num_flush_jobs: usize,
    pub max_l0_ssts: usize,
    pub durable_wal: bool,
}

impl LSMStorageOptions {
    /// Size of a block in SSTable.
    pub fn block_size(mut self, size: usize) -> Self {
        self.block_size = size;
        self
    }
    /// Size of memtables
    pub fn memtable_size(mut self, size: usize) -> Self {
        self.memtables_size = size;
        self
    }
    /// Number of L0 sstables to trigger compaction
    pub fn max_l0_ssts(mut self, max_number: usize) -> Self {
        self.max_l0_ssts = max_number;
        self
    }
    /// Number of async flush tasks
    pub fn num_flush_jobs(mut self, jobs: usize) -> Self {
        self.num_flush_jobs = jobs;
        self
    }
    /// Sync after each append to the wal
    pub fn durable_wal(mut self, durable: bool) -> Self {
        self.durable_wal = durable;
        self
    }
    pub fn open<M: Memtable + Sync + Send>(self, path: impl AsRef<Path>) -> Result<LSMStorage<M>> {
        LSMStorage::open(path, self)
    }
}

impl Default for LSMStorageOptions {
    fn default() -> Self {
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
            memtables_size: DEFAULT_MEMTABLE_SIZE,
            num_flush_jobs: DEFAULT_NUM_FLUSH_WORKERS,
            max_l0_ssts: DEFAULT_MAX_L0_SSTS,
            durable_wal: DEFAULT_DURABLE_WAL,
        }
    }
}
