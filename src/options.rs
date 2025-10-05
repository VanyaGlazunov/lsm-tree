//! Configuration options for LSM-tree storage engine.

use std::path::Path;

use crate::{lsm_storage::LSMStorage, memtable::Memtable};
use anyhow::Result;

const DEFAULT_BLOCK_SIZE: usize = 1 << 12;
const DEFAULT_MEMTABLE_SIZE: usize = 1 << 23;
const DEFAULT_NUM_FLUSH_WORKERS: usize = 2;
const DEFAULT_MAX_L0_SSTS: usize = 4;
const DEFAULT_DURABLE_WAL: bool = false;
const DEFAULT_BASE_LEVEL_SIZE_BYTES: usize = 10 * 1024 * 1024;
const DEFAULT_LEVEL_SIZE_MULTIPLIER: usize = 10;

/// Configuration options for LSM-tree storage engine.
///
/// # Example
/// ```no_run
/// use lsm_tree::options::LSMStorageOptions;
///
/// let options = LSMStorageOptions::default()
///     .block_size(4096)
///     .memtable_size(8 * 1024 * 1024)
///     .max_l0_ssts(4)
///     .num_flush_jobs(2)
///     .durable_wal(false);
/// ```
#[derive(Clone)]
pub struct LSMStorageOptions {
    /// Size of each block in bytes within SSTables.
    pub block_size: usize,
    /// Maximum size of memtable in bytes before triggering flush.
    pub memtables_size: usize,
    /// Number of concurrent flush worker tasks.
    pub num_flush_jobs: usize,
    /// Number of L0 SSTables to trigger compaction.
    pub max_l0_ssts: usize,
    /// Whether to fsync WAL after each write for durability.
    pub durable_wal: bool,
    /// Target size in bytes for L1 (base level).
    pub base_level_size_bytes: usize,
    /// Size multiplier between levels (e.g., 10 means L2 is 10x L1).
    pub level_size_multiplier: usize,
}

impl LSMStorageOptions {
    /// Sets the block size for SSTables in bytes.
    ///
    /// Smaller blocks reduce read amplification but increase metadata overhead.
    /// Larger blocks are more efficient but may read unnecessary data.
    ///
    /// Default: 4096 bytes (4 KB)
    pub fn block_size(mut self, size: usize) -> Self {
        self.block_size = size;
        self
    }

    /// Sets the maximum memtable size in bytes.
    ///
    /// When a memtable exceeds this size, it's flushed to disk as an SSTable.
    /// Larger memtables reduce write amplification but use more memory.
    ///
    /// Default: 8 MB
    pub fn memtable_size(mut self, size: usize) -> Self {
        self.memtables_size = size;
        self
    }

    /// Sets the number of L0 SSTables that trigger compaction.
    ///
    /// When L0 has this many SSTables, they're compacted into L1.
    /// Lower values reduce read amplification but increase write amplification.
    ///
    /// Default: 4 SSTables
    pub fn max_l0_ssts(mut self, max_number: usize) -> Self {
        self.max_l0_ssts = max_number;
        self
    }

    /// Sets the number of concurrent flush worker tasks.
    ///
    /// More workers allow parallel flushing but use more resources.
    ///
    /// Default: 2 workers
    pub fn num_flush_jobs(mut self, jobs: usize) -> Self {
        self.num_flush_jobs = jobs;
        self
    }

    /// Enables or disables durable WAL writes.
    ///
    /// When true, each WAL write is fsynced to disk for durability.
    /// When false, writes are buffered for better performance but less durability.
    ///
    /// Default: false
    pub fn durable_wal(mut self, durable: bool) -> Self {
        self.durable_wal = durable;
        self
    }

    /// Sets the target size for L1 (base level) in bytes.
    ///
    /// This determines when L1 should compact into L2.
    /// Larger values reduce write amplification but increase read amplification.
    ///
    /// Default: 10 MB
    pub fn base_level_size_bytes(mut self, size: usize) -> Self {
        self.base_level_size_bytes = size;
        self
    }

    /// Sets the size multiplier between levels.
    ///
    /// For example, with multiplier 10: L2 = 10 × L1, L3 = 10 × L2, etc.
    /// Higher values reduce write amplification but increase space amplification.
    ///
    /// Default: 10
    pub fn level_size_multiplier(mut self, multiplier: usize) -> Self {
        self.level_size_multiplier = multiplier;
        self
    }

    /// Opens LSM-tree storage with these options.
    ///
    /// # Type Parameters
    /// - `M`: The memtable implementation to use (e.g., BtreeMapMemtable, SkipListMemtable)
    ///
    /// # Errors
    /// Returns an error if unable to create/open storage directory or recover state.
    pub async fn open<M: Memtable + Sync + Send + 'static>(
        self,
        path: impl AsRef<Path>,
    ) -> Result<LSMStorage<M>> {
        LSMStorage::open(path, self).await
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
            base_level_size_bytes: DEFAULT_BASE_LEVEL_SIZE_BYTES,
            level_size_multiplier: DEFAULT_LEVEL_SIZE_MULTIPLIER,
        }
    }
}
