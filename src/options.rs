const DEFAULT_BLOCK_SIZE: usize = 1 << 12;
const DEFAULT_MEMTABLE_SIZE: usize = 1 << 23;
const DEFAULT_MEMTABLE_CAP: usize = 2;
const DEFAULT_NUM_FLUSH_WORKERS: usize = 2;
const DEFAULT_DURABLE_WAL: bool = false;

/// Configuration for LSMStorage
#[derive(Clone)]
pub struct LSMStorageOptions {
    /// Size of a block in SSTable.
    pub block_size: usize,
    /// Size of memtables
    pub memtables_size: usize,
    // Number of immutable memtables stored in memory.
    #[allow(unused)]
    pub memtables_cap: usize,
    /// Number of async flush tasks
    pub num_flush_jobs: usize,
    /// Sync after each append to the wal
    pub durable_wal: bool,
}

impl LSMStorageOptions {
    pub fn block_size(mut self, size: usize) -> Self {
        self.block_size = size;
        self
    }
    pub fn memtable_size(mut self, size: usize) -> Self {
        self.memtables_size = size;
        self
    }
    pub fn memtable_cap(mut self, cap: usize) -> Self {
        self.memtables_cap = cap;
        self
    }
    pub fn num_flush_jobs(mut self, jobs: usize) -> Self {
        self.num_flush_jobs = jobs;
        self
    }
    pub fn durable_wal(mut self, durable: bool) -> Self {
        self.durable_wal = durable;
        self
    }
}

impl Default for LSMStorageOptions {
    fn default() -> Self {
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
            memtables_size: DEFAULT_MEMTABLE_SIZE,
            memtables_cap: DEFAULT_MEMTABLE_CAP,
            num_flush_jobs: DEFAULT_NUM_FLUSH_WORKERS,
            durable_wal: DEFAULT_DURABLE_WAL,
        }
    }
}
