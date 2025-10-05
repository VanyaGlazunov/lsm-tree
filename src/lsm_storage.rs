//! Log-Structured Merge-tree (LSM-tree) storage engine.

use std::{
    collections::{BTreeMap, HashSet},
    fs::{create_dir_all, remove_file},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use anyhow::{Context, Error, Result};
use arc_swap::ArcSwap;
use bytes::Bytes;
use tokio::{
    self,
    sync::{mpsc, oneshot, watch, Mutex},
    task::JoinHandle,
};
use tracing::error;

use crate::{
    compaction::CompactionResult,
    manifest::{Manifest, ManifestRecord},
    memtable::Memtable,
    options::LSMStorageOptions,
    sstable::{builder::SSTableBuilder, SSTable},
    wal::Wal,
};

const TOMBSTONE: Record = Record::Delete;
const FLUSH_CHANNEL_SIZE: usize = 100;
const STATE_CHANNEL_SIZE: usize = 300;

pub(crate) type ImmutableMemtableMap<M> = BTreeMap<usize, Arc<M>>;
pub(crate) type SSTableMap = BTreeMap<usize, Arc<SSTable>>;
pub(crate) type Levels = Vec<Vec<usize>>;

/// Builds an SSTable from a memtable.
pub(crate) async fn memtable_to_sst(
    block_size: usize,
    path: impl AsRef<Path>,
    memtable: &impl Memtable,
) -> Result<SSTable> {
    let mut builder = SSTableBuilder::new(block_size);
    memtable.write_to_sst(&mut builder);

    let id = memtable.get_id();
    builder
        .build(SSTable::get_sst_path(path, id))
        .await
        .context(format!("Failed to flush memtable. id: {id}"))
}

/// Represents a key-value record in the LSM-tree.
///
/// Records can either contain a value ([`Record::Put`]) or represent
/// a deletion tombstone ([`Record::Delete`]).
#[derive(Clone, PartialEq, Debug)]
pub enum Record {
    Put(Bytes),
    Delete,
}

impl Record {
    pub fn value_len(&self) -> usize {
        match self {
            Record::Put(val) => val.len(),
            Record::Delete => 0,
        }
    }

    pub fn put_from_slice(data: impl AsRef<[u8]>) -> Self {
        Record::Put(Bytes::copy_from_slice(data.as_ref()))
    }

    pub fn into_inner(self) -> Option<Bytes> {
        match self {
            Record::Delete => None,
            Record::Put(val) => Some(val),
        }
    }
}

pub(crate) struct StateSnapshot<M> {
    pub imm_memtables: Arc<ImmutableMemtableMap<M>>,
    pub levels: Arc<Levels>,
    pub sstables: Arc<SSTableMap>,
}

impl<M> Clone for StateSnapshot<M> {
    fn clone(&self) -> Self {
        Self {
            imm_memtables: self.imm_memtables.clone(),
            levels: self.levels.clone(),
            sstables: self.sstables.clone(),
        }
    }
}

pub(crate) struct StateWriterContext<M> {
    pub manifest: Mutex<Manifest>,
    pub snapshot_sender: watch::Sender<StateSnapshot<M>>,
}

/// Log-Structured Merge-tree (LSM-tree) storage engine.
///
/// This is the main entry point for the LSM-tree key-value store. It provides
/// asynchronous operations for inserting, retrieving, and deleting keys.
///
/// # Architecture
///
/// The LSM-tree consists of:
/// - **Memtable**: In-memory buffer for recent writes
/// - **Immutable Memtables**: Memtables being flushed to disk
/// - **SSTables**: Sorted string tables on disk organized in levels
/// - **WAL**: Write-ahead log for crash recovery
/// - **Manifest**: Append-only log tracking SSTable organization
/// - **Compaction**: Background process merging SSTables
///
/// # Example
///
/// ```no_run
/// use lsm_tree::{lsm_storage::LSMStorage, memtable::BtreeMapMemtable, options::LSMStorageOptions};
/// use bytes::Bytes;
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let storage = LSMStorageOptions::default()
///         .open::<BtreeMapMemtable>("./data")
///         .await?;
///
///     // Insert a key-value pair
///     storage.insert(b"key1", Bytes::from("value1")).await?;
///
///     // Retrieve a value
///     let value = storage.get(&b"key1").await?;
///     assert_eq!(value, Some(Bytes::from("value1")));
///
///     // Delete a key
///     storage.delete(&b"key1").await?;
///
///     // Graceful shutdown
///     storage.close().await?;
///     Ok(())
/// }
/// ```
pub struct LSMStorage<M> {
    path: PathBuf,
    next_sst_id: Arc<AtomicUsize>,
    flush_lock: Mutex<()>,
    memtable: ArcSwap<M>,
    state_snapshot_receiver: watch::Receiver<StateSnapshot<M>>,
    state_update_sender: mpsc::Sender<StateUpdateEvent<M>>,
    state_update_handle: JoinHandle<()>,
    flush_sender: mpsc::Sender<Arc<M>>,
    compaction_cts: watch::Sender<()>,
    wal: Mutex<Wal>,
    options: LSMStorageOptions,
}

#[derive(Debug)]
pub(crate) enum StateUpdateEvent<M> {
    FlushComplete(usize, SSTable),
    FlushFail(Error),
    CompactionComplete(CompactionResult),
    CompactionFailed(Error),
    NewMemtable(Arc<M>, Option<usize>, oneshot::Sender<()>),
    Close(usize),
}

impl<M: Memtable + Send + Sync + 'static> LSMStorage<M> {
    /// Opens/Creates storage in path given.
    pub async fn open(path: impl AsRef<Path>, options: LSMStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        create_dir_all(path).context("Failed to create DB directory")?;

        let manifest_path = path.join("MANIFEST");
        let (mut manifest, records) = if manifest_path.exists() {
            Manifest::recover(manifest_path)?
        } else {
            (Manifest::new(manifest_path)?, Vec::new())
        };
        let (levels, sst_to_level, mut next_sst_id) = Self::recover_state_from_manifest(records)?;

        let sstables = Self::load_sstables(path, &levels)?;

        let (imm_memtables, memtables_to_flush) =
            Self::recover_memtables_from_wal(path, &sst_to_level, &mut next_sst_id)?;

        let (flush_tx, flush_rx) = mpsc::channel(FLUSH_CHANNEL_SIZE);
        let (state_update_sender, state_update_rx) = mpsc::channel(STATE_CHANNEL_SIZE);

        Self::start_flush_workers(
            path.to_path_buf(),
            options.num_flush_jobs,
            options.block_size,
            flush_rx,
            state_update_sender.clone(),
        );

        for mem_id in memtables_to_flush {
            if let Some(mem) = imm_memtables.get(&mem_id) {
                flush_tx.send(mem.clone()).await?;
            }
        }

        manifest.add_record(ManifestRecord::NewMemtable(next_sst_id))?;
        let memtable = M::new(next_sst_id);
        let wal_path = Wal::get_wal_path(path, next_sst_id);
        next_sst_id += 1;
        let wal = Wal::open(&wal_path, options.durable_wal)?;

        let snapshot = StateSnapshot {
            imm_memtables: imm_memtables.into(),
            levels: levels.into(),
            sstables: sstables.into(),
        };

        let (snapshot_tx, snapshot_rx) = watch::channel(snapshot);

        let context = Arc::new(StateWriterContext {
            manifest: manifest.into(),
            snapshot_sender: snapshot_tx,
        });

        let (compaction_tx, compaction_rx) = watch::channel(());
        let next_sst_id = Arc::new(AtomicUsize::new(next_sst_id));
        Self::start_compaction_worker(
            path.to_path_buf(),
            options.clone(),
            snapshot_rx.clone(),
            state_update_sender.clone(),
            compaction_rx,
            next_sst_id.clone(),
        );

        let state_update_handle =
            Self::init_state_update(path.to_path_buf(), context.clone(), state_update_rx);

        Ok(Self {
            path: path.to_path_buf(),
            next_sst_id,
            state_update_sender,
            flush_lock: Mutex::new(()),
            memtable: Arc::new(memtable).into(),
            state_snapshot_receiver: snapshot_rx,
            state_update_handle,
            flush_sender: flush_tx,
            compaction_cts: compaction_tx,
            options,
            wal: wal.into(),
        })
    }

    fn init_state_update(
        path: PathBuf,
        context: Arc<StateWriterContext<M>>,
        mut state_update_receiver: mpsc::Receiver<StateUpdateEvent<M>>,
    ) -> JoinHandle<()> {
        tokio::spawn({
            async move {
                while let Some(result) = state_update_receiver.recv().await {
                    match result {
                        StateUpdateEvent::FlushComplete(id, sst) => {
                            if let Err(e) = context
                                .manifest
                                .lock()
                                .await
                                .add_record(ManifestRecord::Flush(id))
                            {
                                error!(error = %e, memtable_id = id, "Failed to add flush record to manifest");
                                continue;
                            }

                            let mut snapshot = context.snapshot_sender.borrow().clone();

                            let sstables = Arc::make_mut(&mut snapshot.sstables);
                            let levels = Arc::make_mut(&mut snapshot.levels);
                            let imm_memtables = Arc::make_mut(&mut snapshot.imm_memtables);

                            sstables.insert(id, sst.into());
                            levels[0].push(id);
                            levels[0].sort_unstable();
                            imm_memtables.remove(&id);

                            let _ = context.snapshot_sender.send(snapshot);

                            let wal_path = Wal::get_wal_path(&path, id);
                            if let Err(e) = remove_file(&wal_path) {
                                error!(error = %e, wal_path = ?wal_path, "Failed to remove WAL file");
                            }
                        }
                        StateUpdateEvent::FlushFail(e) => {
                            error!(error = %e, "Flush operation failed");
                        }
                        StateUpdateEvent::CompactionComplete(CompactionResult {
                            task,
                            new_id,
                            new_sst,
                        }) => {
                            let source = task.source_level;
                            let target = source + 1;

                            let old_ids = [task.source_level_ssts, task.target_level_ssts].concat();
                            let manifest_record = ManifestRecord::Compaction {
                                level: target,
                                add_stts: vec![new_id],
                                remove_stts: old_ids.clone(),
                            };

                            if let Err(e) =
                                context.manifest.lock().await.add_record(manifest_record)
                            {
                                error!(error = %e, new_sst_id = new_id, target_level = target, "Failed to add compaction record to manifest");
                                continue;
                            }

                            let mut snapshot = context.snapshot_sender.borrow().clone();

                            let sstables = Arc::make_mut(&mut snapshot.sstables);
                            let levels = Arc::make_mut(&mut snapshot.levels);

                            for level in levels.iter_mut() {
                                level.retain(|id| !old_ids.contains(id));
                            }

                            for id in &old_ids {
                                sstables.remove(id);
                            }

                            while levels.len() <= target {
                                levels.push(Vec::new());
                            }

                            sstables.insert(new_id, new_sst.into());

                            levels[target].push(new_id);

                            levels[target].sort_unstable_by_key(|id| {
                                sstables.get(id).map_or(Bytes::new(), |sst| sst.first_key())
                            });

                            let _ = context.snapshot_sender.send(snapshot);

                            // Delete old SSTable files after state update
                            // SAFETY (Unix-specific):
                            for id in &old_ids {
                                let sst_path = SSTable::get_sst_path(&path, *id);
                                if let Err(e) = remove_file(&sst_path) {
                                    error!(error = %e, sst_id = id, "Failed to delete old SSTable file");
                                }
                            }
                        }
                        StateUpdateEvent::CompactionFailed(e) => {
                            error!(error = %e, "Compaction operation failed");
                        }
                        StateUpdateEvent::NewMemtable(old_memtable, new_id, ack) => {
                            if let Some(new_id) = new_id {
                                if let Err(e) = context
                                    .manifest
                                    .lock()
                                    .await
                                    .add_record(ManifestRecord::NewMemtable(new_id))
                                {
                                    error!(error = %e, memtable_id = new_id, "Failed to add NewMemtable record to manifest");
                                }
                            }

                            let mut snapshot = context.snapshot_sender.borrow().clone();

                            let imm_memtables = Arc::make_mut(&mut snapshot.imm_memtables);
                            imm_memtables.insert(old_memtable.get_id(), old_memtable);

                            let _ = context.snapshot_sender.send(snapshot);

                            let _ = ack.send(());
                        }
                        StateUpdateEvent::Close(id) => {
                            if let Err(e) = context
                                .manifest
                                .lock()
                                .await
                                .add_record(ManifestRecord::Flush(id))
                            {
                                error!(error = %e, memtable_id = id, "Failed to add close flush record to manifest");
                            }
                        }
                    }
                }
            }
        })
    }

    fn recover_state_from_manifest(
        records: Vec<ManifestRecord>,
    ) -> Result<(Levels, BTreeMap<usize, usize>, usize)> {
        let mut levels: Vec<Vec<usize>> = vec![Vec::new()];
        let mut sst_to_level = BTreeMap::<usize, usize>::new();
        let mut next_sst_id = 0_usize;

        for record in records {
            match record {
                ManifestRecord::Flush(id) => {
                    levels[0].push(id);
                    sst_to_level.insert(id, 0);
                    next_sst_id = next_sst_id.max(id);
                }
                ManifestRecord::NewMemtable(id) => {
                    next_sst_id = next_sst_id.max(id);
                }
                ManifestRecord::Compaction {
                    level,
                    add_stts,
                    remove_stts,
                } => {
                    for sst_id in &remove_stts {
                        if let Some(from_level) = sst_to_level.remove(sst_id) {
                            if let Some(from_level_entries) = levels.get_mut(from_level) {
                                let reomove_idx = from_level_entries
                                    .iter()
                                    .position(|e| e.eq(sst_id))
                                    .unwrap();
                                from_level_entries.remove(reomove_idx);
                            }
                        }
                    }

                    while levels.len() <= level {
                        levels.push(Vec::new());
                    }

                    for sst_id in &add_stts {
                        levels[level].push(*sst_id);
                        sst_to_level.insert(*sst_id, level);
                        next_sst_id = next_sst_id.max(*sst_id);
                    }
                }
            }
        }

        Ok((levels, sst_to_level, next_sst_id + 1))
    }

    fn load_sstables(path: &Path, levels: &Levels) -> Result<SSTableMap> {
        let mut sstables = SSTableMap::new();
        for level in levels {
            for id in level {
                let sstable_path = SSTable::get_sst_path(path, *id);
                let sstable = SSTable::open(&sstable_path)
                    .context(format!("Cannot open sst {sstable_path:?}"))?;
                sstables.insert(*id, sstable.into());
            }
        }
        Ok(sstables)
    }

    fn recover_memtables_from_wal(
        path: &Path,
        sst_to_level: &BTreeMap<usize, usize>,
        next_sst_id: &mut usize,
    ) -> Result<(ImmutableMemtableMap<M>, Vec<usize>)> {
        let mut imm_memtables_map = BTreeMap::<usize, Arc<M>>::new();
        let mut memtables_to_flush = Vec::new();
        let all_sst_ids: HashSet<usize> = sst_to_level.keys().cloned().collect();

        for entry in std::fs::read_dir(path)? {
            let entry_path = entry?.path();
            if entry_path.extension().is_some_and(|e| e == "wal") {
                let id_str = entry_path.file_stem().unwrap().to_str().unwrap();
                let id = id_str.parse::<usize>()?;

                if all_sst_ids.contains(&id) {
                    remove_file(entry_path)?;
                    continue;
                }

                let memtable = M::new(id);
                let records: Result<Vec<_>, _> = Wal::open(entry_path, false)?.iter().collect();
                let records = records?;
                for (key, value) in records {
                    memtable.set(key, value);
                }

                if memtable.size_estimate() > 0 {
                    let memtable_arc = Arc::new(memtable);
                    imm_memtables_map.insert(id, memtable_arc.clone());
                    memtables_to_flush.push(id);
                }
                *next_sst_id = (*next_sst_id).max(id + 1);
            }
        }

        Ok((imm_memtables_map, memtables_to_flush))
    }

    /// Gracefully shuts down the storage engine.
    ///
    /// This method:
    /// 1. Flushes the current memtable to disk if it contains data
    /// 2. Waits for all pending flush operations to complete
    /// 3. Stops the compaction worker
    /// 4. Waits for all background tasks to finish
    ///
    /// # Errors
    ///
    /// Returns an error if flushing fails or background tasks panic.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use lsm_tree::{lsm_storage::LSMStorage, memtable::BtreeMapMemtable, options::LSMStorageOptions};
    /// # #[tokio::main]
    /// # async fn main() -> anyhow::Result<()> {
    /// let storage = LSMStorageOptions::default()
    ///     .open::<BtreeMapMemtable>("./data")
    ///     .await?;
    ///
    /// // ... use storage ...
    ///
    /// storage.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn close(self) -> Result<()> {
        let memtable = self.memtable.into_inner();
        if memtable.size_estimate() > 0 {
            memtable_to_sst(self.options.block_size, &self.path, &*memtable).await?;
            self.state_update_sender
                .send(StateUpdateEvent::Close(memtable.get_id()))
                .await?;
        }

        drop(self.flush_sender);
        drop(self.state_update_sender);
        self.compaction_cts.send(())?;

        self.state_update_handle.await?;

        Ok(())
    }

    /// Inserts a key-value pair into the storage.
    ///
    /// The write is first recorded in the WAL for durability, then added to
    /// the in-memory memtable. If the memtable exceeds the configured size,
    /// it's asynchronously flushed to disk as an SSTable.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert (must be non-empty)
    /// * `value` - The value to associate with the key
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The key is empty
    /// - WAL append fails
    /// - Flush initiation fails
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use lsm_tree::{lsm_storage::LSMStorage, memtable::BtreeMapMemtable, options::LSMStorageOptions};
    /// # use bytes::Bytes;
    /// # #[tokio::main]
    /// # async fn main() -> anyhow::Result<()> {
    /// # let storage = LSMStorageOptions::default().open::<BtreeMapMemtable>("./data").await?;
    /// storage.insert(b"user:123", Bytes::from("Alice")).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn insert(&self, key: impl AsRef<[u8]>, value: Bytes) -> Result<()> {
        self.insert_inner(key, Record::Put(value)).await
    }

    /// Retrieves the value associated with a key.
    ///
    /// The search follows this order:
    /// 1. Current memtable (most recent writes)
    /// 2. Immutable memtables (being flushed)
    /// 3. L0 SSTables (newest to oldest)
    /// 4. L1+ SSTables (using key range filtering)
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up
    ///
    /// # Returns
    ///
    /// * `Ok(Some(value))` - Key found with associated value
    /// * `Ok(None)` - Key not found or was deleted
    /// * `Err(_)` - I/O or decoding error
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use lsm_tree::{lsm_storage::LSMStorage, memtable::BtreeMapMemtable, options::LSMStorageOptions};
    /// # use bytes::Bytes;
    /// # #[tokio::main]
    /// # async fn main() -> anyhow::Result<()> {
    /// # let storage = LSMStorageOptions::default().open::<BtreeMapMemtable>("./data").await?;
    /// if let Some(value) = storage.get(&b"user:123").await? {
    ///     println!("Found: {:?}", value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get(&self, key: &impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        let snapshot = self.state_snapshot_receiver.borrow().clone();
        let key = key.as_ref();
        let memtable = self.memtable.load();

        if let Some(rec) = memtable.get(key) {
            return Ok(rec.into_inner());
        }

        drop(memtable);

        for imm_memtable in snapshot.imm_memtables.values().rev() {
            if let Some(rec) = imm_memtable.get(key) {
                return Ok(rec.into_inner());
            }
        }

        if let Some(level0) = snapshot.levels.first() {
            for sst_id in level0.iter().rev() {
                if let Some(sst) = snapshot.sstables.get(sst_id) {
                    if let Some(rec) = sst.get(key).await? {
                        return Ok(rec.into_inner());
                    }
                }
            }
        }

        for level in snapshot.levels.iter().skip(1) {
            for sst_id in level {
                if let Some(sst) = snapshot.sstables.get(sst_id) {
                    if key >= sst.first_key().as_ref() && key <= sst.last_key().as_ref() {
                        if let Some(rec) = sst.get(key).await? {
                            return Ok(rec.into_inner());
                        }
                        break;
                    }
                }
            }
        }
        Ok(None)
    }

    /// Deletes a key from the storage.
    ///
    /// This writes a deletion tombstone to the WAL and memtable.
    /// The actual space won't be reclaimed until compaction removes
    /// the tombstone and any older versions of the key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to delete (must be non-empty)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The key is empty
    /// - WAL append fails
    /// - Flush initiation fails
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use lsm_tree::{lsm_storage::LSMStorage, memtable::BtreeMapMemtable, options::LSMStorageOptions};
    /// # #[tokio::main]
    /// # async fn main() -> anyhow::Result<()> {
    /// # let storage = LSMStorageOptions::default().open::<BtreeMapMemtable>("./data").await?;
    /// storage.delete(&b"user:123").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete(&self, key: &impl AsRef<[u8]>) -> Result<()> {
        self.insert_inner(key, TOMBSTONE).await
    }

    async fn insert_inner(&self, key: impl AsRef<[u8]>, value: Record) -> Result<()> {
        let key = Bytes::copy_from_slice(key.as_ref());
        if key.is_empty() {
            anyhow::bail!("Empty keys are not allowed");
        }

        {
            let mut wal = self.wal.lock().await;
            wal.append(key.clone(), &value)?;
        }

        let memtable = self.memtable.load();
        memtable.set(key, value.clone());

        if memtable.size_estimate() > self.options.memtables_size {
            let lock = self.flush_lock.try_lock();

            if lock.is_ok() {
                let current_memtable = self.memtable.load();
                if Arc::ptr_eq(&memtable, &current_memtable) {
                    self.flush().await.context("Failed to start flushing")?;
                }
            }
        }

        Ok(())
    }

    /// Forces the current memtable to be flushed to disk.
    ///
    /// This method waits for any ongoing flush to complete, then flushes
    /// the current memtable if it contains any data. Useful for:
    /// - Ensuring data is persisted before shutdown
    /// - Testing flush behavior
    /// - Forcing compaction by creating more L0 SSTables
    ///
    /// # Errors
    ///
    /// Returns an error if the flush operation fails.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use lsm_tree::{lsm_storage::LSMStorage, memtable::BtreeMapMemtable, options::LSMStorageOptions};
    /// # #[tokio::main]
    /// # async fn main() -> anyhow::Result<()> {
    /// # let storage = LSMStorageOptions::default().open::<BtreeMapMemtable>("./data").await?;
    /// storage.force_flush().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn force_flush(&self) -> Result<()> {
        // Wait for any ongoing flush to complete (blocking)
        let _lock = self.flush_lock.lock().await;

        // Check if current memtable has data
        let memtable = self.memtable.load();
        if memtable.size_estimate() > 0 {
            self.flush().await?;
        }

        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let new_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);

        let new_memtable = M::new(new_id);
        let new_wal_path = Wal::get_wal_path(&self.path, new_id);
        let new_wal = Wal::open(new_wal_path, self.options.durable_wal)?;

        let old_memtable = self.memtable.load_full();

        let (ack_tx, ack_rx) = oneshot::channel();
        self.state_update_sender
            .send(StateUpdateEvent::NewMemtable(
                old_memtable.clone(),
                Some(new_id),
                ack_tx,
            ))
            .await?;

        ack_rx
            .await
            .context("State update task failed to acknowledge memtable swap")?;

        {
            let mut current_wal = self.wal.lock().await;
            *current_wal = new_wal;
        };
        self.memtable.store(new_memtable.into());

        self.flush_sender
            .send(old_memtable)
            .await
            .context("Failed to send memtable into flush channel")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::memtable::BtreeMapMemtable;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_basic_crud() -> Result<()> {
        let dir = tempdir()?;
        let storage = LSMStorageOptions::default()
            .open::<BtreeMapMemtable>(&dir)
            .await?;

        let expected = Bytes::from("val1");
        let key = b"key";
        storage.insert(&key, expected.clone()).await?;
        let actual = storage.get(&key).await?;

        assert_eq!(actual, Some(expected));

        storage.delete(&key).await?;
        let actual = storage.get(&key).await?;

        assert_eq!(actual, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_persistence() -> Result<()> {
        let dir = tempdir()?;

        let storage = LSMStorageOptions::default()
            .open::<BtreeMapMemtable>(&dir)
            .await?;
        let key = b"key";
        let expected = Bytes::from("data");
        storage.insert(&key, expected.clone()).await?;
        storage.close().await?;

        let storage = LSMStorageOptions::default()
            .open::<BtreeMapMemtable>(&dir)
            .await?;
        let actual = storage.get(&key).await?;
        assert_eq!(actual, Some(expected));
        Ok(())
    }

    #[tokio::test]
    async fn test_flush_recovery() -> Result<()> {
        let dir = tempdir()?;
        let storage = LSMStorageOptions::default()
            .memtable_size(1)
            .open::<BtreeMapMemtable>(&dir)
            .await?;

        let key = b"key";
        let expected = Bytes::from("data");
        storage.insert(key.clone(), expected.clone()).await?;
        tokio::time::sleep(Duration::from_secs(1)).await; // Wait for flush
        storage.close().await?;
        let storage = LSMStorageOptions::default()
            .open::<BtreeMapMemtable>(&dir)
            .await?;
        let actual = storage.get(key).await?;
        assert_eq!(actual, Some(expected));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_compaction_is_triggered_automatically() -> Result<()> {
        let dir = tempdir().unwrap();
        let storage = LSMStorageOptions::default()
            .max_l0_ssts(2)
            .open::<BtreeMapMemtable>(&dir)
            .await?;

        storage.insert(b"a", Bytes::from("1")).await.unwrap();
        storage.insert(b"b", Bytes::from("1")).await.unwrap();
        storage.force_flush().await.unwrap();

        storage.insert(b"c", Bytes::from("2")).await.unwrap();
        storage.insert(b"d", Bytes::from("2")).await.unwrap();
        storage.force_flush().await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;

        let levels = &storage.state_snapshot_receiver.borrow().levels.clone();

        assert!(levels[0].is_empty());
        assert_eq!(levels.len(), 2);
        assert_eq!(levels[1].len(), 1);

        assert_eq!(storage.get(&b"a").await.unwrap(), Some(Bytes::from("1")));
        assert_eq!(storage.get(&b"b").await.unwrap(), Some(Bytes::from("1")));
        assert_eq!(storage.get(&b"c").await.unwrap(), Some(Bytes::from("2")));
        assert_eq!(storage.get(&b"d").await.unwrap(), Some(Bytes::from("2")));

        Ok(())
    }
}
