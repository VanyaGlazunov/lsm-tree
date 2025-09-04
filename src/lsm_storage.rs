use std::{
    collections::{BTreeMap, HashSet},
    fs::{create_dir_all, remove_file},
    mem::replace,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use anyhow::{Context, Result};
use bytes::Bytes;
use tokio::{
    self,
    sync::{mpsc::{Receiver, Sender}, Mutex, RwLock, RwLockWriteGuard},
    task::JoinHandle,
};

use crate::{
    flush_system::{start_flush_workers, FlushResult},
    manifest::{Manifest, ManifestRecord},
    memtable::Memtable,
    options::LSMStorageOptions,
    sstable::{builder::SSTableBuilder, SSTable},
    wal::Wal,
};

const TOMBSTONE: Record = Record::Delete;

pub(crate) type ImmutableMemtableMap<M> = BTreeMap<usize, Arc<M>>;
pub(crate) type SSTableMap = BTreeMap<usize, SSTable>;
pub(crate) type Levels = Vec<Vec<usize>>;

/// Builds SSTable from memtable via SSTableBuilder.
pub(crate) async fn flush_memtable(
    block_size: usize,
    path: impl AsRef<Path>,
    memtable: &impl Memtable,
) -> Result<SSTable> {
    let mut builder = SSTableBuilder::new(block_size);
    for (key, value) in memtable.iter() {
        builder.add(key, value);
    }

    let id = memtable.get_id();
    builder
        .build(SSTable::get_sst_path(path, id))
        .context(format!("Failed to flush memtable. id: {id}"))
}

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

/// Consists of data that is needed during flush, compaction.
pub(crate) struct State<M: Memtable + Send + Sync + 'static> {
    pub path: PathBuf,                                  // Storage directory
    pub imm_memtables: RwLock<ImmutableMemtableMap<M>>, // Frozen memtables by IDs
    pub levels: RwLock<Levels>,                         // Compaction levels for ssts
    pub sstables: RwLock<SSTableMap>,                   // All SSTables by IDs
    pub next_sst_id: AtomicUsize,                       // SSTable ID counter
    pub manifest: Mutex<Manifest>,                      // Recovery log
    pub options: LSMStorageOptions,                     // Configuration
}

struct RecoveryState<M> {
    levels: Levels,
    sstables: SSTableMap,
    sst_to_level: BTreeMap<usize, usize>,
    imm_memtables: ImmutableMemtableMap<M>,
    next_sst_id: usize,
}

/// Main storage engine.
pub struct LSMStorage<M: Memtable + Send + Sync + 'static> {
    swap_lock: Mutex<()>, // Memtable swap synchronization
    memtable: RwLock<M>,  // Active mutable memtable
    state: Arc<State<M>>,
    state_update_handle: JoinHandle<()>,
    flush_sender: Sender<Arc<M>>,
    wal: Mutex<Wal>,
}

impl<M: Memtable + Clone + Send + Sync + std::fmt::Debug> LSMStorage<M> {
    /// Opens/Creates storage in path given.
    pub async fn open(path: impl AsRef<Path>, options: LSMStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        create_dir_all(path).context("Failed to create DB directory")?;

        let manifest_path = path.join("MANIFEST");
        let (manifest, records) = if manifest_path.exists() {
            Manifest::recover(manifest_path)?
        } else {
            (Manifest::new(manifest_path)?, Vec::new())
        };
        let mut recovery_state = Self::recover_state_from_manifest(records)?;

        Self::load_sstables(path, &mut recovery_state)?;

        let memtables_to_flush = Self::recover_memtables_from_wal(path, &mut recovery_state)?;

        let (cmd_tx, res_rx) = start_flush_workers::<M>(
            path.to_path_buf(),
            options.num_flush_jobs,
            options.block_size,
        );
        for mem_id in memtables_to_flush {
            if let Some(mem) = recovery_state.imm_memtables.get(&mem_id) {
                cmd_tx.send(mem.clone()).await?;
            }
        }

        let next_sst_id = recovery_state.next_sst_id;
        manifest.add_record(ManifestRecord::NewMemtable(next_sst_id))?;

        let memtable = RwLock::new(M::new(next_sst_id));
        let wal_path = Wal::get_wal_path(path, next_sst_id);
        let wal = Mutex::new(Wal::open(&wal_path, options.durable_wal)?);

        let state = Arc::new(State::<M> {
            path: path.to_path_buf(),
            imm_memtables: RwLock::new(recovery_state.imm_memtables),
            levels: RwLock::new(recovery_state.levels),
            sstables: RwLock::new(recovery_state.sstables),
            next_sst_id: AtomicUsize::new(next_sst_id),
            manifest: Mutex::new(manifest),
            options,
        });

        let state_update_handle = Self::init_state_update(state.clone(), res_rx);

        Ok(Self {
            swap_lock: Mutex::new(()),
            memtable,
            state,
            state_update_handle,
            flush_sender: cmd_tx,
            wal,
        })
    }

    fn init_state_update(state: Arc<State<M>>, mut flush_receiver: Receiver<FlushResult>) -> JoinHandle<()> {
        tokio::spawn({
            async move {
                while let Some(result) = flush_receiver.recv().await {
                    let id = result.id;
                    let sst = result.sstable;
                    state
                        .manifest
                        .lock()
                        .await
                        .add_record(ManifestRecord::Flush(id))
                        .unwrap();

                    state.sstables.write().await.insert(id, sst);

                    state.levels.write().await[0].push(id);

                    state.imm_memtables.write().await.remove(&id);

                    let wal_path = Wal::get_wal_path(&state.path, id);
                    if let Err(e) = remove_file(&wal_path) {
                        eprintln!("Failed to remove WAL file {wal_path:?}: {e}");
                    }
                }
            }
        })
    }

    fn recover_state_from_manifest(records: Vec<ManifestRecord>) -> Result<RecoveryState<M>> {
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
                                from_level_entries.remove(*sst_id);
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

        Ok(RecoveryState {
            levels,
            sstables: BTreeMap::new(),
            imm_memtables: ImmutableMemtableMap::new(),
            sst_to_level,
            next_sst_id: next_sst_id + 1,
        })
    }

    fn load_sstables(path: &Path, recovery_state: &mut RecoveryState<M>) -> Result<()> {
        for level in &recovery_state.levels {
            for id in level {
                let sstable_path = SSTable::get_sst_path(path, *id);
                let sstable = SSTable::open(&sstable_path)
                    .context(format!("Cannot open sst {sstable_path:?}"))?;
                recovery_state.sstables.insert(*id, sstable);
            }
        }
        Ok(())
    }

    fn recover_memtables_from_wal(
        path: &Path,
        recovery_state: &mut RecoveryState<M>,
    ) -> Result<Vec<usize>> {
        let mut imm_memtables_map = BTreeMap::<usize, Arc<M>>::new();
        let mut memtables_to_flush = Vec::new();
        let all_sst_ids: HashSet<usize> = recovery_state.sst_to_level.keys().cloned().collect();

        for entry in std::fs::read_dir(path)? {
            let entry_path = entry?.path();
            if entry_path.extension().is_some_and(|e| e == "wal") {
                let id_str = entry_path.file_stem().unwrap().to_str().unwrap();
                let id = id_str.parse::<usize>()?;

                if all_sst_ids.contains(&id) {
                    remove_file(entry_path)?;
                    continue;
                }

                let mut memtable = M::new(id);
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
                recovery_state.next_sst_id = recovery_state.next_sst_id.max(id + 1);
            }
        }

        recovery_state.imm_memtables = imm_memtables_map;

        Ok(memtables_to_flush)
    }

    /// Graceful shutdown (waits until all data will be flushed).
    pub async fn close(self) -> Result<()> {
        let memtable = self.memtable.into_inner();
        if memtable.size_estimate() > 0 {
            let id = memtable.get_id();
            flush_memtable(self.state.options.block_size, &self.state.path, &memtable).await?;
            self.state
                .manifest
                .lock()
                .await
                .add_record(ManifestRecord::Flush(id))?;
        }

        drop(self.flush_sender);

        self.state_update_handle.await?;

        Ok(())
    }

    /// Inserts key-value pair.
    ///
    /// # Errors
    /// Returns error if key is empty.
    pub async fn insert(&self, key: impl AsRef<[u8]>, value: Bytes) -> Result<()> {
        self.insert_inner(key, Record::Put(value)).await
    }

    /// Retrieves value for key.
    pub async fn get(&self, key: &impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        let memtable = self.memtable.read().await;
        let key = key.as_ref();

        if let Some(rec) = memtable.get(key) {
            return Ok(rec.into_inner());
        }

        drop(memtable);

        let imm_memtables = self.state.imm_memtables.read().await;
        for imm_memtable in imm_memtables.values().rev() {
            if let Some(rec) = imm_memtable.get(key) {
                return Ok(rec.into_inner());
            }
        }

        drop(imm_memtables);

        let levels = self.state.levels.read().await;
        let sstables = self.state.sstables.read().await;

        if let Some(level0) = levels.first() {
            for sst_id in level0.iter().rev() {
                if let Some(sst) = sstables.get(sst_id) {
                    if let Some(rec) = sst.get(key)? {
                        return Ok(rec.into_inner());
                    }
                }
            }
        }

        for level in levels.iter().skip(1) {
            for sst_id in level {
                if let Some(sst) = sstables.get(sst_id) {
                    if key >= sst.first_key().as_ref() && key <= sst.last_key().as_ref() {
                        if let Some(rec) = sst.get(key)? {
                            return Ok(rec.into_inner());
                        } else {
                            return Ok(None);
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    /// Deletes key.
    ///
    /// # Errors
    /// Returns error if key is empty.
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

        let mut memtable = self.memtable.write().await;
        memtable.set(key, value.clone());

        if memtable.size_estimate() > self.state.options.memtables_size {
            self.flush(memtable)
                .await
                .context("Failed to start flushing")?;
        }

        Ok(())
    }

    /// Forces flush of current memtable.
    pub async fn force_flush(&self) -> Result<()> {
        let memtable = self.memtable.write().await;
        self.flush(memtable).await
    }

    async fn flush(&self, mut memtable: RwLockWriteGuard<'_, M>) -> Result<()> {
        let old_id = self.state.next_sst_id.fetch_add(1, Ordering::SeqCst);

        let new_id = old_id + 1;
        let new_memtable = M::new(new_id);
        let new_wal_path = Wal::get_wal_path(&self.state.path, new_id);
        let new_wal = Wal::open(new_wal_path, self.state.options.durable_wal)?;

        let old_memtable;
        {
            let _lock = self.swap_lock.lock().await;
            old_memtable = replace(&mut *memtable, new_memtable);
            let mut current_wal = self.wal.lock().await;
            *current_wal = new_wal;
        };
        let old_memtable = Arc::new(old_memtable);

        self.state
            .manifest
            .lock()
            .await
            .add_record(ManifestRecord::NewMemtable(old_id))
            .context("Failed to add NewMemtable record to manifest")?;

        self.state
            .imm_memtables
            .write()
            .await
            .insert(old_id, old_memtable.clone());

        self.flush_sender
            .send(old_memtable)
            .await
            .context("Failed to send memtable into flush channel")?;

        drop(memtable);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::memtable::BtreeMapMemtable;
    use tempfile::tempdir;

    type Storage = LSMStorage<BtreeMapMemtable>;

    #[tokio::test]
    async fn test_basic_crud() -> Result<()> {
        let dir = tempdir()?;
        let storage = Storage::open(&dir, LSMStorageOptions::default()).await?;

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

        let storage = Storage::open(&dir, LSMStorageOptions::default()).await?;
        let key = b"key";
        let expected = Bytes::from("data");
        storage.insert(&key, expected.clone()).await?;
        storage.close().await?;

        let storage = Storage::open(&dir, LSMStorageOptions::default()).await?;
        let actual = storage.get(&key).await?;
        assert_eq!(actual, Some(expected));
        Ok(())
    }

    #[tokio::test]
    async fn test_flush_recovery() -> Result<()> {
        let dir = tempdir()?;
        let storage = Storage::open(
            &dir,
            LSMStorageOptions {
                memtables_size: 1,
                ..Default::default()
            },
        )
        .await?;

        let key = b"key";
        let expected = Bytes::from("data");
        storage.insert(key.clone(), expected.clone()).await?;
        tokio::time::sleep(Duration::from_secs(1)).await; // Wait for flush
        storage.close().await?;
        let storage = Storage::open(&dir, LSMStorageOptions::default()).await?;
        let actual = storage.get(key).await?;
        assert_eq!(actual, Some(expected));
        Ok(())
    }
}
