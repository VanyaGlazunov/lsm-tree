use std::{
    collections::{BTreeSet, HashMap}, fs::create_dir_all, mem, path::{Path, PathBuf}, sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    }, thread::sleep, time::Duration
};

use anyhow::{Context, Result};
use bytes::Bytes;
use tokio::{
    self,
    sync::{Mutex, Notify, RwLock, Semaphore},
    task::JoinHandle,
};

use crate::{
    manifest::{Manifest, ManifestRecord},
    memtable::Memtable,
    sstable::{builder::SSTableBuilder, SSTable},
};

// TODO: Fix tombstone logic. i.e user shouldn't be able to replicate tombstone.
const TOMBSTONE: Bytes = Bytes::from_static(b"__TOMBSTONE__");
const FLUSH_CHANNEL_SIZE: usize = 100;
const DEFAULT_BLOCK_SIZE: usize = 1 << 12;
const DEFAULT_MEMTABLE_SIZE: usize = 1 << 23;
const DEFAULT_MEMTABLE_CAP: usize = 2;
const DEFAULT_NUM_FLUSH_WORKERS: usize = 2;

pub struct LSMStorageOptions {
    pub block_size: usize,
    pub memtables_size: usize,
    // Number of immutable memtables stored in memory.
    #[allow(unused)]
    pub memtables_cap: usize,
    pub num_flush_jobs: usize,
}

struct LSMData<M: Memtable> {
    memtable: M,
    imm_memtables: HashMap<usize, M>,
    l0_tables: Vec<usize>,
    sstables: HashMap<usize, SSTable>,
}

struct LSMStorageInner<M: Memtable + Send + Sync + 'static> {
    path: PathBuf,
    data: Arc<RwLock<LSMData<M>>>,
    options: Arc<LSMStorageOptions>,
    next_sst_id: AtomicUsize,
    manifest: Arc<Mutex<Manifest>>,
    active_flush_tasks: Arc<AtomicUsize>,
    flush_sender: tokio::sync::mpsc::Sender<M>,
    flush_handle: JoinHandle<()>,
    flush_notifier: Arc<Notify>,
    flush_complete_notify: Arc<Notify>
}

pub struct LSMStorage<M: Memtable + Send + Sync + 'static> {
    inner: LSMStorageInner<M>,
}

impl Default for LSMStorageOptions {
    fn default() -> Self {
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
            memtables_size: DEFAULT_MEMTABLE_SIZE,
            memtables_cap: DEFAULT_MEMTABLE_CAP,
            num_flush_jobs: DEFAULT_NUM_FLUSH_WORKERS,
        }
    }
}


// Needs refactoring...
async fn flush_worker<M: Memtable + Send + Sync + 'static>(
    data: Arc<RwLock<LSMData<M>>>,
    block_size: usize,
    manifest: Arc<Mutex<Manifest>>,
    mut receiver: tokio::sync::mpsc::Receiver<M>,
    path: Arc<PathBuf>,
    active_flush_tasks: Arc<AtomicUsize>,
    flush_complete_notify: Arc<Notify>,
    notify: Arc<Notify>,
    num_flush_jobs: usize,
) {
    let semaphore = Box::new(Semaphore::new(num_flush_jobs));
    let semaphore = Box::leak(semaphore);
    dbg!("Start WORKER");
    loop {
        tokio::select! {
            Some(memtable) = receiver.recv() => {
                let permit = semaphore.acquire().await;
                active_flush_tasks.fetch_add(1, Ordering::SeqCst);
                let data_clone = Arc::clone(&data);
                let path_clone = Arc::clone(&path);
                let manifest_clone = Arc::clone(&manifest);
                let active_flush_tasks_clone = Arc::clone(&active_flush_tasks);
                let flush_complete_notify_clone = Arc::clone(&flush_complete_notify);

                tokio::spawn(async move {
                    let id = memtable.get_id();
                    let sst_path = get_sst_path(path_clone.as_path(), id);

                    let sst = {
                        let mut builder = SSTableBuilder::new(block_size);
                        memtable.flush(&mut builder);
                        builder.build(sst_path).unwrap()
                    };

                    {
                        let manifest = manifest_clone.lock().await;
                        manifest.add_record(ManifestRecord::Flush(id)).unwrap();
                    }

                    {
                        let mut guard = data_clone.write().await;
                        guard.l0_tables.push(id);
                        guard.sstables.insert(id, sst);
                        guard.imm_memtables.remove(&id);
                    }

                    active_flush_tasks_clone.fetch_sub(1, Ordering::SeqCst);
                    flush_complete_notify_clone.notify_one();
                    drop(permit);
                });
            }
            _ = notify.notified() => {
                break;
            }
        }
    }
    // panic!();
}



fn get_sst_path(path: impl AsRef<Path>, id: usize) -> PathBuf {
    path.as_ref().to_path_buf().join(format!("{id}.sst"))
}

impl<M: Memtable + Clone + Send + Sync> LSMStorageInner<M> {
    fn open(path: impl AsRef<Path>, options: LSMStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let mut l0_tables = Vec::<usize>::new();
        let mut sstables = HashMap::<usize, SSTable>::new();

        let mut next_sst_id = 0 as usize;

        if !path.exists() {
            create_dir_all(path).context("Failed to create DB directory")?;
        }

        // Manifest path = path/MANIFEST
        let manifest_path = path.join("MANIFEST");
        let manifest;
        // recover sstables from manifest...
        if manifest_path.exists() {
            let records: Vec<ManifestRecord>;
            (manifest, records) = Manifest::recover(manifest_path)?;
            let mut memtables = BTreeSet::<usize>::new();
            dbg!(&records);
            for record in records {
                match record {
                    ManifestRecord::NewMemtable(id) => {
                        memtables.insert(id);
                    }
                    ManifestRecord::Flush(id) => {
                        let removed = memtables.remove(&id);
                        if !removed {
                            anyhow::bail!("Wrong id for flushed memtable {id}. Can not recover.")
                        }

                        l0_tables.push(id);
                        next_sst_id = next_sst_id.max(id);
                    }
                }
            }
            next_sst_id = next_sst_id + 1;
            for id in &l0_tables {
                let sstable_path = get_sst_path(path, *id);
                let sstable = SSTable::open(&sstable_path)
                    .context(format!("Cannot open sst {sstable_path:?}"))?;
                sstables.insert(*id, sstable);
            }
        } else {
            manifest = Manifest::new(manifest_path)?;
            manifest.add_record(ManifestRecord::NewMemtable(0))?;
        }

        // TODO: recover memtables from WALs..

        let data = Arc::new(RwLock::new(LSMData {
            memtable: M::new(next_sst_id),
            imm_memtables: HashMap::new(),
            l0_tables,
            sstables,
        }));
        let manifest = Arc::new(Mutex::new(manifest));
        let (active_flush_tasks, flush_notifier, flush_complete_notifier, handle, sender) = LSMStorageInner::spawn_flush_worker(
            data.clone(),
            &options,
            manifest.clone(),
            path.to_path_buf(),
        );

        Ok(LSMStorageInner {
            data: data,
            path: path.to_path_buf(),
            next_sst_id: AtomicUsize::new(next_sst_id),
            options: Arc::new(options),
            manifest: manifest,
            active_flush_tasks,
            flush_sender: sender,
            flush_handle: handle,
            flush_notifier: flush_notifier,
            flush_complete_notify: flush_complete_notifier,
        })
    }

    fn spawn_flush_worker(
        data: Arc<RwLock<LSMData<M>>>,
        options: &LSMStorageOptions,
        manifest: Arc<Mutex<Manifest>>,
        path: PathBuf,
    ) -> (Arc<AtomicUsize>, Arc<Notify>, Arc<Notify>, JoinHandle<()>, tokio::sync::mpsc::Sender<M>) {
        let (tx, rx) = tokio::sync::mpsc::channel(FLUSH_CHANNEL_SIZE);
        let path = Arc::new(path);
        let active_flush_tasks = Arc::new(AtomicUsize::new(0));
        let flush_complete_notify = Arc::new(Notify::new());
        let flush_notifier = Arc::new(Notify::new());

        let handle = tokio::spawn(flush_worker(
            data,
            options.block_size,
            manifest,
            rx,
            path,
            active_flush_tasks.clone(),
            flush_complete_notify.clone(),
            flush_notifier.clone(),
            options.num_flush_jobs,
        ));

        (active_flush_tasks, flush_notifier, flush_complete_notify, handle, tx)
    }

    /// Waits for all flushes to be done then closes db.
    async fn close(self) -> Result<()> {
        while self.active_flush_tasks.load(Ordering::SeqCst) > 0 {
            self.flush_complete_notify.notified().await;
        }
        self.flush_notifier.notify_one();
        self.flush_handle.await.unwrap();
        let mut guard = self.data.write().await;

        let id = guard.memtable.get_id();
        let memtable = std::mem::replace(&mut guard.memtable, M::new(0));
        guard.imm_memtables.insert(id, memtable);

        for (_, memtable) in &guard.imm_memtables {
            if memtable.size_estimate() == 0 {
                continue;
            }
            let id = memtable.get_id();
            let sst_path = self.path.join(format!("{id}.sst"));

            let mut builder = SSTableBuilder::new(self.options.block_size);
            memtable.flush(&mut builder);
            builder.build(sst_path).unwrap();

            let manifest = self.manifest.lock().await;
            manifest.add_record(ManifestRecord::Flush(id))?;
            drop(manifest);
        }

        Ok(())
    }

    async fn insert(&mut self, key: impl AsRef<[u8]>, value: Bytes) {
        let key = key.as_ref();
        // TODO: Propper handling of channel overflow.
        if key.is_empty() {
            panic!("Empty keys are not allowed")
        }
        let mut guard = self.data.write().await;

        guard
            .memtable
            .set(Bytes::copy_from_slice(key), value);

        if guard.memtable.size_estimate() > self.options.memtables_size {
            let old_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
            let new_memtable = M::new(old_id + 1);
            let old_memtable = mem::replace(&mut guard.memtable, new_memtable);
            guard
                .imm_memtables
                .insert(old_id, old_memtable.clone());

            {
                // TODO: Propper handling of channel overflow.
                self.manifest.lock().await.add_record(ManifestRecord::NewMemtable(old_id)).unwrap();
            }

            // TODO: Propper handling of channel overflow.
            loop {
                let res = self.flush_sender.try_send(old_memtable.clone());
                match res {
                    Ok(()) => break,
                    Err(_) => {
                        sleep(Duration::from_secs(2));
                    }
                }
            }
        }
    }

    async fn get(&self, key: &impl AsRef<[u8]>) -> Option<Bytes> {
        let guard = self.data.read().await;
        let key = key.as_ref();

        let mut candidate = guard.memtable.get(key);
        // dbg!(&guard.sstables);
        if candidate.is_some()  {
            if candidate == Some(TOMBSTONE) {
                return None;
            } else {
                return candidate;
            }
        }

        for (_, memtable) in &guard.imm_memtables {
            match memtable.get(key) {
                None => continue,
                Some(val) => candidate = Some(val)
            }
        }

        if candidate.is_some() {
            if candidate == Some(TOMBSTONE) {
                return None;
            }
            return candidate;
        }

        for sst_id in &guard.l0_tables {
            // TODO: Propper error handling
            let sst = guard.sstables.get(sst_id).unwrap();
            let try_get_value = sst.get(key);
            // TODO: Propper error handling
            match try_get_value {
                Ok(maybe_value) => {
                    if maybe_value.is_some()  {
                        candidate = maybe_value;
                    }
                }
                Err(_) => continue,
            }
        }

        if candidate.is_some() {
            if candidate == Some(TOMBSTONE) {
                return None;
            }
            return candidate;
        }

        return None;
    }
}

impl<M: Memtable + Clone + Send + Sync> LSMStorage<M> {
    /// Opens lsm storage from path or creates a new one.
    pub fn open(path: &impl AsRef<Path>, options: LSMStorageOptions) -> Result<Self> {
        Ok(Self {
            inner: LSMStorageInner::open(path, options)?,
        })
    }

    /// Hopefully closes db...
    pub async fn close(self) -> Result<()> {
        self.inner.close().await
    }

    pub async fn insert(&mut self, key: impl AsRef<[u8]>, value: Bytes) {
        self.inner.insert(key, value).await;
    }

    pub async fn get(&self, key: &impl AsRef<[u8]>) -> Option<Bytes> {
        self.inner.get(key).await
    }

    pub async fn delete(&mut self, key: &impl AsRef<[u8]>) {
        self.inner.insert(key, TOMBSTONE).await;
    }
}

#[cfg(test)]
mod lsm_storage_tests {
    use std::time::Duration;

    use super::*;
    use crate::memtable::BtreeMapMemtable;
    use tempfile::tempdir;

    type Storage = LSMStorage<BtreeMapMemtable>;

    #[tokio::test]
    async fn test_basic_crud() -> Result<()> {
        let dir = tempdir()?;
        let mut storage = Storage::open(&dir, LSMStorageOptions::default())?;
        
        storage.insert(b"key1", Bytes::from("val1")).await;
        assert_eq!(storage.get(&b"key1").await, Some(Bytes::from("val1")));
        
        storage.delete(&b"key1").await;
        assert_eq!(storage.get(&b"key1").await, None);
        Ok(())
    }

    #[tokio::test]
    async fn test_persistence() -> Result<()> {
        let dir = tempdir()?;
        {
            let mut storage = Storage::open(&dir, LSMStorageOptions::default())?;
            storage.insert(b"persisted", Bytes::from("data")).await;
            storage.close().await?;
        }

        let storage = Storage::open(&dir, LSMStorageOptions::default())?;
        assert_eq!(storage.get(&b"persisted").await, Some(Bytes::from("data")));
        Ok(())
    }

    #[tokio::test]
    async fn test_flush_recovery() -> Result<()> {
        let dir = tempdir()?;
        let mut storage = Storage::open(&dir, LSMStorageOptions {
            memtables_size: 1,
            ..Default::default()
        })?;
        
        storage.insert(b"flushed", Bytes::from("value")).await;
        tokio::time::sleep(Duration::from_secs(1)).await; // Wait for flush
        storage.close().await.unwrap();
        
        let storage = Storage::open(&dir, LSMStorageOptions::default())?;
        assert_eq!(storage.get(&b"flushed").await, Some(Bytes::from("value")));
        Ok(())
    }
}
