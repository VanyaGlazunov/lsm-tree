use anyhow::Result;
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc::Sender, watch},
    task::JoinHandle,
};

use crate::{
    lsm_storage::{LSMStorage, Levels, SSTableMap, StateSnapshot, StateUpdateEvent},
    memtable::Memtable,
    options::LSMStorageOptions,
    sstable::{builder::SSTableBuilder, merge_iterator::MergeIterator, SSTable, SSTableIterator},
};

const CHECK_COMPACTION_INTERVAL: u64 = 1000;

#[derive(Debug)]
pub struct CompactionResult {
    pub task: CompactionTask,
    pub new_id: usize,
    pub new_sst: SSTable,
}

#[derive(Debug)]
pub struct CompactionTask {
    pub source_level_ssts: Vec<usize>,
    pub source_level: usize,
    pub target_level_ssts: Vec<usize>,
}

pub struct LeveledCompactionStrategy {
    l0_compaction_trigger: usize,
}

impl LeveledCompactionStrategy {
    pub fn new(l0_max_size: usize) -> Self {
        Self {
            l0_compaction_trigger: l0_max_size,
        }
    }

    pub fn pick_compaction(
        &self,
        levels: &Levels,
        sstables: &SSTableMap,
    ) -> Option<CompactionTask> {
        if levels[0].len() >= self.l0_compaction_trigger {
            let source_ssts = levels[0].clone();

            let min_key = source_ssts
                .iter()
                .filter_map(|id| sstables.get(id))
                .map(|sst| sst.first_key())
                .min()
                .unwrap();

            let max_key = source_ssts
                .iter()
                .filter_map(|id| sstables.get(id))
                .map(|sst| sst.last_key())
                .max()
                .unwrap();

            let target_ssts = levels.get(1).map_or(Vec::new(), |l1| {
                l1.iter()
                    .filter(|id| {
                        let sst = sstables.get(id).unwrap();
                        sst.last_key() >= min_key && sst.first_key() <= max_key
                    })
                    .cloned()
                    .collect()
            });

            return Some(CompactionTask {
                source_level_ssts: source_ssts,
                source_level: 0,
                target_level_ssts: target_ssts,
            });
        }

        None
    }
}

impl<M: Memtable + Send + Sync + 'static> LSMStorage<M> {
    /// Starts a background worker that periodically checks for and runs compactions.
    pub(crate) fn start_compaction_worker(
        path: PathBuf,
        options: LSMStorageOptions,
        snapshot_receiver: watch::Receiver<StateSnapshot<M>>,
        event_sender: Sender<StateUpdateEvent<M>>,
        mut cts: watch::Receiver<()>,
        next_sst_id: Arc<AtomicUsize>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let strategy = LeveledCompactionStrategy::new(options.max_l0_ssts);
            let path = Arc::new(path);
            let mut interval =
                tokio::time::interval(Duration::from_millis(CHECK_COMPACTION_INTERVAL));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let (levels, sstables) = {
                            let snapshot = snapshot_receiver.borrow();
                            (
                                snapshot.levels.clone(),
                                snapshot.sstables.clone(),
                            )
                        };

                        if let Some(task) = strategy.pick_compaction(&levels, &sstables) {
                            let sender = event_sender.clone();
                            let path = path.clone();
                            let next_sst_id = next_sst_id.clone();

                            match Self::run_compaction(&path, options.block_size, task, next_sst_id, &sstables).await {
                                Ok(result) => {
                                    sender.send(StateUpdateEvent::CompactionComplete(result)).await.unwrap();
                                },
                                Err(e) => {
                                    sender.send(StateUpdateEvent::CompactionFailed(e)).await.unwrap();
                                },
                            }
                        }
                    }
                    _ = cts.changed() => {
                        break;
                    }
                }
            }
        })
    }

    /// Executes a compaction task.
    ///
    /// Merges the source and target SSTables into a single new SSTable.
    pub(crate) async fn run_compaction(
        path: &Path,
        block_size: usize,
        task: CompactionTask,
        next_sst_id: Arc<AtomicUsize>,
        sstables: &SSTableMap,
    ) -> Result<CompactionResult> {
        let mut iters = Vec::new();
        let mut ids = [
            task.source_level_ssts.clone(),
            task.target_level_ssts.clone(),
        ]
        .concat();
        ids.sort();
        for id in &ids {
            if let Some(sst) = sstables.get(id) {
                iters.push(SSTableIterator::new(sst.clone()));
            }
        }

        let merge_iter = MergeIterator::new(iters)?;

        let mut builder = SSTableBuilder::new(block_size);
        for (key, value) in merge_iter {
            builder.add(key, value);
        }

        let new_id = next_sst_id.fetch_add(1, Ordering::SeqCst);
        let new_sst = builder.build(SSTable::get_sst_path(path, new_id)).await?;

        Ok(CompactionResult {
            task,
            new_id,
            new_sst,
        })
    }
}
