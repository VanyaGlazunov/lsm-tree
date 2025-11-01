use anyhow::Result;
use bytes::Bytes;
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
use tracing::{error, warn};

use crate::{
    lsm_storage::{LSMStorage, Levels, Record, SSTableMap, StateSnapshot, StateUpdateEvent},
    memtable::ThreadSafeMemtable,
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
    base_level_size_bytes: usize,
    level_size_multiplier: usize,
}

impl LeveledCompactionStrategy {
    pub fn new(options: &LSMStorageOptions) -> Self {
        Self {
            l0_compaction_trigger: options.max_l0_ssts,
            base_level_size_bytes: options.base_level_size_bytes,
            level_size_multiplier: options.level_size_multiplier,
        }
    }

    /// Calculates target size for a given level in bytes.
    fn level_target_size(&self, level: usize) -> usize {
        if level == 0 {
            return usize::MAX; // L0 uses count-based trigger
        }
        self.base_level_size_bytes * self.level_size_multiplier.pow((level - 1) as u32)
    }

    /// Calculates total size of SSTables in a level.
    fn level_size(&self, level_ssts: &[usize], sstables: &SSTableMap) -> usize {
        level_ssts
            .iter()
            .filter_map(|id| sstables.get(id))
            .map(|sst| sst.size_bytes())
            .sum()
    }

    pub fn pick_compaction(
        &self,
        levels: &Levels,
        sstables: &SSTableMap,
    ) -> Option<CompactionTask> {
        if levels[0].len() >= self.l0_compaction_trigger {
            return self.pick_l0_compaction(levels, sstables);
        }

        for level in 1..levels.len() {
            let current_size = self.level_size(&levels[level], sstables);
            let target_size = self.level_target_size(level);

            if current_size > target_size {
                return self.pick_level_compaction(level, levels, sstables);
            }
        }

        None
    }

    /// Picks L0 → L1 compaction (all L0 files + overlapping L1 files).
    fn pick_l0_compaction(&self, levels: &Levels, sstables: &SSTableMap) -> Option<CompactionTask> {
        let source_ssts = levels[0].clone();

        let Some(min_key) = source_ssts
            .iter()
            .filter_map(|id| sstables.get(id))
            .map(|sst| sst.first_key())
            .min()
        else {
            warn!("No valid SSTables found for L0 compaction (min_key check)");
            return None;
        };

        let Some(max_key) = source_ssts
            .iter()
            .filter_map(|id| sstables.get(id))
            .map(|sst| sst.last_key())
            .max()
        else {
            warn!("No valid SSTables found for L0 compaction (max_key check)");
            return None;
        };

        let target_ssts = levels.get(1).map_or(Vec::new(), |l1| {
            l1.iter()
                .filter_map(|id| {
                    let sst = sstables.get(id)?;
                    if sst.last_key() >= min_key && sst.first_key() <= max_key {
                        Some(*id)
                    } else {
                        None
                    }
                })
                .collect()
        });

        Some(CompactionTask {
            source_level_ssts: source_ssts,
            source_level: 0,
            target_level_ssts: target_ssts,
        })
    }

    /// Picks Ln → Ln+1 compaction (one source file + overlapping target files).
    fn pick_level_compaction(
        &self,
        source_level: usize,
        levels: &Levels,
        sstables: &SSTableMap,
    ) -> Option<CompactionTask> {
        let source_id = *levels[source_level].first()?;
        let source_sst = sstables.get(&source_id)?;

        let min_key = source_sst.first_key();
        let max_key = source_sst.last_key();

        let target_level = source_level + 1;
        let target_ssts = levels.get(target_level).map_or(Vec::new(), |level| {
            level
                .iter()
                .filter_map(|id| {
                    let sst = sstables.get(id)?;
                    if sst.last_key() >= min_key && sst.first_key() <= max_key {
                        Some(*id)
                    } else {
                        None
                    }
                })
                .collect()
        });

        Some(CompactionTask {
            source_level_ssts: vec![source_id],
            source_level,
            target_level_ssts: target_ssts,
        })
    }
}

impl<M: ThreadSafeMemtable> LSMStorage<M> {
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
            let strategy = LeveledCompactionStrategy::new(&options);
            let path = Arc::new(path);
            let mut interval =
                tokio::time::interval(Duration::from_millis(CHECK_COMPACTION_INTERVAL));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let task_and_sstables = {
                            let snapshot = snapshot_receiver.borrow();
                            strategy.pick_compaction(&snapshot.levels, &snapshot.sstables)
                                .map(|task| (task, snapshot.sstables.clone()))
                        };

                        if let Some((task, sstables)) = task_and_sstables {
                            let sender = event_sender.clone();
                            let path = path.clone();
                            let next_sst_id = next_sst_id.clone();

                            let levels = snapshot_receiver.borrow().levels.clone();

                            match Self::run_compaction(&path, options.block_size, task, next_sst_id, &sstables, &levels).await {
                                Ok(result) => {
                                    if let Err(e) = sender.send(StateUpdateEvent::CompactionComplete(result)).await {
                                        warn!(error = %e, "Failed to send compaction result (likely shutdown)");
                                    }
                                },
                                Err(e) => {
                                    error!(error = %e, "Compaction operation failed");
                                    if let Err(send_err) = sender.send(StateUpdateEvent::CompactionFailed(e)).await {
                                        warn!(error = %send_err, "Failed to send compaction failure (likely shutdown)");
                                    }
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
    /// Filters out tombstones (deletes) only when compacting to the last existing level.
    pub(crate) async fn run_compaction(
        path: &Path,
        block_size: usize,
        task: CompactionTask,
        next_sst_id: Arc<AtomicUsize>,
        sstables: &SSTableMap,
        levels: &Levels,
    ) -> Result<CompactionResult> {
        let mut iters: Vec<Box<dyn Iterator<Item = (Bytes, Record)> + Send>> = Vec::new();
        let mut ids = [
            task.source_level_ssts.clone(),
            task.target_level_ssts.clone(),
        ]
        .concat();
        ids.sort_unstable_by(|a, b| b.cmp(a));
        for id in &ids {
            if let Some(sst) = sstables.get(id) {
                iters.push(Box::new(SSTableIterator::new(sst.clone())));
            }
        }

        let merge_iter = MergeIterator::new(iters);

        let target_level = task.source_level + 1;
        let is_last_level = target_level >= levels.len() - 1;

        let mut builder = SSTableBuilder::new(block_size);
        for (key, value) in merge_iter {
            if is_last_level && matches!(value, crate::lsm_storage::Record::Delete) {
                continue;
            }
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
