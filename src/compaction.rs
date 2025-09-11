use anyhow::Result;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::{
    sync::{mpsc::Sender, watch},
    task::JoinHandle,
    time,
};

use crate::{
    lsm_storage::{LSMStorage, Levels, SSTableMap, State, StateUpdateEvent},
    memtable::Memtable,
    sstable::{builder::SSTableBuilder, merge_iterator::MergeIterator, SSTable, SSTableIterator},
};

const CHECK_COMPACTION_INTERVAL: u64 = 1000;

#[derive(Debug)]
pub struct CompactionResult {
    pub task: CompactionTask,
    pub new_id: usize,
    pub new_sst: SSTable,
}

pub async fn run_compaction<M: Memtable + Send + Sync>(
    task: CompactionTask,
    state: Arc<State<M>>,
) -> Result<CompactionResult> {
    let mut iters = Vec::new();
    let ssts = state.sstables.read().await;
    for id in task
        .source_level_ssts
        .iter()
        .chain(task.target_level_ssts.iter())
    {
        if let Some(sst) = ssts.get(id) {
            iters.push(SSTableIterator::new(Arc::new(sst.try_clone()?)));
        }
    }
    drop(ssts);

    let merge_iter = MergeIterator::new(iters)?;

    let mut builder = SSTableBuilder::new(state.options.block_size);
    for (key, value) in merge_iter {
        builder.add(key, value);
    }

    let new_id = state.next_sst_id.fetch_add(1, Ordering::SeqCst);
    let new_sst = builder
        .build(SSTable::get_sst_path(&state.path, new_id))
        .await?;

    Ok(CompactionResult {
        task,
        new_id,
        new_sst,
    })
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

impl<M: Memtable + Send + Sync> LSMStorage<M> {
    pub(crate) fn start_compaction_worker(
        state: Arc<State<M>>,
        event_sender: Sender<StateUpdateEvent>,
        cts: watch::Receiver<()>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let strategy = LeveledCompactionStrategy::new(state.options.max_l0_ssts);
            let mut cts = cts.clone();
            loop {
                tokio::select! {
                    _ = time::sleep(Duration::from_millis(CHECK_COMPACTION_INTERVAL)) => {
                        let levels = state.levels.read().await;
                        let sstables = state.sstables.read().await;

                        if let Some(task) = strategy.pick_compaction(&levels, &sstables) {
                            drop(levels);
                            drop(sstables);

                            let state = state.clone();
                            let sender = event_sender.clone();

                            tokio::spawn(async move {
                                match run_compaction(task, state).await {
                                    Ok(result) => {
                                        sender.send(StateUpdateEvent::CompactionComplete(result)).await.unwrap();
                                    },
                                    Err(e) => {
                                        sender.send(StateUpdateEvent::CompactionFailed(e)).await.unwrap();
                                    },
                                }
                            });
                        }
                    }
                    _ = cts.changed() => {
                        break;
                    }
                }
            }
        })
    }
}
