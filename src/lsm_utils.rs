use std::path::{Path, PathBuf};

use crate::{
    memtable::Memtable,
    sstable::{builder::SSTableBuilder, SSTable},
};
use anyhow::{Context, Result};

/// Creates sstable from memtable via build method in SSTableBuilder.
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
        .build(get_sst_path(path, id))
        .context(format!("Failed to flush memtable. id: {id}"))
}

pub(crate) fn get_sst_path(path: impl AsRef<Path>, id: usize) -> PathBuf {
    path.as_ref().to_path_buf().join(format!("{id}.sst"))
}
