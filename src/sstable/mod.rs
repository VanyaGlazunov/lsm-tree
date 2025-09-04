pub(crate) mod builder;
pub(crate) mod merge_iterator;

use std::{
    fs::{File, OpenOptions},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    block::{builder::BlockEntry, Block},
    lsm_storage::Record,
};
use anyhow::{Context, Result};
use bloomfilter::Bloom;
use bytes::{Buf, Bytes};

const OFFSET_SIZE: usize = std::mem::size_of::<u32>();

#[derive(Debug)]
pub struct BlockMeta {
    offset: usize,
    first_key: Bytes,
    last_key: Bytes,
}

/// On-disk sorted key-value data.
#[derive(Debug)]
pub struct SSTable {
    path: PathBuf,            // underlying file
    meta: Vec<BlockMeta>,     // Blocks' meta data
    meta_block_offset: usize, // Offset of meta block in file
    bloom: Bloom<[u8]>,       // Bloom filter to speed up lookups
    first_key: Bytes,         // First key in SSTable
    last_key: Bytes,          // Last key in SSTable
}

fn deserialize_metadata(mut buf: &[u8]) -> Vec<BlockMeta> {
    let num_entries = buf.get_u32() as usize;
    let mut metas = Vec::with_capacity(num_entries);

    for _ in 0..num_entries {
        let offset = buf.get_u32() as usize;
        let first_key_len = buf.get_u16() as usize;
        let first_key = buf.copy_to_bytes(first_key_len);
        let last_key_len = buf.get_u16() as usize;
        let last_key = buf.copy_to_bytes(last_key_len);
        metas.push(BlockMeta {
            offset,
            first_key,
            last_key,
        });
    }

    metas
}

impl SSTable {
    /// Returns first key in SSTable.
    pub fn first_key(&self) -> Bytes {
        self.first_key.clone()
    }

    /// Returns last key in SSTable.
    pub fn last_key(&self) -> Bytes {
        self.last_key.clone()
    }

    pub(crate) fn get_sst_path(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().to_path_buf().join(format!("{id}.sst"))
    }

    /// Opens existing SSTable
    ///
    /// #Errors
    /// - Returns error if can not open SSTable file
    /// - Returns error if can not decode SSTable file  
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .open(&path)
            .context("Failed to open sstable file")?;

        let file_size = file.metadata()?.len() as usize;
        if file_size < OFFSET_SIZE {
            anyhow::bail!("SSTable file is too small to be valid");
        }

        let mut meta_offset = [0u8; OFFSET_SIZE];
        file.read_exact_at(&mut meta_offset, (file_size - OFFSET_SIZE) as u64)
            .context("Failed to read meta offset")?;

        let meta_offset = (&meta_offset[..]).get_u32() as usize;
        if meta_offset >= file_size - OFFSET_SIZE {
            anyhow::bail!("Invalid meta offset");
        }

        let meta_length = file_size - meta_offset - OFFSET_SIZE;
        let mut buf = vec![0; meta_length];
        file.read_exact_at(&mut buf, meta_offset as u64)
            .context("Failed to read meta")?;

        let meta_length = (&buf[buf.len() - OFFSET_SIZE..]).get_u32() as usize;
        let meta_bytes = &buf[..meta_length];

        let meta = deserialize_metadata(meta_bytes);

        let bloom = Bloom::<[u8]>::from_slice(&buf[meta_length..buf.len() - OFFSET_SIZE]).unwrap();

        let first_key = meta
            .first()
            .context("SSTable has no blocks")?
            .first_key
            .clone();
        let last_key = meta
            .last()
            .context("SSTable has no blocks")?
            .last_key
            .clone();

        Ok(SSTable {
            path,
            meta,
            meta_block_offset: meta_offset,
            bloom,
            first_key,
            last_key,
        })
    }

    /// Lookups given key in SSTable
    pub fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        if !self.bloom.check(key) {
            return Ok(None);
        }

        // Index of the first block with first_key >= key.
        let partition_point = self.meta.partition_point(|block| block.first_key <= key);

        if partition_point == 0 {
            return Ok(None);
        }
        let block_ind = partition_point - 1;

        let block = self.read_block(block_ind)?;

        let mut iter = block.iter();
        iter.seek(key);
        match iter.next() {
            Some((cand_key, value)) => {
                if cand_key.as_ref() == key {
                    Ok(Some(value))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    /// Reads block from sstable by index.
    fn read_block(&self, block_index: usize) -> Result<Block> {
        let block_offset = self.meta[block_index].offset;
        let end_offset = self
            .meta
            .get(block_index + 1)
            .map_or(self.meta_block_offset, |b| b.offset);
        let block_len = end_offset - block_offset;

        let mut buf = vec![0; block_len];
        let file =
            File::open(&self.path).context(format!("Failed to open SSTable {:?}", self.path))?;
        file.read_exact_at(&mut buf[..], block_offset as u64)
            .context("Failed to read block data")?;

        Ok(Block::decode(&buf[..]))
    }

    pub fn try_clone(&self) -> Result<Self> {
        SSTable::open(&self.path)
    }
}

pub struct SSTableIterator {
    sstable: Arc<SSTable>,
    current_block: Option<Block>,
    block_idx: usize,
    entry_idx: usize,
}

impl SSTableIterator {
    pub fn new(sstable: Arc<SSTable>) -> Result<Self> {
        let first_block = if !sstable.meta.is_empty() {
            Some(sstable.read_block(0)?)
        } else {
            None
        };

        Ok(Self {
            sstable,
            current_block: first_block,
            block_idx: 0,
            entry_idx: 0,
        })
    }

    fn entry_to_item(entry: &BlockEntry) -> (Bytes, Record) {
        let key = Bytes::from(entry.key.clone());
        let value = match &entry.value {
            None => Record::Delete,
            Some(val) => Record::Put(Bytes::from(val.clone())),
        };
        (key, value)
    }
}

impl Iterator for SSTableIterator {
    type Item = (Bytes, Record);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let block = self.current_block.as_ref()?;

            if let Some(entry) = block.entries.get(self.entry_idx) {
                self.entry_idx += 1;
                return Some(Self::entry_to_item(entry));
            }

            self.block_idx += 1;
            if self.block_idx >= self.sstable.meta.len() {
                self.current_block = None;
                return None;
            }

            self.current_block = self.sstable.read_block(self.block_idx).ok();
            self.entry_idx = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{lsm_storage::Record, sstable::SSTableIterator};

    use super::{builder::SSTableBuilder, Bytes, Result, SSTable};
    use tempfile::NamedTempFile;

    #[test]
    fn test_single_entry() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        {
            let mut builder = SSTableBuilder::new(1024);
            builder.add(Bytes::from("key1"), Record::put_from_slice("value1"));
            builder.build(path)?;
        }

        let sstable = SSTable::open(path)?;
        assert_eq!(
            sstable.get(b"key1")?,
            Some(Record::put_from_slice("value1"))
        );
        assert_eq!(sstable.first_key(), Bytes::from("key1"));
        assert_eq!(sstable.last_key(), Bytes::from("key1"));
        Ok(())
    }

    #[test]
    fn test_multiple_blocks() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        {
            let mut builder = SSTableBuilder::new(30);
            let keys = vec!["a", "b", "c", "d", "e", "f"];
            for key in &keys {
                builder.add(
                    Bytes::from(*key),
                    Record::put_from_slice(format!("value{}", key)),
                );
            }
            builder.build(path)?;
        }

        let sstable = SSTable::open(path)?;
        assert_eq!(sstable.meta.len(), 6);
        assert_eq!(sstable.first_key(), Bytes::from("a"));
        assert_eq!(sstable.last_key(), Bytes::from("f"));

        for key in ["a", "b", "c", "d", "e", "f"] {
            assert_eq!(
                sstable.get(key.as_bytes())?,
                Some(Record::put_from_slice(format!("value{}", key)))
            );
        }
        Ok(())
    }

    #[test]
    fn test_nonexistent_key() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        {
            let mut builder = SSTableBuilder::new(1024);
            builder.add(Bytes::from("b"), Record::put_from_slice("value"));
            builder.build(path)?;
        }

        let sstable = SSTable::open(path)?;

        assert_eq!(sstable.get(b"a")?, None);
        assert_eq!(sstable.get(b"c")?, None);
        Ok(())
    }

    #[test]
    fn test_seek_boundary() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        {
            let mut builder = SSTableBuilder::new(30);
            builder.add(Bytes::from("b"), Record::put_from_slice("value1"));
            builder.add(Bytes::from("d"), Record::put_from_slice("value2"));
            builder.add(Bytes::from("f"), Record::put_from_slice("value3"));
            builder.build(path)?;
        }

        let sstable = SSTable::open(path)?;

        assert_eq!(sstable.get(b"c")?, None);
        assert_eq!(sstable.get(b"e")?, None);
        assert_eq!(sstable.get(b"f")?, Some(Record::put_from_slice("value3")));
        Ok(())
    }

    #[test]
    fn test_corrupted_file() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        std::fs::write(path, "invalid_data")?;

        let result = SSTable::open(path);
        assert!(result.is_err(), "Should fail to open corrupted file");
        Ok(())
    }

    #[test]
    fn test_empty_sstable() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let builder = SSTableBuilder::new(1024);
        let result = builder.build(path);
        assert!(result.is_err(), "Building empty SSTable should error");
    }

    #[test]
    fn test_reopen_persistence() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        {
            let mut builder = SSTableBuilder::new(1024);
            builder.add(Bytes::from("persist"), Record::put_from_slice("data"));
            builder.build(path)?;
        }

        let sstable = SSTable::open(path)?;
        assert_eq!(
            sstable.get(b"persist")?,
            Some(Record::put_from_slice("data"))
        );
        Ok(())
    }

    #[test]
    fn test_sstable_iterator() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        let mut builder = SSTableBuilder::new(60);
        builder.add(Bytes::from("a"), Record::put_from_slice("val_a"));
        builder.add(Bytes::from("b"), Record::put_from_slice("val_b"));
        builder.add(Bytes::from("c"), Record::put_from_slice("val_c"));
        builder.add(Bytes::from("d"), Record::put_from_slice("val_d"));
        builder.build(path)?;

        let sstable = Arc::new(SSTable::open(path)?);
        let mut iter = SSTableIterator::new(sstable)?;

        assert_eq!(
            iter.next(),
            Some((Bytes::from("a"), Record::put_from_slice("val_a")))
        );
        assert_eq!(
            iter.next(),
            Some((Bytes::from("b"), Record::put_from_slice("val_b")))
        );
        assert_eq!(
            iter.next(),
            Some((Bytes::from("c"), Record::put_from_slice("val_c")))
        );
        assert_eq!(
            iter.next(),
            Some((Bytes::from("d"), Record::put_from_slice("val_d")))
        );
        assert_eq!(iter.next(), None);

        Ok(())
    }

    #[test]
    fn test_sstable_iterator_empty() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        let mut builder = SSTableBuilder::new(1024);
        builder.add(Bytes::from("key"), Record::put_from_slice("value"));
        builder.build(path)?;

        let sstable = Arc::new(SSTable::open(path)?);
        let mut iter = SSTableIterator::new(sstable)?;
        assert!(iter.next().is_some());
        assert!(iter.next().is_none());

        Ok(())
    }
}
