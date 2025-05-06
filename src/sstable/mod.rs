pub(crate) mod builder;

use std::{
    fs::{File, OpenOptions},
    os::unix::fs::FileExt,
    path::Path,
};

use crate::{block::Block, lsm_storage::Record};
use anyhow::{Context, Result};
use bytes::{Buf, Bytes};

const FOOTER_SIZE: usize = std::mem::size_of::<u32>();

#[derive(Debug)]
pub struct BlockMeta {
    offset: usize,
    first_key: Bytes,
    last_key: Bytes,
}

#[derive(Debug)]
pub struct SSTable {
    file: File,
    meta: Vec<BlockMeta>,
    meta_block_offset: usize,
    first_key: Bytes,
    last_key: Bytes,
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
    pub fn first_key(&self) -> Bytes {
        self.first_key.clone()
    }

    pub fn last_key(&self) -> Bytes {
        self.last_key.clone()
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .context("Failed to open sstable file")?;

        let file_size = file.metadata()?.len() as usize;
        if file_size < FOOTER_SIZE {
            anyhow::bail!("SSTable file is too small to be valid");
        }

        let mut footer = [0u8; FOOTER_SIZE];
        let footer_offset = file_size - FOOTER_SIZE;
        file.read_exact_at(&mut footer, footer_offset as u64)
            .context("Failed to read footer")?;

        let meta_offset = (&footer[..]).get_u32() as usize;
        if meta_offset >= file_size - FOOTER_SIZE {
            anyhow::bail!("Invalid metadata offset");
        }

        let meta_section_length = file_size - meta_offset - FOOTER_SIZE;
        let mut meta_bytes = vec![0; meta_section_length];
        file.read_exact_at(&mut meta_bytes, meta_offset as u64)
            .context("Failed to read metadata")?;

        let meta = deserialize_metadata(&meta_bytes);

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
            file,
            meta,
            meta_block_offset: meta_offset,
            first_key,
            last_key,
        })
    }

    /// Returns result containing value associated with the given key if it is present in the table, None otherwise.
    pub fn get(&self, key: &[u8]) -> Result<Option<Record>> {
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

    /// Reads block from sstable file.
    pub fn read_block(&self, block_index: usize) -> Result<Block> {
        let block_offset = self.meta[block_index].offset;
        let end_offset = self
            .meta
            .get(block_index + 1)
            .map_or(self.meta_block_offset, |b| b.offset);
        let block_len = end_offset - block_offset;

        let mut buf = vec![0; block_len];
        self.file
            .read_exact_at(&mut buf[..], block_offset as u64)
            .context("Failed to read block data")?;

        Ok(Block::decode(&buf[..]))
    }
}

impl FromIterator<(Bytes, Record)> for SSTable {
    fn from_iter<T: IntoIterator<Item = (Bytes, Record)>>(_iter: T) -> Self {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::lsm_storage::Record;

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
        assert_eq!(sstable.meta.len(), 3);
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
}
