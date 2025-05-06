use std::{fs::OpenOptions, io::Write, path::Path};

use anyhow::{Context, Result};
use bytes::{BufMut, Bytes};

use crate::{block::builder::BlockBuilder, lsm_storage::Record};

use super::{BlockMeta, SSTable};

pub struct SSTableBuilder {
    block_builder: BlockBuilder,
    target_block_size: usize,
    data: Vec<u8>,
    meta: Vec<BlockMeta>,
    current_first_key: Bytes,
    current_last_key: Bytes,
}

impl SSTableBuilder {
    pub fn new(target_block_size: usize) -> Self {
        Self {
            block_builder: BlockBuilder::new(target_block_size),
            target_block_size,
            data: Vec::new(),
            meta: Vec::new(),
            current_first_key: Bytes::new(),
            current_last_key: Bytes::new(),
        }
    }

    pub fn add(&mut self, key: Bytes, value: Record) {
        if self.current_first_key.is_empty() {
            self.current_first_key = key.clone();
        }

        if self.block_builder.add(key.clone(), value.clone()) {
            self.current_last_key = key;
            return;
        }

        self.finish_block();
        self.block_builder.add(key.clone(), value);
        self.current_first_key = key.clone();
        self.current_last_key = key;
    }

    pub fn finish_block(&mut self) {
        if self.block_builder.is_empty() {
            return;
        }

        let builder = std::mem::replace(
            &mut self.block_builder,
            BlockBuilder::new(self.target_block_size),
        );
        let block = builder.build();
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: std::mem::replace(&mut self.current_first_key, Bytes::new()),
            last_key: std::mem::replace(&mut self.current_last_key, Bytes::new()),
        });

        self.data.extend(block.encode());
    }

    fn serialize_metadata(&self) -> Vec<u8> {
        let mut buf = Vec::<u8>::new();
        buf.put_u32(self.meta.len() as u32);

        for block_meta in &self.meta {
            buf.put_u32(block_meta.offset as u32);
            buf.put_u16(block_meta.first_key.len() as u16);
            buf.put_slice(&block_meta.first_key);
            buf.put_u16(block_meta.last_key.len() as u16);
            buf.put_slice(&block_meta.last_key);
        }

        buf
    }

    /// Builds sst from added data and writes it on disk ans syncs; returns built sst.
    pub fn build(mut self, path: impl AsRef<Path>) -> Result<SSTable> {
        self.finish_block();

        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .open(path)
            .context("Failed to create file for SSTable")?;

        let meta_offset = self.data.len();
        let meta_bytes = self.serialize_metadata();

        let mut buf = self.data;
        buf.extend(meta_bytes);
        buf.put_u32(meta_offset as u32);
        file.write_all(&buf).context("Failed to write sstablw")?;
        file.sync_all().context("Failed to sync sstable file")?;

        Ok(SSTable {
            file,
            first_key: self
                .meta
                .first()
                .context("No first key in sstable -> It is empty")?
                .first_key
                .clone(),
            last_key: self
                .meta
                .last()
                .context("No last key in sstable -> It is empty")?
                .last_key
                .clone(),
            meta: self.meta,
            meta_block_offset: meta_offset,
        })
    }
}
