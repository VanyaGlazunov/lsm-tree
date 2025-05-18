use bincode::{Decode, Encode};

use crate::lsm_storage::Record;

use super::{Block, Bytes, SIZEOF_U64};

/// Constructs [Block]
#[derive(Debug, Encode, Decode)]
pub(crate) struct BlockBuilder {
    entries: Vec<BlockEntry>,
    current_size: usize,
    block_size: usize,
}

#[derive(Debug, Encode, Decode)]
pub struct BlockEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

impl BlockBuilder {
    /// Creates new instance.
    pub fn new(block_size: usize) -> Self {
        Self {
            entries: Vec::new(),
            current_size: 0,
            block_size,
        }
    }

    /// Returns current size of the block.
    pub fn size(&self) -> usize {
        self.current_size + SIZEOF_U64
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Tries to appends key-record pair to the block.
    ///
    /// # Returns
    /// Returns true if estimated block size is valid after addition a new element, false otherwise.
    pub fn add(&mut self, key: Bytes, value: Record) -> bool {
        // Check if (key, value) fits in current block or if it is the first element in the block.
        let add_size = key.len() + value.value_len() + 1 + 2 * SIZEOF_U64;
        if self.size() + add_size > self.block_size && !self.is_empty() {
            return false;
        }

        self.current_size += add_size;
        let mut entry = BlockEntry {
            key: key.to_vec(),
            value: None,
        };

        match value {
            Record::Delete => (),
            Record::Put(val) => {
                entry.value = Some(Vec::from(val));
            }
        }

        self.entries.push(entry);
        true
    }

    /// Finilizes block.
    pub fn build(self) -> Block {
        Block {
            entries: self.entries,
        }
    }
}
