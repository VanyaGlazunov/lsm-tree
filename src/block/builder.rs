use crate::lsm_storage::Record;

use super::{Block, BufMut, Bytes, SIZEOF_U16};

/// Constructs [Block]
#[derive(Debug)]
pub(crate) struct BlockBuilder {
    offsets: Vec<u16>,
    data: Vec<u8>,
    block_size: usize,
}

impl BlockBuilder {
    /// Creates new instance.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
        }
    }

    /// Returns size of the block.
    pub fn size(&self) -> usize {
        // TODO: Maybe miscalculated slightly.
        self.data.len() + self.offsets.len() * SIZEOF_U16 + SIZEOF_U16
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Tries to appends key-record pair to the block.
    ///
    /// # Returns
    /// Returns true if estimated block size is valid after addition a new element, false otherwise.
    pub fn add(&mut self, key: Bytes, value: Record) -> bool {
        // Check if (key, value) fits in current block or if it is the first element in the block.
        let add_size = key.len() + value.value_len() + SIZEOF_U16 * 3;
        if self.size() + add_size > self.block_size && !self.is_empty() {
            return false;
        }

        self.offsets.push(self.data.len() as u16);
        self.data.put_u16(key.len() as u16);
        self.data.put(key);
        match value {
            Record::Delete => self.data.put_u8(1),
            Record::Put(val) => {
                self.data.put_u8(0);
                // value len
                self.data.put_u32(val.len() as u32);
                // value
                self.data.put(val);
            }
        }

        true
    }

    /// Finilizes block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
