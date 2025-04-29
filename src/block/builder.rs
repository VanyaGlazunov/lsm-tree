use super::{Block, BufMut, Bytes, SIZEOF_U16};

#[derive(Debug)]
pub(crate) struct BlockBuilder {
    offsets: Vec<u16>,
    data: Vec<u8>,
    block_size: usize,
}

impl BlockBuilder {
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

    /// Appends (key, value) to the block.
    /// Returns true if estimated block size is valid after addition a new element, false otherwise.
    pub fn add(&mut self, key: Bytes, value: Bytes) -> bool {
        // Check if (key, value) fits in current block or if it is the first element in the block.
        if self.size() + key.len() + value.len() + SIZEOF_U16 * 3 > self.block_size
            && !self.is_empty()
        {
            return false;
        }

        // Firstly put offset
        self.offsets.push(self.data.len() as u16);
        // Then key len
        self.data.put_u16(key.len() as u16);
        // Then key itself
        self.data.put(key);
        // Then value len
        self.data.put_u16(value.len() as u16);
        // Then value
        self.data.put(value);

        true
    }

    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
