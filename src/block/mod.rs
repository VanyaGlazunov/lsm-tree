pub(crate) mod builder;

use bincode::{
    config::{Config, Configuration, Fixint, LittleEndian, NoLimit},
    decode_from_slice, encode_to_vec, Decode, Encode,
};
use builder::BlockEntry;
use bytes::Bytes;

use crate::lsm_storage::Record;

const SIZEOF_U64: usize = std::mem::size_of::<u64>();

/// A fixed-size storage unit containing key-value entries and offset metadata.
///
/// # Fields
/// - `data`: Serialized key-value entries (binary format)
/// - `offsets`: Array of u16 offsets pointing to entry locations in `data`
/// ## Block Format (On-Disk)
///
/// | Component          | Data Type      | Description                                                                 |
/// |---------------------|----------------|-----------------------------------------------------------------------------|
/// | **Data**           | `Vec<u8>`      | Serialized entries in binary format. Each entry contains:                   |
/// |                    |                | - **Key Length** (2 bytes): `u16` length of key                             |
/// |                    |                | - **Key**: Raw key bytes                                                   |
/// |                    |                | - **Tombstone** (1 byte): `0` = Put, `1` = Delete                          |
/// |                    |                | - **Value Length** (4 bytes, optional): Only present for `Put` records     |
/// |                    |                | - **Value** (optional): Raw value bytes for `Put`                          |
/// | **Offsets**        | `Vec<u16>`     | Array of 2-byte offsets pointing to the start of each entry in `data`       |
/// | **Footer**         | `u16`          | 2-byte count of entries (number of offsets)                                 |

#[derive(Debug, Encode, Decode)]
pub struct Block {
    pub entries: Vec<BlockEntry>,
}

fn bincode_config() -> impl Config {
    Configuration::<LittleEndian, Fixint, NoLimit>::default()
}

impl Block {
    /// Creates an iterator over the block's entries
    pub fn iter(&self) -> BlockIterator {
        BlockIterator {
            entries: &self.entries,
            current_idx: 0,
        }
    }

    /// Serializes block into [Bytes]
    pub fn encode(self) -> Vec<u8> {
        encode_to_vec(self, bincode_config()).unwrap()
    }

    /// Reconstructs block from byte slice
    ///
    /// #Panics
    /// - May panic if the block is corrupted/invalid. To prevent this use [builder::BlockBuilder] to build blocks.
    pub fn decode(buf: &[u8]) -> Self {
        decode_from_slice(buf, bincode_config()).unwrap().0
    }
}

/// Iterator for scanning entries in a [Block]
pub struct BlockIterator<'a> {
    entries: &'a [BlockEntry],
    current_idx: usize,
}

impl BlockIterator<'_> {
    /// Binary search to first entry with key >= target.
    pub fn seek(&mut self, target: &[u8]) {
        self.current_idx = self
            .entries
            .partition_point(|entry| &entry.key[..] < target);
    }
}

impl Iterator for BlockIterator<'_> {
    type Item = (Bytes, Record);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_idx >= self.entries.len() {
            return None;
        }

        let BlockEntry { key, value } = &self.entries[self.current_idx];
        let key = Bytes::from(key.clone());
        let value = match value {
            None => Record::Delete,
            Some(val) => Record::Put(val.clone().into()),
        };

        self.current_idx += 1;

        Some((key, value))
    }
}

#[cfg(test)]
mod tests {
    use crate::lsm_storage::Record;

    use super::*;
    use builder::BlockBuilder;
    use Bytes;

    #[test]
    fn test_empty_block() {
        let builder = BlockBuilder::new(1024);
        let block = builder.build();
        assert!(block.entries.is_empty());

        let encoded = block.encode();
        let decoded = Block::decode(&encoded);
        assert!(decoded.entries.is_empty());
    }

    #[test]
    fn test_single_entry() {
        let mut builder = BlockBuilder::new(1024);
        assert!(builder.add(Bytes::from("key"), Record::put_from_slice("value")));

        let block = builder.build();
        let encoded = block.encode();
        let decoded = Block::decode(&encoded);

        let mut iter = decoded.iter();
        assert_eq!(
            iter.next(),
            Some((Bytes::from("key"), Record::put_from_slice("value")))
        );
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_multiple_entries() {
        let mut builder = BlockBuilder::new(1024);
        let entries = vec![
            (Bytes::from("apple"), Record::put_from_slice("red")),
            (Bytes::from("banana"), Record::put_from_slice("yellow")),
            (Bytes::from("cherry"), Record::put_from_slice("red")),
        ];

        for (k, v) in &entries {
            assert!(builder.add(k.clone(), v.clone()));
        }

        let block = builder.build();
        let mut iter = block.iter();

        for (k, v) in entries {
            assert_eq!(iter.next(), Some((k, v)));
        }
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_block_boundary() {
        let mut builder = BlockBuilder::new(54); // 3 entries will fill exactly 54 bytes
        assert!(builder.add(Bytes::from("k1"), Record::put_from_slice("val1")));
        assert!(builder.add(Bytes::from("k2"), Record::put_from_slice("val2")));

        assert!(!builder.add(Bytes::from("k3"), Record::put_from_slice("val3")));

        let block = builder.build();
        assert_eq!(block.entries.len(), 2);
    }

    #[test]
    fn test_seek_behavior() {
        let mut builder = BlockBuilder::new(1024);
        let entries = vec![
            (Bytes::from("b"), Record::put_from_slice("1")),
            (Bytes::from("d"), Record::put_from_slice("2")),
            (Bytes::from("f"), Record::put_from_slice("3")),
        ];

        for (k, v) in entries {
            builder.add(k, v);
        }

        let block = builder.build();
        let mut iter = block.iter();

        iter.seek(b"d");
        assert_eq!(
            iter.next(),
            Some((Bytes::from("d"), Record::put_from_slice("2")))
        );

        iter.seek(b"c");
        assert_eq!(
            iter.next(),
            Some((Bytes::from("d"), Record::put_from_slice("2")))
        );

        iter.seek(b"z");
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_corrupted_data() {
        let invalid_data = vec![0u8; 10];
        let decoded = Block::decode(&invalid_data);
        assert!(decoded.entries.is_empty());
    }

    #[test]
    fn test_edge_cases() {
        let mut builder = BlockBuilder::new(1024);
        assert!(builder.add(Bytes::new(), Record::put_from_slice("")));

        let block = builder.build();
        let mut iter = block.iter();
        assert_eq!(
            iter.next(),
            Some((Bytes::new(), Record::put_from_slice("")))
        );

        let mut builder = BlockBuilder::new(1024);
        let binary_key = Bytes::from(vec![0u8, 255u8]);
        let binary_val = Record::put_from_slice(vec![1u8, 2u8, 3u8]);
        assert!(builder.add(binary_key.clone(), binary_val.clone()));

        let block = builder.build();
        let mut iter = block.iter();
        assert_eq!(iter.next(), Some((binary_key, binary_val)));
    }

    #[test]
    fn test_size() {
        let mut builder = BlockBuilder::new(1024);
        assert_eq!(builder.size(), 8);

        builder.add(Bytes::from("k"), Record::put_from_slice("v"));
        let expected_size = 27;
        assert_eq!(builder.size(), expected_size);
    }

    #[test]
    fn test_large_entry() {
        let mut builder = BlockBuilder::new(10);
        assert!(builder.add(
            Bytes::from("very_long_key"),
            Record::put_from_slice("very_long_value")
        ));

        let block = builder.build();
        assert_eq!(block.entries.len(), 1);
    }
}
