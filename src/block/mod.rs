pub(crate) mod builder;

use bytes::{Buf, BufMut, Bytes};

use crate::lsm_storage::Record;

const SIZEOF_U16: usize = std::mem::size_of::<u16>();

#[derive(Debug)]
pub struct Block {
    pub data: Vec<u8>,
    pub offsets: Vec<u16>,
}

impl Block {
    pub fn iter(&self) -> BlockIterator {
        BlockIterator {
            data: &self.data,
            offsets: &self.offsets,
            current_idx: 0,
        }
    }

    pub fn encode(self) -> Bytes {
        let mut buf = self.data;
        let offsets_len = self.offsets.len();
        for offset in self.offsets {
            buf.put_u16(offset);
        }

        buf.put_u16(offsets_len as u16);
        buf.into()
    }

    pub fn decode(buf: &[u8]) -> Self {
        let offsets_len = (&buf[buf.len() - SIZEOF_U16..]).get_u16() as usize;
        let data_end = buf.len() - SIZEOF_U16 - SIZEOF_U16 * offsets_len;
        let offsets_raw = &buf[data_end..buf.len() - SIZEOF_U16];
        let offsets = offsets_raw
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        let data = buf[0..data_end].to_vec();
        Self { data, offsets }
    }
}

pub struct BlockIterator<'a> {
    data: &'a [u8],
    offsets: &'a [u16],
    current_idx: usize,
}

impl BlockIterator<'_> {
    /// Seeks to the first entry with key >= target
    pub fn seek(&mut self, target: &[u8]) {
        self.current_idx = self.offsets.partition_point(|&offset| {
            let mut entry = &self.data[offset as usize..];
            let key_len = entry.get_u16() as usize;
            let key = &entry[..key_len];
            key < target
        });
    }
}

impl Iterator for BlockIterator<'_> {
    type Item = (Bytes, Record);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_idx >= self.offsets.len() {
            return None;
        }

        let start = self.offsets[self.current_idx] as usize;
        if start >= self.data.len() {
            return None;
        }

        let mut entry = &self.data[start..];
        let key_len = entry.get_u16() as usize;
        let key = entry.copy_to_bytes(key_len);
        let tombstone = entry.get_u8();

        self.current_idx += 1;
        if tombstone == 0 {
            let value_len = entry.get_u16() as usize;
            let value = &entry[..value_len];
            Some((key, Record::put_from_slice(value)))
        } else {
            Some((key, Record::Delete))
        }
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
        assert!(block.data.is_empty());
        assert!(block.offsets.is_empty());

        let encoded = block.encode();
        let decoded = Block::decode(&encoded);
        assert!(decoded.data.is_empty());
        assert!(decoded.offsets.is_empty());
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
        // (2 + 2 + 2 + 4 + 2) * 3 + 2 = 38
        let mut builder = BlockBuilder::new(37); // 3 entries will fill exactly 38 bytes
        assert!(builder.add(Bytes::from("k1"), Record::put_from_slice("val1")));
        assert!(builder.add(Bytes::from("k2"), Record::put_from_slice("val2")));

        assert!(!builder.add(Bytes::from("k3"), Record::put_from_slice("val3")));

        let block = builder.build();
        assert_eq!(block.offsets.len(), 2);
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
        assert!(decoded.offsets.is_empty());
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
        assert_eq!(builder.size(), 2);

        builder.add(Bytes::from("k"), Record::put_from_slice("v"));
        let expected_size = 11;
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
        assert_eq!(block.offsets.len(), 1);
    }
}
