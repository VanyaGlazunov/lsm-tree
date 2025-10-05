use crate::{lsm_storage::Record, sstable::SSTableIterator};
use anyhow::Result;
use bytes::Bytes;
use std::{cmp::Ordering, collections::BinaryHeap};

#[derive(Debug)]
struct HeapItem {
    item: (Bytes, Record),
    iterator_idx: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.item.0 == other.item.0 && self.iterator_idx == other.iterator_idx
    }
}
impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.item.0.cmp(&other.item.0).reverse() {
            Ordering::Equal => self.iterator_idx.cmp(&other.iterator_idx),
            other => other,
        }
    }
}

pub struct MergeIterator {
    iters: Vec<SSTableIterator>,
    heap: BinaryHeap<HeapItem>,
}

impl MergeIterator {
    pub fn new(mut iters: Vec<SSTableIterator>) -> Result<Self> {
        let mut heap = BinaryHeap::new();

        for (idx, iter) in iters.iter_mut().enumerate() {
            if let Some(item) = iter.next() {
                heap.push(HeapItem {
                    item,
                    iterator_idx: idx,
                });
            }
        }

        Ok(Self { iters, heap })
    }
}

impl Iterator for MergeIterator {
    type Item = (Bytes, Record);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let top_item = self.heap.pop()?;

            let source_idx = top_item.iterator_idx;
            if let Some(next_item) = self.iters[source_idx].next() {
                self.heap.push(HeapItem {
                    item: next_item,
                    iterator_idx: source_idx,
                });
            }

            while let Some(peeked_item) = self.heap.peek() {
                if peeked_item.item.0 == top_item.item.0 {
                    let duplicate = self.heap.pop().unwrap();
                    if let Some(next_item) = self.iters[duplicate.iterator_idx].next() {
                        self.heap.push(HeapItem {
                            item: next_item,
                            iterator_idx: duplicate.iterator_idx,
                        });
                    }
                } else {
                    break;
                }
            }

            if top_item.item.1 != Record::Delete {
                return Some(top_item.item);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        lsm_storage::Record,
        sstable::{builder::SSTableBuilder, SSTable},
    };
    use std::sync::Arc;
    use tempfile::tempdir;

    async fn create_sstable(data: &[(&str, &str)]) -> Result<(SSTable, tempfile::TempDir)> {
        let dir = tempdir()?;
        let path = dir.path().join("test.sst");
        let mut builder = SSTableBuilder::new(1024);
        for (key, value) in data {
            let key_bytes = Bytes::copy_from_slice(key.as_bytes());
            let record = Record::put_from_slice(value.as_bytes());
            builder.add(key_bytes, record);
        }
        builder.build(&path).await?;
        let sst = SSTable::open(&path)?;
        Ok((sst, dir))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_merge_iterator_simple() -> Result<()> {
        let (sst1, _dir1) = create_sstable(&[("a", "1"), ("c", "3")]).await?;
        let (sst2, _dir2) = create_sstable(&[("b", "2"), ("d", "4")]).await?;

        let iter1 = SSTableIterator::new(Arc::new(sst1));
        let iter2 = SSTableIterator::new(Arc::new(sst2));
        let mut merge_iter = MergeIterator::new(vec![iter1, iter2])?;

        assert_eq!(merge_iter.next().unwrap().0, "a");
        assert_eq!(merge_iter.next().unwrap().0, "b");
        assert_eq!(merge_iter.next().unwrap().0, "c");
        assert_eq!(merge_iter.next().unwrap().0, "d");
        assert!(merge_iter.next().is_none());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_merge_iterator_duplicates() -> Result<()> {
        let (sst1, _dir1) = create_sstable(&[("a", "old_a"), ("c", "new_c")]).await?;
        let (sst2, _dir2) = create_sstable(&[("a", "new_a"), ("b", "b_val")]).await?;

        let iter1 = SSTableIterator::new(Arc::new(sst1));
        let iter2 = SSTableIterator::new(Arc::new(sst2));
        let mut merge_iter = MergeIterator::new(vec![iter1, iter2])?;

        let (k, v) = merge_iter.next().unwrap();
        assert_eq!(k, "a");
        assert_eq!(v, Record::put_from_slice(b"new_a"));

        assert_eq!(merge_iter.next().unwrap().0, "b");
        assert_eq!(merge_iter.next().unwrap().0, "c");
        assert!(merge_iter.next().is_none());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_merge_iterator_tombstone() -> Result<()> {
        let dir1 = tempdir()?;
        let sst1 = {
            let path = dir1.path().join("sst1.sst");
            let mut builder = SSTableBuilder::new(1024);
            builder.add(Bytes::from("a"), Record::put_from_slice(b"new_a"));
            builder.add(Bytes::from("b"), Record::Delete);
            builder.build(&path).await?;
            Arc::new(SSTable::open(&path)?)
        };
        let (sst2, _dir2) = create_sstable(&[("b", "old_b"), ("c", "c_val")]).await?;

        let iter1 = SSTableIterator::new(sst1);
        let iter2 = SSTableIterator::new(Arc::new(sst2));
        let mut merge_iter = MergeIterator::new(vec![iter2, iter1])?;

        assert_eq!(merge_iter.next().unwrap().0, "a");
        assert_eq!(merge_iter.next().unwrap().0, "c");
        assert!(merge_iter.next().is_none());

        Ok(())
    }
}
