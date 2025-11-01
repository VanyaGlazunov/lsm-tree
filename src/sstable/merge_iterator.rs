use crate::lsm_storage::Record;
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
            Ordering::Equal => self.iterator_idx.cmp(&other.iterator_idx).reverse(),
            other => other,
        }
    }
}

pub struct MergeIterator {
    iters: Vec<Box<dyn Iterator<Item = (Bytes, Record)> + Send>>,
    heap: BinaryHeap<HeapItem>,
}

impl MergeIterator {
    pub fn new(mut iters: Vec<Box<dyn Iterator<Item = (Bytes, Record)> + Send>>) -> Self {
        let mut heap = BinaryHeap::new();

        for (idx, iter) in iters.iter_mut().enumerate() {
            if let Some(item) = iter.next() {
                heap.push(HeapItem {
                    item,
                    iterator_idx: idx,
                });
            }
        }

        Self { iters, heap }
    }
}

impl Iterator for MergeIterator {
    type Item = (Bytes, Record);

    fn next(&mut self) -> Option<Self::Item> {
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

        Some(top_item.item)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        lsm_storage::Record,
        sstable::{builder::SSTableBuilder, SSTable, SSTableIterator},
    };
    use std::sync::Arc;
    use tempfile::tempdir;

    async fn create_sstable(
        data: &[(&str, Option<&str>)],
    ) -> Result<(SSTable, tempfile::TempDir), anyhow::Error> {
        let dir = tempdir()?;
        let path = dir.path().join("test.sst");
        let mut builder = SSTableBuilder::new(1024);
        for (key, value) in data {
            let key_bytes = Bytes::copy_from_slice(key.as_bytes());
            let record = match value {
                None => Record::Delete,
                Some(v) => Record::put_from_slice(v.as_bytes()),
            };
            builder.add(key_bytes, record);
        }
        builder.build(&path).await?;
        let sst = SSTable::open(&path)?;
        Ok((sst, dir))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_merge_iterator_simple() -> Result<(), anyhow::Error> {
        let (sst1, _dir1) = create_sstable(&[("a", Some("1")), ("c", Some("3"))]).await?;
        let (sst2, _dir2) = create_sstable(&[("b", Some("2")), ("d", Some("4"))]).await?;

        let iter1 = SSTableIterator::new(Arc::new(sst1));
        let iter2 = SSTableIterator::new(Arc::new(sst2));
        let mut merge_iter = MergeIterator::new(vec![Box::new(iter1), Box::new(iter2)]);

        assert_eq!(merge_iter.next().unwrap().0, "a");
        assert_eq!(merge_iter.next().unwrap().0, "b");
        assert_eq!(merge_iter.next().unwrap().0, "c");
        assert_eq!(merge_iter.next().unwrap().0, "d");
        assert!(merge_iter.next().is_none());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_merge_iterator_duplicates() -> Result<(), anyhow::Error> {
        let (sst1, _dir1) = create_sstable(&[("a", Some("old_a")), ("c", Some("new_c"))]).await?;
        let (sst2, _dir2) = create_sstable(&[("a", Some("new_a")), ("b", Some("b_val"))]).await?;

        let iter_old = SSTableIterator::new(Arc::new(sst1));
        let iter_new = SSTableIterator::new(Arc::new(sst2));
        let mut merge_iter = MergeIterator::new(vec![Box::new(iter_new), Box::new(iter_old)]);

        let (k, v) = merge_iter.next().unwrap();
        assert_eq!(k, "a");
        assert_eq!(v, Record::put_from_slice(b"new_a"));

        assert_eq!(merge_iter.next().unwrap().0, "b");
        assert_eq!(merge_iter.next().unwrap().0, "c");
        assert!(merge_iter.next().is_none());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_merge_iterator_tombstone() -> Result<(), anyhow::Error> {
        let (sst1, _dir1) = create_sstable(&[("a", Some("new_a")), ("b", None)]).await?;
        let (sst2, _dir2) = create_sstable(&[("b", Some("old_b")), ("c", Some("c_val"))]).await?;

        let iter_new = SSTableIterator::new(Arc::new(sst1));
        let iter_old = SSTableIterator::new(Arc::new(sst2));
        let mut merge_iter = MergeIterator::new(vec![Box::new(iter_new), Box::new(iter_old)]);

        let (k1, v1) = merge_iter.next().unwrap();
        assert_eq!(k1, "a");
        assert_eq!(v1, Record::put_from_slice(b"new_a"));

        let (k2, v2) = merge_iter.next().unwrap();
        assert_eq!(k2, "b");
        assert_eq!(v2, Record::Delete);

        let (k3, v3) = merge_iter.next().unwrap();
        assert_eq!(k3, "c");
        assert_eq!(v3, Record::put_from_slice(b"c_val"));

        assert!(merge_iter.next().is_none());

        Ok(())
    }
}
