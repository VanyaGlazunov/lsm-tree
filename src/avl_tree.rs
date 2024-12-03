use std::cmp::Ordering;
use std::fs::File;
use std::io::{ErrorKind, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::{cmp, io};

#[derive(Debug, Clone)]
enum Data {
    Value(Vec<u8>),
    Pointer(PathBuf, u64, usize),
}

#[derive(Debug, Clone)]
struct Node<K> {
    key: K,
    value: Data,
    height: i8,
    left_child: Option<Box<Node<K>>>,
    right_child: Option<Box<Node<K>>>,
}

#[derive(Debug)]
pub struct AVLtree<K> {
    root: Option<Box<Node<K>>>,
    size: u32,
}

#[allow(dead_code)]
impl<K: Ord> AVLtree<K> {
    pub fn new() -> Self {
        AVLtree {
            root: None,
            size: 0,
        }
    }

    pub fn size(&self) -> u32 {
        self.size
    }

    pub fn insert(&mut self, key: K, value: Vec<u8>) {
        let result = Node::insert(self.root.take(), key, value);
        self.root = Some(result.0);
        if result.1 {
            self.size += 1;
        }
    }

    pub fn get(&self, key: &K) -> io::Result<Vec<u8>> {
        match &self.root {
            Some(root) => root.get(key),
            None => Err(ErrorKind::NotFound.into()),
        }
    }

    pub fn contains(&self, key: &K) -> bool {
        match &self.root {
            Some(root) => root.contains(key),
            None => false,
        }
    }

    pub fn flush(&mut self, path: &Path) -> io::Result<()> {
        let mut buffer = Vec::<u8>::new();
        match self.root.take() {
            None => Ok(()),
            Some(root) => match Node::flush(root, path, 0, &mut buffer) {
                Err(e) => Err(e),
                Ok(result) => {
                    self.root = result.0;
                    let mut file = File::create(path)?;
                    file.write_all(&buffer)?;
                    Ok(())
                }
            },
        }
    }
}

#[allow(dead_code)]
impl<K: Ord> Node<K> {
    fn new(key: K, value: Vec<u8>) -> Self {
        Node {
            key,
            value: Data::Value(value),
            height: 1,
            left_child: None,
            right_child: None,
        }
    }

    fn get_value(&self) -> io::Result<Vec<u8>> {
        match &self.value {
            Data::Value(value) => Ok(value.clone()),
            Data::Pointer(path, position, size) => {
                let mut file = File::open(path)?;

                file.seek(io::SeekFrom::Start(*position))?;
                let mut value = vec![0; *size];
                file.read_exact(&mut value)?;
                Ok(value)
            }
        }
    }

    fn get_left_height(&self) -> i8 {
        match &self.left_child {
            Some(left_child) => left_child.height,
            None => 0,
        }
    }

    fn get_right_height(&self) -> i8 {
        match &self.right_child {
            Some(right_child) => right_child.height,
            None => 0,
        }
    }

    fn update_height(&mut self) {
        self.height = cmp::max(self.get_left_height(), self.get_right_height()) + 1;
    }

    fn get_balance(&self) -> i8 {
        self.get_left_height() - self.get_right_height()
    }

    fn rotate_left(mut node: Box<Node<K>>) -> Box<Node<K>> {
        if node.right_child.is_none() {
            return node;
        }
        let mut right_node = node.right_child.take().unwrap();
        node.right_child = right_node.left_child;
        node.update_height();
        right_node.left_child = Some(node);
        right_node.update_height();
        right_node
    }

    fn rotate_right(mut node: Box<Node<K>>) -> Box<Node<K>> {
        if node.left_child.is_none() {
            return node;
        }
        let mut left_node = node.left_child.take().unwrap();
        node.left_child = left_node.right_child;
        node.update_height();
        left_node.right_child = Some(node);
        left_node.update_height();
        left_node
    }

    fn big_rotate_left(mut node: Box<Node<K>>) -> Box<Node<K>> {
        node.right_child = Some(Self::rotate_right(node.right_child.unwrap()));
        Self::rotate_left(node)
    }

    fn big_rotate_right(mut node: Box<Node<K>>) -> Box<Node<K>> {
        node.left_child = Some(Self::rotate_left(node.left_child.unwrap()));
        Self::rotate_right(node)
    }

    fn balance(node: Box<Node<K>>) -> Box<Node<K>> {
        match node.get_balance() {
            -2 => {
                if node.right_child.as_ref().unwrap().get_balance() <= 0 {
                    return Self::rotate_left(node);
                }
                Self::big_rotate_left(node)
            }
            2 => {
                if node.left_child.as_ref().unwrap().get_balance() > 0 {
                    return Self::rotate_right(node);
                }
                Self::big_rotate_right(node)
            }
            _ => node,
        }
    }

    fn insert(node: Option<Box<Node<K>>>, key: K, value: Vec<u8>) -> (Box<Node<K>>, bool) {
        if node.is_none() {
            return (Box::new(Self::new(key, value)), true);
        }
        let mut node = node.unwrap();
        let mut inserted = true;
        match key.cmp(&node.key) {
            Ordering::Less => {
                let result = Self::insert(node.left_child, key, value);
                inserted &= result.1;
                node.left_child = Some(result.0);
            }
            Ordering::Equal => {
                node.value = Data::Value(value);
                return (node, false);
            }
            Ordering::Greater => {
                let result = Self::insert(node.right_child, key, value);
                inserted &= result.1;
                node.right_child = Some(result.0);
            }
        };
        node.update_height();
        (Self::balance(node), inserted)
    }

    fn get(&self, key: &K) -> io::Result<Vec<u8>> {
        let child = match key.cmp(&self.key) {
            Ordering::Less => &self.left_child,
            Ordering::Equal => return self.get_value(),
            Ordering::Greater => &self.right_child,
        };

        match child {
            Some(node) => node.get(key),
            None => Err(ErrorKind::NotFound.into()),
        }
    }

    fn contains(&self, key: &K) -> bool {
        let child = match key.cmp(&self.key) {
            Ordering::Less => &self.left_child,
            Ordering::Equal => return true,
            Ordering::Greater => &self.right_child,
        };

        child.is_none()
    }

    fn flush(
        mut node: Box<Node<K>>,
        path: &Path,
        position: u64,
        buffer: &mut Vec<u8>,
    ) -> io::Result<(Option<Box<Node<K>>>, u64)> {
        match node.value {
            Data::Pointer(ref _path, _position, _len) => Ok((Some(node), position)),
            Data::Value(mut value) => {
                let mut next_pos = position;
                if let Some(left_child) = node.left_child {
                    (node.left_child, next_pos) =
                        Node::flush(left_child, path, next_pos, buffer).unwrap();
                }

                let len = value.len();
                buffer.append(&mut value);
                node.value = Data::Pointer(path.to_path_buf(), next_pos, len);
                next_pos += len as u64;

                if let Some(right_child) = node.right_child {
                    (node.right_child, next_pos) =
                        Node::flush(right_child, path, next_pos, buffer).unwrap();
                }

                Ok((Some(node), next_pos))
            }
        }
    }
}

#[cfg(test)]
mod avl_tests {
    use super::*;
    use tempfile::{self, NamedTempFile};

    fn get_height<K>(avl_node: &Node<K>) -> i32 {
        let mut max_height = 0;
        if let Some(left_child) = &avl_node.left_child {
            max_height = get_height(&left_child);
        }
        if let Some(right_child) = &avl_node.right_child {
            let right_height = get_height(&right_child);
            if right_height > max_height {
                max_height = right_height;
            }
        }
        max_height + 1
    }

    fn get_balance<K>(avl_node: &Node<K>) -> i32 {
        let mut balance = 0;
        if let Some(left_child) = &avl_node.left_child {
            balance += get_height(&left_child);
        }
        if let Some(right_child) = &avl_node.right_child {
            balance -= get_height(&right_child);
        }
        balance
    }

    fn check_correctness<K>(avl_node: &Node<K>) -> bool {
        let balance = get_balance(avl_node);
        if balance > 1 || balance < -1 {
            return false;
        }
        let mut result = true;
        if let Some(left_child) = &avl_node.left_child {
            result &= check_correctness(&left_child);
        }
        if let Some(right_child) = &avl_node.right_child {
            result &= check_correctness(&right_child);
        }
        result
    }

    #[test]
    fn insert_one_elem() {
        let mut avl = AVLtree::new();
        let key = 1;
        let value = b"value".to_vec();
        avl.insert(key, value.clone());
        let retrieved = avl.get(&key);
        assert_eq!(retrieved.unwrap(), value);
    }

    #[test]
    fn insert_sorted_sequence() {
        let mut avl = AVLtree::new();
        let value = b"value".to_vec();
        for key in (1..100).collect::<Vec<i32>>() {
            avl.insert(key, value.clone());
        }

        for key in (1..100).collect::<Vec<i32>>() {
            assert_eq!(avl.get(&key).unwrap(), value.clone());
        }

        assert!(check_correctness(&avl.root.unwrap()));
    }

    #[test]
    fn insert_reverse_sorted_sequence() {
        let mut avl = AVLtree::new();
        let value = b"value".to_vec();
        for key in (1..100).rev().collect::<Vec<i32>>() {
            avl.insert(key, value.clone());
        }

        for key in (1..100).collect::<Vec<i32>>() {
            assert_eq!(avl.get(&key).unwrap(), value.clone());
        }

        assert!(check_correctness(&avl.root.unwrap()));
    }

    #[test]
    fn get_nonexistent() {
        let mut avl = AVLtree::new();
        let value = b"value".to_vec();
        avl.insert(1, value.clone());
        avl.insert(2, value.clone());
        avl.insert(3, value.clone());
        let key = 5;
        assert!(avl.get(&key).is_err());
    }

    #[test]
    fn test_flush() -> io::Result<()> {
        let mut avl = AVLtree::new();
        let value1 = b"Value1".to_vec();
        let value2 = b"Value2".to_vec();
        let value3 = b"Value3".to_vec();
        avl.insert(1, value1.clone());
        avl.insert(2, value2.clone());
        avl.insert(3, value3.clone());

        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        avl.flush(path)?;
        let mut file = File::open(&temp_file)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let expected = [value1.clone(), value2.clone(), value3.clone()].concat();
        assert_eq!(buffer, expected);

        if let Some(root) = &avl.root {
            assert_eq!(root.get_value().unwrap(), value2);
            match &root.value {
                Data::Pointer(p, _pos, len) => {
                    assert_eq!(p, &path);
                    assert_eq!(*len, value2.len());
                }
                _ => panic!("Expected Data::Pointer"),
            }
            if let Some(left) = &root.left_child {
                assert_eq!(left.get_value().unwrap(), value1);
                match &left.value {
                    Data::Pointer(p, _pos, len) => {
                        assert_eq!(p, &path);
                        assert_eq!(*len, value1.len());
                    }
                    _ => panic!("Expected Data::Pointer"),
                }
            }
            if let Some(right) = &root.right_child {
                assert_eq!(right.get_value().unwrap(), value3);
                match &right.value {
                    Data::Pointer(p, _pos, len) => {
                        assert_eq!(p, &path);
                        assert_eq!(*len, value3.len());
                    }
                    _ => panic!("Expected Data::Pointer"),
                }
            }
        } else {
            panic!("Root should not be None after flush");
        }

        Ok(())
    }
}
