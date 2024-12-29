use std::cmp::Ordering;
use std::io::ErrorKind;
use std::{cmp, io};

#[derive(Debug, Clone, PartialEq)]
pub enum NodeData {
    Value(Vec<u8>),
    Pointer(usize, usize, usize),
}

#[derive(Debug, Clone)]
struct Node<K> {
    key: K,
    value: NodeData,
    height: i8,
    left_child: Option<Box<Node<K>>>,
    right_child: Option<Box<Node<K>>>,
}

#[derive(Debug)]
pub struct AVLtree<K> {
    root: Option<Box<Node<K>>>,
    size: usize,
}

impl<K: Ord> AVLtree<K> {
    pub fn new() -> Self {
        AVLtree {
            root: None,
            size: 0,
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn insert(&mut self, key: K, value: Vec<u8>) {
        let value_size = value.len();
        let result = Node::insert(self.root.take(), key, value);
        self.root = Some(result.0);
        if result.1 {
            self.size += value_size;
        }
    }

    pub fn get(&self, key: &K) -> io::Result<NodeData> {
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

    pub fn flush(&mut self, table_index: usize) -> Option<Vec<u8>> {
        let mut buffer = Vec::<u8>::new();
        match self.root.take() {
            None => None,
            Some(root) => {
                self.root = Node::flush(root, table_index, 0, &mut buffer).0;
                self.size = 0;
                Some(buffer)
            }
        }
    }
}

impl<K: Ord> Node<K> {
    fn new(key: K, value: Vec<u8>) -> Self {
        Node {
            key,
            value: NodeData::Value(value),
            height: 1,
            left_child: None,
            right_child: None,
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
                node.value = NodeData::Value(value);
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

    fn get(&self, key: &K) -> io::Result<NodeData> {
        let child = match key.cmp(&self.key) {
            Ordering::Less => &self.left_child,
            Ordering::Equal => return Ok(self.value.clone()),
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
        table_index: usize,
        position: usize,
        buffer: &mut Vec<u8>,
    ) -> (Option<Box<Node<K>>>, usize) {
        match node.value {
            NodeData::Pointer(ref _path, _position, _len) => (Some(node), position),
            NodeData::Value(mut value) => {
                let mut next_pos = position;
                if let Some(left_child) = node.left_child {
                    (node.left_child, next_pos) =
                        Node::flush(left_child, table_index, next_pos, buffer);
                }

                let len = value.len();
                buffer.append(&mut value);
                node.value = NodeData::Pointer(table_index, next_pos, len);
                next_pos += len;

                if let Some(right_child) = node.right_child {
                    (node.right_child, next_pos) =
                        Node::flush(right_child, table_index, next_pos, buffer);
                }

                (Some(node), next_pos)
            }
        }
    }
}

#[cfg(test)]
mod avl_tests {
    use super::*;

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
        assert_eq!(retrieved.unwrap(), NodeData::Value(value));
    }

    #[test]
    fn insert_sorted_sequence() {
        let mut avl = AVLtree::new();
        let value = b"value".to_vec();
        for key in (1..100).collect::<Vec<i32>>() {
            avl.insert(key, value.clone());
        }

        for key in (1..100).collect::<Vec<i32>>() {
            assert_eq!(avl.get(&key).unwrap(), NodeData::Value(value.clone()));
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
            assert_eq!(avl.get(&key).unwrap(), NodeData::Value(value.clone()));
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
    fn test_flush() {
        let mut avl = AVLtree::new();
        let value1 = b"Value1".to_vec();
        let value2 = b"Value2".to_vec();
        let value3 = b"Value3".to_vec();
        avl.insert(1, value1.clone());
        avl.insert(2, value2.clone());
        avl.insert(3, value3.clone());

        let buffer = avl.flush(0).unwrap();

        let expected = [value1.clone(), value2.clone(), value3.clone()].concat();
        assert_eq!(buffer, expected);

        if let Some(root) = &avl.root {
            assert_eq!(root.value, NodeData::Pointer(0, 6, 6));
            match &root.left_child {
                Some(left) => assert_eq!(left.value, NodeData::Pointer(0, 0, 6)),
                None => panic!("Left node should not be None after flush"),
            }
            match &root.right_child {
                Some(right) => assert_eq!(right.value, NodeData::Pointer(0, 12, 6)),
                None => panic!("Right node should not be None after flush"),
            }
        } else {
            panic!("Root should not be None after flush");
        }
    }
}
