use std::cmp;
use std::cmp::Ordering;
use std::mem;

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct Node<K, V> {
    key: K,
    value: V,
    height: i8,
    left_child: Option<Box<Node<K, V>>>,
    right_child: Option<Box<Node<K, V>>>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct AVL<K, V> {
    root: Option<Box<Node<K, V>>>,
}

pub struct AVLIterator<'a, K, V> {
    prev_nodes: Vec<&'a Node<K, V>>,
    current_node: &'a Option<Box<Node<K, V>>>,
}

#[allow(dead_code)]
impl<K: Ord + Clone, V: Clone> AVL<K, V> {
    pub fn new() -> Self {
        AVL { root: None }
    }

    fn iter<'a>(&'a self) -> AVLIterator<'a, K, V> {
        AVLIterator {
            prev_nodes: Vec::new(),
            current_node: &self.root,
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.root = Some(Node::insert(self.root.take(), key, value));
    }

    pub fn get(&self, key: &K) -> Option<V> {
        match &self.root {
            Some(root) => root.get(key),
            None => None,
        }
    }

    pub fn contains(&self, key: &K) -> bool {
        match &self.root {
            Some(root) => root.contains(key),
            None => false,
        }
    }

    pub fn remove(&mut self, key: &K) {
        if let Some(root) = self.root.take() {
            self.root = Node::remove(root, key);
        }
    }
}

impl<'a, K, V> Iterator for AVLIterator<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.current_node {
                None => {
                    match self.prev_nodes.pop() {
                        None => {
                            return None
                        },
                        Some(prev_node) => {
                            self.current_node = &prev_node.right_child;

                            return Some((&prev_node.key, &prev_node.value))
                        }
                    }
                }
                Some(current_node) => {
                    if current_node.left_child.is_some() {
                        self.prev_nodes.push(current_node);
                        self.current_node = &current_node.left_child;
                    } else if current_node.right_child.is_some() {
                        self.current_node = &current_node.right_child;
                        return Some((&current_node.key, &current_node.value));
                    } else {
                        self.current_node = &None;
                        return Some((&current_node.key, &current_node.value));
                    }
                }
            }
        }
    }
}

#[allow(dead_code)]
impl<K: Ord + Clone, V: Clone> Node<K, V> {
    fn new(key: K, value: V) -> Self {
        Node {
            key,
            value,
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

    fn rotate_left(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
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

    fn rotate_right(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
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

    fn big_rotate_left(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        node.right_child = Some(Self::rotate_right(node.right_child.unwrap()));
        Self::rotate_left(node)
    }

    fn big_rotate_right(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        node.left_child = Some(Self::rotate_left(node.left_child.unwrap()));
        Self::rotate_right(node)
    }

    fn balance(node: Box<Node<K, V>>) -> Box<Node<K, V>> {
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

    fn insert(node: Option<Box<Node<K, V>>>, key: K, value: V) -> Box<Node<K, V>> {
        if node.is_none() {
            return Box::new(Self::new(key, value));
        }
        let mut node = node.unwrap();
        match key.cmp(&node.key) {
            Ordering::Less => {
                let result = Self::insert(node.left_child, key, value);
                node.left_child = Some(result);
            }
            Ordering::Equal => {
                node.value = value;
            }
            Ordering::Greater => {
                let result = Self::insert(node.right_child, key, value);
                node.right_child = Some(result);
            }
        };
        node.update_height();
        Self::balance(node)
    }

    fn get(&self, key: &K) -> Option<V> {
        let child = match key.cmp(&self.key) {
            Ordering::Less => &self.left_child,
            Ordering::Equal => return Some(self.value.clone()),
            Ordering::Greater => &self.right_child,
        };

        match child {
            Some(node) => node.get(key),
            None => None,
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

    fn remove(mut node: Box<Node<K, V>>, key: &K) -> Option<Box<Node<K, V>>> {
        match key.cmp(&node.key) {
            Ordering::Less => {
                if let Some(left_child) = node.left_child.take() {
                    node.left_child = Self::remove(left_child, key);
                    node.update_height();
                }
                Some(Node::balance(node))
            }
            Ordering::Equal => match (node.left_child.take(), node.right_child.take()) {
                (Some(_left_child), Some(right_child)) => {
                    let mut right_min = right_child.get_min();
                    mem::swap(node.as_mut(), &mut right_min);
                    node.right_child = Node::remove(right_child, &node.key);
                    node.update_height();
                    Some(Node::balance(node))
                }
                (Some(left_child), None) => Some(left_child),
                (None, Some(right_child)) => Some(right_child),
                (None, None) => None,
            },
            Ordering::Greater => {
                if let Some(right_child) = node.right_child.take() {
                    node.right_child = Self::remove(right_child, key);
                    node.update_height();
                }
                Some(Node::balance(node))
            }
        }
    }

    fn get_min(&self) -> Self {
        if let Some(left_child) = &self.left_child {
            return Node::get_min(left_child);
        };

        self.clone()
    }
}

#[cfg(test)]
mod avl_tests {
    use std::iter::zip;

    use super::*;

    fn get_height<K, V>(avl_node: &Node<K, V>) -> i32 {
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

    fn get_balance<K, V>(avl_node: &Node<K, V>) -> i32 {
        let mut balance = 0;
        if let Some(left_child) = &avl_node.left_child {
            balance += get_height(&left_child);
        }
        if let Some(right_child) = &avl_node.right_child {
            balance -= get_height(&right_child);
        }
        balance
    }

    fn check_correctness<K, V>(avl_node: &Node<K, V>) -> bool {
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
    fn insert_find_one_elem() {
        let mut avl = AVL::new();
        avl.insert(1, 1);
        assert_eq!(avl.get(&1).unwrap(), 1);
    }

    #[test]
    fn insert_ordered_sequence() {
        let mut avl = AVL::new();
        for key in 0..100 {
            avl.insert(key, key);
        }

        for key in 0..100 {
            assert_ne!(avl.get(&key), None);
        }
        assert!(check_correctness(avl.root.as_ref().unwrap()));
    }

    #[test]
    fn insert_reverse_ordered_sequence() {
        let mut avl = AVL::new();
        for key in (0..100).rev() {
            avl.insert(key, key);
        }

        for key in 0..100 {
            assert_ne!(avl.get(&key), None);
        }
        assert!(check_correctness(avl.root.as_ref().unwrap()));
    }

    #[test]
    fn insert_erase_find() {
        let mut avl = AVL::new();
        for key in 0..100 {
            avl.insert(key, key);
        }

        for key in 0..100 {
            avl.remove(&key);
        }

        for key in 0..100 {
            assert_eq!(avl.get(&key), None);
        }
    }

    #[test]
    fn chek_iter() {
        let mut avl = AVL::new();
        for key in 0..100 {
            avl.insert(key, key);
        }
        
        for (actual, expected) in zip(avl.iter(), 0..100) {
            assert_eq!(actual, (&expected, &expected));
        }
    }
}
