use std::cmp::Ordering;

struct Node {
    key: i32,
    value: i32,
    left_child: Option<Box<Node>>,
    right_child: Option<Box<Node>>,
}

pub struct AVL {
    root: Option<Box<Node>>,
}

impl AVL {
    fn new() -> Self {
        AVL { root: None }
    }
    fn insert(&mut self, key: i32, value: i32) {
        match &mut self.root {
            Some(root) => root.insert(key, value),
            None => self.root = Some(Box::new(Node::new(key, value))),
        }
    }
    fn find(&self, key: i32) -> Option<i32> {
        match &self.root {
            Some(root) => root.find(key),
            None => None,
        }
    }
    fn erase(&mut self, key: i32) {
        if let Some(root) = self.root.take() {
            self.root = Node::erase(root, key);
        }
    }
}

impl Node {
    fn new(key: i32, value: i32) -> Self {
        Node {
            key,
            value,
            left_child: None,
            right_child: None,
        }
    }
    fn insert(&mut self, key: i32, value: i32) {
        let child = match key.cmp(&self.key) {
            Ordering::Less => &mut self.left_child,
            Ordering::Equal => return,
            Ordering::Greater => &mut self.right_child,
        };

        match child {
            Some(node) => node.insert(key, value),
            None => *child = Some(Box::new(Node::new(key, value))),
        }
    }
    fn find(&self, key: i32) -> Option<i32> {
        let child = match key.cmp(&self.key) {
            Ordering::Less => &self.left_child,
            Ordering::Equal => return Some(self.value),
            Ordering::Greater => &self.right_child,
        };

        match child {
            Some(node) => node.find(key),
            None => None,
        }
    }
    fn erase(mut this: Box<Node>, key: i32) -> Option<Box<Node>> {
        match key.cmp(&this.key) {
            Ordering::Less => {
                if let Some(left_child) = this.left_child.take() {
                    this.left_child = Self::erase(left_child, key)
                }
                Some(this)
            }
            Ordering::Equal => match (this.left_child.take(), this.right_child.take()) {
                (Some(_left_child), Some(right_child)) => {
                    let rigth_min_key = right_child.min();
                    let mut new_node = Box::new(Node::new(rigth_min_key, this.value));
                    new_node.left_child = this.left_child;
                    new_node.right_child = Self::erase(right_child, rigth_min_key);
                    Some(new_node)
                }
                (Some(left_child), None) => Some(left_child),
                (None, Some(right_child)) => Some(right_child),
                (None, None) => None,
            },
            Ordering::Greater => {
                if let Some(right_child) = this.right_child.take() {
                    this.right_child = Self::erase(right_child, key);
                }
                Some(this)
            }
        }
    }
    fn min(&self) -> i32 {
        if let Some(left_child) = &self.left_child {
            return left_child.min()
        };
        self.key
    }
}

#[cfg(test)]
mod avl_tests {
    use super::*;

    #[test]
    fn insert_find_one_elem() {
        let mut avl = AVL::new();
        avl.insert(1, 1);
        assert_eq!(avl.find(1).unwrap(), 1);
    }
    #[test]
    fn insert_erase_find_one_elem() {
        let mut avl = AVL::new();
        avl.insert(1, 1);
        avl.erase(1);
        assert_eq!(avl.find(1), None, "kek");
    }
    #[test]
    fn insert_erase_find() {
        let mut avl = AVL::new();
        let keys = vec![1, 0, -1, 1000, 2, -1000];
        for key in &keys {
            avl.insert(*key, *key);
        }
        for key in &keys {
            avl.erase(*key);
        }
        for key in &keys {
            assert_eq!(avl.find(*key), None);
        }
    }
}
