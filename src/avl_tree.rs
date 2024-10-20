use std::cmp::Ordering;
#[allow(dead_code)]
struct Node {
    key: i32,
    value: i32,
    height: i32,
    left_child: Option<Box<Node>>,
    right_child: Option<Box<Node>>,
}

#[allow(dead_code)]
pub struct AVL {
    root: Option<Box<Node>>,
}

#[allow(dead_code)]
impl AVL {
    fn new() -> Self {
        AVL { root: None }
    }
    fn insert(&mut self, key: i32, value: i32) {
        self.root = Some(Node::insert(self.root.take(), key, value).1)
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

#[allow(dead_code)]
impl Node {
    fn new(key: i32, value: i32) -> Self {
        Node {
            key,
            value,
            height: 1,
            left_child: None,
            right_child: None,
        }
    }
    fn update_height(&mut self) {
        let mut max_height = 0;
        if let Some(left_child) = &self.left_child {
            max_height = left_child.height;
        }
        if let Some(right_child) = &self.right_child {
            if right_child.height > max_height {
                max_height = right_child.height;
            }
        }
        self.height = max_height + 1;
    }
    fn get_balance(&self) -> i32 {
        match (&self.left_child, &self.right_child) {
            (Some(left_child), Some(right_child)) => left_child.height - right_child.height,
            (Some(left_child), None) => left_child.height,
            (None, Some(right_child)) => -right_child.height,
            (None, None) => 0,
        }
    }
    fn rotate_left(mut this: Box<Node>) -> Box<Node> {
        if this.right_child.is_none() {
            return this;
        }
        let mut right_node = this.right_child.take().unwrap();
        this.right_child = right_node.left_child;
        this.update_height();
        right_node.left_child = Some(this);
        right_node.update_height();
        right_node
    }
    fn rotate_right(mut this: Box<Node>) -> Box<Node> {
        if this.left_child.is_none() {
            return this;
        }
        let mut left_node = this.left_child.take().unwrap();
        this.left_child = left_node.right_child;
        this.update_height();
        left_node.right_child = Some(this);
        left_node.update_height();
        left_node
    }
    fn big_rotate_left(mut this: Box<Node>) -> Box<Node> {
        this.right_child = Some(Self::rotate_right(this.right_child.unwrap()));
        Self::rotate_left(this)
    }
    fn big_rotate_right(mut this: Box<Node>) -> Box<Node> {
        this.left_child = Some(Self::rotate_left(this.left_child.unwrap()));
        Self::rotate_right(this)
    }
    fn balance(this: Box<Node>) -> Box<Node> {
        match this.get_balance() {
            -2 => {
                if this.right_child.as_ref().unwrap().get_balance() <= 0 {
                    return Self::rotate_left(this);
                }
                Self::big_rotate_left(this)
            }
            2 => {
                if this.left_child.as_ref().unwrap().get_balance() > 0 {
                    return Self::rotate_right(this);
                }
                Self::big_rotate_right(this)
            }
            _ => this,
        }
    }
    fn insert(this: Option<Box<Node>>, key: i32, value: i32) -> (bool, Box<Node>) {
        if this.is_none() {
            return (true, Box::new(Self::new(key, value)));
        }
        let mut node = this.unwrap();
        let mut is_inserted = false;
        match key.cmp(&node.key) {
            Ordering::Less => {
                let result = Self::insert(node.left_child, key, value);
                node.left_child = Some(result.1);
                is_inserted = result.0;
            }
            Ordering::Equal => {
                node.value = value;
            }
            Ordering::Greater => {
                let result = Self::insert(node.right_child, key, value);
                node.right_child = Some(result.1);
                is_inserted = result.0;
            }
        };
        node.update_height();
        (is_inserted, Self::balance(node))
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
                    this.left_child = Self::erase(left_child, key);
                    this.update_height();
                }
                Some(Node::balance(this))
            }
            Ordering::Equal => match (this.left_child.take(), this.right_child.take()) {
                (Some(_left_child), Some(right_child)) => {
                    let right_min = right_child.min();
                    this.value = right_min.value;
                    this.key = right_min.key;
                    this.right_child = Node::erase(right_child, this.key);
                    this.update_height();
                    Some(Node::balance(this))
                }
                (Some(left_child), None) => Some(left_child),
                (None, Some(right_child)) => Some(right_child),
                (None, None) => None,
            },
            Ordering::Greater => {
                if let Some(right_child) = this.right_child.take() {
                    this.right_child = Self::erase(right_child, key);
                    this.update_height();
                }
                Some(Node::balance(this))
            }
        }
    }
    fn min(&self) -> &Self {
        if let Some(left_child) = &self.left_child {
            return left_child.min();
        };
        self
    }
}

#[cfg(test)]
mod avl_tests {
    use super::*;

    fn get_height(avl_node: &Node) -> i32 {
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

    fn get_balance(avl_node: &Node) -> i32 {
        let mut balance = 0;
        if let Some(left_child) = &avl_node.left_child {
            balance += get_height(&left_child);
        }
        if let Some(right_child) = &avl_node.right_child {
            balance -= get_height(&right_child);
        }
        balance
    }

    fn check_correctness(avl_node: &Node) -> bool {
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
        assert_eq!(avl.find(1).unwrap(), 1);
    }
    #[test]
    fn insert_erase_find_one_elem() {
        let mut avl = AVL::new();
        avl.insert(1, 1);
        avl.erase(1);
        assert_eq!(avl.find(1), None);
    }
    #[test]
    fn insert_ordered_sequence() {
        let mut avl = AVL::new();
        for i in 1..100 {
            avl.insert(i, i);
        }
        for i in 1..100 {
            assert_ne!(avl.find(i), None, "{} not found!", i);
        }
        assert!(check_correctness(avl.root.as_ref().unwrap()));
    }
    #[test]
    fn insert_reverse_ordered_sequence() {
        let mut avl = AVL::new();
        for i in (1..100).rev() {
            avl.insert(i, i);
            println!();
        }
        for i in 1..100 {
            assert_ne!(avl.find(i), None, "{} not found!", i);
        }
        assert!(check_correctness(avl.root.as_ref().unwrap()));
    }
    #[test]
    fn insert_erase_find() {
        let mut avl = AVL::new();
        for key in 1..100 {
            avl.insert(key, key);
        }
        for key in 1..100 {
            avl.erase(key);
        }
        for key in 1..100 {
            assert_eq!(avl.find(key), None);
        }
    }
}
