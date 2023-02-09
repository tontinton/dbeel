use core::fmt::Debug;
use std::{alloc::Layout, cmp::Ordering, marker, ops::Index};

#[derive(Debug, PartialEq)]
enum Color {
    Red,
    Black,
}

#[derive(Debug)]
struct Node<K: Ord, V> {
    color: Color,
    left: NodePtr<K, V>,
    right: NodePtr<K, V>,
    parent: NodePtr<K, V>,
    key: K,
    value: V,
}

impl<K: Ord, V> Node<K, V> {
    fn new(key: K, value: V, color: Color) -> Self {
        Self::new_with_parent(key, value, color, NodePtr::null())
    }

    fn new_with_parent(
        key: K,
        value: V,
        color: Color,
        parent: NodePtr<K, V>,
    ) -> Self {
        Self {
            color,
            left: NodePtr::null(),
            right: NodePtr::null(),
            parent,
            key,
            value,
        }
    }
}

#[derive(Debug)]
struct NodePtr<K: Ord, V>(*mut Node<K, V>);

impl<K: Ord, V> Clone for NodePtr<K, V> {
    fn clone(&self) -> NodePtr<K, V> {
        NodePtr(self.0)
    }
}

impl<K: Ord, V> Copy for NodePtr<K, V> {}

impl<K: Ord, V> Ord for NodePtr<K, V> {
    fn cmp(&self, other: &NodePtr<K, V>) -> Ordering {
        unsafe { (*self.0).key.cmp(&(*other.0).key) }
    }
}

impl<K: Ord, V> PartialOrd for NodePtr<K, V> {
    fn partial_cmp(&self, other: &NodePtr<K, V>) -> Option<Ordering> {
        unsafe { Some((*self.0).key.cmp(&(*other.0).key)) }
    }
}

impl<K: Ord, V> PartialEq for NodePtr<K, V> {
    fn eq(&self, other: &NodePtr<K, V>) -> bool {
        self.0 == other.0
    }
}

impl<K: Ord, V> Eq for NodePtr<K, V> {}

impl<K: Ord, V> NodePtr<K, V> {
    #[inline]
    fn null() -> NodePtr<K, V> {
        NodePtr(std::ptr::null_mut())
    }

    #[inline]
    fn is_null(&self) -> bool {
        self.0.is_null()
    }

    fn as_option(&self) -> Option<&Node<K, V>> {
        if self.0.is_null() {
            None
        } else {
            unsafe { Some(&(*self.0)) }
        }
    }

    fn as_option_mut(&self) -> Option<&mut Node<K, V>> {
        if self.0.is_null() {
            None
        } else {
            unsafe { Some(&mut (*self.0)) }
        }
    }

    #[inline]
    fn unsafe_deref(&self) -> &Node<K, V> {
        unsafe { &(*self.0) }
    }

    #[inline]
    fn unsafe_deref_mut(&self) -> &mut Node<K, V> {
        unsafe { &mut (*self.0) }
    }

    fn unwrap_mut(&self) -> &mut Node<K, V> {
        if self.0.is_null() {
            panic!("called `NodePtr::unwrap_as_mut()` on a null pointer");
        }
        self.unsafe_deref_mut()
    }

    fn find_min(&self) -> NodePtr<K, V> {
        let mut node_ptr = self;
        let mut result = node_ptr;
        while let Some(node) = node_ptr.as_option_mut() {
            result = node_ptr;
            node_ptr = &node.left;
        }
        return *result;
    }

    fn find_max(&self) -> NodePtr<K, V> {
        let mut node_ptr = self;
        let mut result = node_ptr;
        while let Some(node) = node_ptr.as_option_mut() {
            result = node_ptr;
            node_ptr = &node.right;
        }
        return *result;
    }

    fn next(&self) -> NodePtr<K, V> {
        if self.is_null() {
            return *self;
        }

        let right = self.unsafe_deref().right;
        if !right.is_null() {
            right.find_min()
        } else {
            let mut node = self.unsafe_deref();
            loop {
                if let Some(parent) = node.parent.as_option() {
                    if parent.left == *self {
                        return node.parent;
                    }
                    node = parent;
                } else {
                    return NodePtr::null();
                }
            }
        }
    }

    fn prev(&self) -> NodePtr<K, V> {
        if self.is_null() {
            return *self;
        }

        let left = self.unsafe_deref().left;
        if !left.is_null() {
            left.find_min()
        } else {
            let mut node = self.unsafe_deref();
            loop {
                if let Some(parent) = node.parent.as_option() {
                    if parent.right == *self {
                        return node.parent;
                    }
                    node = parent;
                } else {
                    return NodePtr::null();
                }
            }
        }
    }
}

pub struct Iter<'a, K: Ord + 'a, V: 'a> {
    head: NodePtr<K, V>,
    tail: NodePtr<K, V>,
    length: usize,
    _marker: marker::PhantomData<&'a ()>,
}

impl<'a, K: Ord + 'a, V: 'a> Clone for Iter<'a, K, V> {
    fn clone(&self) -> Iter<'a, K, V> {
        Iter {
            head: self.head,
            tail: self.tail,
            length: self.length,
            _marker: self._marker,
        }
    }
}

impl<'a, K: Ord + 'a, V: 'a> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<(&'a K, &'a V)> {
        if self.length == 0 || self.head.is_null() {
            return None;
        }

        let next = self.head.next();
        let (key, value) =
            unsafe { (&(*self.head.0).key, &(*self.head.0).value) };
        self.head = next;
        self.length -= 1;
        Some((key, value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.length, Some(self.length))
    }
}

impl<'a, K: Ord + 'a, V: 'a> DoubleEndedIterator for Iter<'a, K, V> {
    fn next_back(&mut self) -> Option<(&'a K, &'a V)> {
        if self.length == 0 || self.tail.is_null() {
            return None;
        }

        let prev = self.tail.prev();
        let (key, value) =
            unsafe { (&(*self.tail.0).key, &(*self.tail.0).value) };
        self.tail = prev;
        self.length -= 1;
        Some((key, value))
    }
}

pub struct IntoIter<K: Ord, V> {
    head: NodePtr<K, V>,
    tail: NodePtr<K, V>,
    length: usize,

    // Info to free the arena without dropping the key and values inside it, as
    // they should already be freed by iterating them by value.
    arena_ptr: *mut Node<K, V>,
    arena_layout: Layout,
}

impl<K: Ord, V> Drop for IntoIter<K, V> {
    fn drop(&mut self) {
        let arena_ptr = self.arena_ptr;
        let arena_layout = self.arena_layout;

        // Drop keys and values we didn't get to iterate.
        for (_, _) in self {}

        // Free the arena's memory.
        unsafe {
            std::alloc::dealloc(arena_ptr as *mut u8, arena_layout);
        }
    }
}

impl<K: Ord, V> Iterator for IntoIter<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<(K, V)> {
        if self.length == 0 || self.head.is_null() {
            return None;
        }

        let next = self.head.next();
        let (key, value) = unsafe {
            (
                core::ptr::read(&(*self.head.0).key),
                core::ptr::read(&(*self.head.0).value),
            )
        };
        self.head = next;
        self.length -= 1;
        Some((key, value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.length, Some(self.length))
    }
}

impl<K: Ord, V> DoubleEndedIterator for IntoIter<K, V> {
    fn next_back(&mut self) -> Option<(K, V)> {
        if self.length == 0 || self.tail.is_null() {
            return None;
        }

        let prev = self.tail.prev();
        let (key, value) = unsafe {
            (
                core::ptr::read(&(*self.tail.0).key),
                core::ptr::read(&(*self.tail.0).value),
            )
        };
        self.tail = prev;
        self.length -= 1;
        Some((key, value))
    }
}

pub struct RedBlackTree<K: Ord, V> {
    arena: Vec<Node<K, V>>,
    root: NodePtr<K, V>,
    length: usize,
}

impl<K, V> Debug for RedBlackTree<K, V>
where
    K: Ord + Debug,
    V: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<K, V> PartialEq for RedBlackTree<K, V>
where
    K: Eq + Ord,
    V: PartialEq,
{
    fn eq(&self, other: &RedBlackTree<K, V>) -> bool {
        if self.len() != other.len() {
            return false;
        }

        self.iter()
            .all(|(key, value)| other.get(key).map_or(false, |v| *value == *v))
    }
}

impl<K, V> Eq for RedBlackTree<K, V>
where
    K: Eq + Ord,
    V: Eq,
{
}

impl<'a, K, V> Index<&'a K> for RedBlackTree<K, V>
where
    K: Ord,
{
    type Output = V;

    fn index(&self, index: &K) -> &V {
        self.get(index).expect("key not found")
    }
}

impl<K: Ord, V> IntoIterator for RedBlackTree<K, V> {
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(mut self) -> IntoIter<K, V> {
        let length = self.len();
        let head = self.root.find_min();
        let tail = self.root.find_max();

        self.clear_no_dealloc();

        let arena_ptr = self.arena.as_mut_ptr();
        let arena_layout = Layout::from_size_align(
            std::mem::size_of_val(&self.arena),
            std::mem::align_of::<Vec<Node<K, V>>>(),
        )
        .unwrap();

        std::mem::forget(self.arena);

        IntoIter {
            head,
            tail,
            length,
            arena_ptr,
            arena_layout,
        }
    }
}

impl<K: Ord, V> RedBlackTree<K, V> {
    pub fn new() -> Self {
        Self {
            arena: Vec::new(),
            root: NodePtr::null(),
            length: 0,
        }
    }

    fn clear_no_dealloc(&mut self) {
        self.root = NodePtr::null();
        self.length = 0;
    }

    pub fn clear(&mut self) {
        self.clear_no_dealloc();
        self.arena = Vec::new();
    }

    pub fn iter(&self) -> Iter<K, V> {
        Iter {
            head: self.root.find_min(),
            tail: self.root.find_max(),
            length: self.len(),
            _marker: marker::PhantomData,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        return self.length;
    }

    fn alloc_node(&mut self, node: Node<K, V>) -> NodePtr<K, V> {
        self.length += 1;
        self.arena.push(node);
        unsafe { NodePtr(self.arena.as_mut_ptr().add(self.arena.len() - 1)) }
    }

    fn find_node(&self, key: &K) -> NodePtr<K, V> {
        let mut node_ptr = self.root;

        loop {
            match node_ptr.as_option_mut() {
                Some(node) => {
                    let next = match key.cmp(&node.key) {
                        Ordering::Less => &mut node.left,
                        Ordering::Greater => &mut node.right,
                        Ordering::Equal => return node_ptr,
                    };
                    node_ptr = *next;
                }
                None => break,
            }
        }

        NodePtr::null()
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let node_ptr = self.find_node(key);
        if node_ptr.is_null() {
            None
        } else {
            unsafe { Some(&(*node_ptr.0).value) }
        }
    }

    pub fn contains_key(&self, k: &K) -> bool {
        let node_ptr = self.find_node(k);
        return !node_ptr.is_null();
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let mut node_ptr = self.root;
        if node_ptr.is_null() {
            self.root = self.alloc_node(Node::new(key, value, Color::Black));
            return None;
        }

        loop {
            let node = node_ptr.unsafe_deref_mut();
            let (left, next_ptr) = match key.cmp(&node.key) {
                Ordering::Less => (true, node.left),
                Ordering::Greater => (false, node.right),
                Ordering::Equal => {
                    return Some(std::mem::replace(&mut node.value, value));
                }
            };

            if next_ptr.is_null() {
                let new_node_ptr = self.alloc_node(Node::new_with_parent(
                    key,
                    value,
                    Color::Red,
                    node_ptr,
                ));

                if left {
                    node.left = new_node_ptr;
                } else {
                    node.right = new_node_ptr;
                }

                self.insert_fixup(new_node_ptr);
                return None;
            }

            node_ptr = next_ptr;
        }
    }

    fn insert_fixup(&mut self, inserted_node_ptr: NodePtr<K, V>) {
        let mut node_ptr = inserted_node_ptr;
        while node_ptr != self.root {
            let node = node_ptr.unwrap_mut();
            let parent_ptr = node.parent;
            let parent = parent_ptr.unwrap_mut();
            if parent.color == Color::Black {
                break;
            }

            let grand_parent_ptr = parent.parent;
            let mut grand_parent = grand_parent_ptr.unwrap_mut();

            if parent_ptr == grand_parent.left {
                let uncle_ptr = grand_parent.right;
                let rotate = uncle_ptr.is_null()
                    || uncle_ptr.unsafe_deref().color == Color::Black;
                if rotate {
                    if node_ptr == parent.right {
                        self.rotate_left(parent_ptr);
                    }

                    node.color = Color::Black;
                    grand_parent.color = Color::Red;
                    node_ptr = parent_ptr;
                    self.rotate_right(grand_parent_ptr);
                } else {
                    let uncle = uncle_ptr.unsafe_deref_mut();
                    parent.color = Color::Black;
                    uncle.color = Color::Black;
                    grand_parent.color = Color::Red;
                    node_ptr = grand_parent_ptr;
                }
            } else {
                let uncle_ptr = grand_parent.left;
                let rotate = uncle_ptr.is_null()
                    || uncle_ptr.unsafe_deref().color == Color::Black;
                if rotate {
                    if node_ptr == parent.left {
                        self.rotate_right(parent_ptr);
                    }

                    node.color = Color::Black;
                    grand_parent.color = Color::Red;
                    node_ptr = parent_ptr;
                    self.rotate_left(grand_parent_ptr);
                } else {
                    let uncle = uncle_ptr.unsafe_deref_mut();
                    parent.color = Color::Black;
                    uncle.color = Color::Black;
                    grand_parent.color = Color::Red;
                    node_ptr = grand_parent_ptr;
                }
            }
        }
        self.root.unwrap_mut().color = Color::Black;
    }

    fn rotate_left(&mut self, node_ptr: NodePtr<K, V>) {
        let node = node_ptr.unwrap_mut();
        let right_ptr = node.right;
        let right = right_ptr.unwrap_mut();

        node.right = right.left;
        if let Some(left) = right.left.as_option_mut() {
            left.parent = node_ptr;
        }

        right.parent = node.parent;

        match node.parent.as_option_mut() {
            Some(parent) => {
                if node_ptr == parent.left {
                    parent.left = right_ptr;
                } else {
                    parent.right = right_ptr;
                }
            }
            None => self.root = right_ptr,
        }

        right.left = node_ptr;
        node.parent = right_ptr;
    }

    fn rotate_right(&mut self, node_ptr: NodePtr<K, V>) {
        let node = node_ptr.unwrap_mut();
        let left_ptr = node.left;
        let left = left_ptr.unwrap_mut();

        node.left = left.right;
        if let Some(right) = left.right.as_option_mut() {
            right.parent = node_ptr;
        }

        left.parent = node.parent;

        match node.parent.as_option_mut() {
            Some(parent) => {
                if node_ptr == parent.right {
                    parent.left = left_ptr;
                } else {
                    parent.right = left_ptr;
                }
            }
            None => self.root = left_ptr,
        }

        left.right = node_ptr;
        node.parent = left_ptr;
    }
}

#[cfg(test)]
mod tests {
    use super::RedBlackTree;

    #[test]
    fn insert_int() {
        let mut tree = RedBlackTree::new();
        assert_eq!(tree.len(), 0);
        tree.insert(1, 2);
        assert_eq!(tree.len(), 1);
        tree.insert(2, 4);
        assert_eq!(tree.len(), 2);
        tree.insert(2, 6);
        assert_eq!(tree.len(), 2);
        assert!(tree.contains_key(&1));
        assert!(!tree.contains_key(&100));
        assert_eq!(tree.get(&1), Some(&2));
        assert_eq!(tree.get(&2), Some(&6));
        assert_eq!(tree.get(&3), None);
    }

    #[test]
    fn insert_str() {
        let mut tree = RedBlackTree::new();
        tree.insert("B", "are");
        tree.insert("A", "B");
        tree.insert("A", "Trees");
        tree.insert("C", "cool");
        assert_eq!(tree.len(), 3);
        assert!(tree.contains_key(&"C"));
        assert!(!tree.contains_key(&"nope"));
        assert_eq!(tree.get(&"A"), Some(&"Trees"));
        assert_eq!(tree[&"B"], "are");
        assert_eq!(tree.get(&"C"), Some(&"cool"));
        assert_eq!(tree.get(&"D"), None);
    }

    #[test]
    fn iter() {
        let mut tree = RedBlackTree::new();
        tree.insert(100, "c");
        tree.insert(50, "a");
        tree.insert(75, "b");
        tree.insert(150, "d");
        let mut iter = tree.iter();
        assert_eq!(iter.next(), Some((&50, &"a")));
        assert_eq!(iter.next(), Some((&75, &"b")));
        assert_eq!(iter.next(), Some((&100, &"c")));
        assert_eq!(iter.next(), Some((&150, &"d")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn iter_reverse() {
        let mut tree = RedBlackTree::new();
        tree.insert(100, "c");
        tree.insert(50, "a");
        tree.insert(75, "b");
        tree.insert(150, "d");
        let mut iter = tree.iter().rev();
        assert_eq!(iter.next(), Some((&150, &"d")));
        assert_eq!(iter.next(), Some((&100, &"c")));
        assert_eq!(iter.next(), Some((&75, &"b")));
        assert_eq!(iter.next(), Some((&50, &"a")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn into_iter() {
        let mut tree = RedBlackTree::new();
        tree.insert(100, "c");
        tree.insert(50, "a");
        tree.insert(75, "b");
        tree.insert(150, "d");
        let mut iter = tree.into_iter();
        assert_eq!(iter.next(), Some((50, "a")));
        assert_eq!(iter.next(), Some((75, "b")));
        assert_eq!(iter.next(), Some((100, "c")));
        assert_eq!(iter.next(), Some((150, "d")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn into_iter_reverse() {
        let mut tree = RedBlackTree::new();
        tree.insert(100, "c");
        tree.insert(50, "a");
        tree.insert(75, "b");
        tree.insert(150, "d");
        let mut iter = tree.into_iter().rev();
        assert_eq!(iter.next(), Some((150, "d")));
        assert_eq!(iter.next(), Some((100, "c")));
        assert_eq!(iter.next(), Some((75, "b")));
        assert_eq!(iter.next(), Some((50, "a")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn into_iter_for_loop_no_crash() {
        struct User {
            // Test there is no double free on a dynamically allocated struct.
            _name: String,
            _age: u8,
        }

        let mut tree = RedBlackTree::new();
        tree.insert(
            "id1",
            User {
                _name: "John Doe".to_string(),
                _age: 123,
            },
        );
        tree.insert(
            "id2",
            User {
                _name: "Tony Solomonik".to_string(),
                _age: 24,
            },
        );

        for (_, _) in tree {}
    }

    #[test]
    fn clear() {
        let mut tree = RedBlackTree::new();
        tree.insert(100, "c");
        tree.insert(50, "a");
        tree.insert(75, "b");
        tree.insert(150, "d");
        tree.clear();
        assert_eq!(tree.len(), 0);
    }

    #[test]
    fn equality() {
        let mut tree1 = RedBlackTree::new();
        tree1.insert(100, "c");
        tree1.insert(50, "a");
        tree1.insert(75, "b");
        tree1.insert(150, "d");

        let mut tree2 = RedBlackTree::new();
        tree2.insert(150, "d");
        tree2.insert(50, "a");
        tree2.insert(100, "c");
        tree2.insert(75, "b");

        assert_eq!(tree1, tree2);
    }
}
