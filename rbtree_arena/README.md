A cache friendly red black tree implementation that allocates a single vector of nodes.

Example:

```rust
use rbtree_arena::RedBlackTree;

let mut tree = RedBlackTree::with_capacity(4);
tree.set(100, "very")?;
tree.set(50, "Trees")?;
tree.set(75, "are")?;
tree.set(150, "cool!")?;

for (k, v) in tree {
    println!("{}: {}", k, v);
}
```

Outputs:

```
50: Trees
75: are
100: very
150: cool!
```
