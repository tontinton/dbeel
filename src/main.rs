use redblacktree::RedBlackTree;

fn main() {
    let mut tree = RedBlackTree::new();
    tree.insert("B", "are");
    tree.insert("A", "B");
    tree.insert("A", "Trees");
    tree.insert("C", "cool");
    for (_k, v) in tree {
        println!("{}", v);
    }
}
