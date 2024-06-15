use std::collections::{HashMap, HashSet};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BlockHash(String);

#[derive(Debug, Clone)]
struct Block {
    height: u32,
    hash: BlockHash,
    prev_hash: BlockHash,
}

#[derive(Debug)]
struct BlockCache {
    out_of_order_blocks: HashMap<BlockHash, Vec<Block>>,
    staged_blocks: StagedBlocks,
}

#[derive(Debug, Clone)]
struct TreeNode {
    block: Block,
    parent: Option<BlockHash>,
    children: HashSet<BlockHash>,
}

#[derive(Debug)]
struct StagedBlocks {
    block_tree_root: Option<BlockHash>,
    block_nodes: HashMap<BlockHash, TreeNode>,
    block_tree_depth: u16,
}

impl TreeNode {
    fn new(block: Block) -> Self {
        TreeNode {
            block,
            parent: None,
            children: HashSet::new(),
        }
    }
}

impl BlockCache {
    fn new() -> Self {
        BlockCache {
            out_of_order_blocks: HashMap::new(),
            staged_blocks: StagedBlocks::new(),
        }
    }

    fn add_block(&mut self, block: &Block) {
        if self.staged_blocks.block_tree_root.is_none()
            || self
                .staged_blocks
                .block_nodes
                .get(&block.prev_hash)
                .is_some()
        {
            self.staged_blocks.add_block(block);
            //dbg!("added {}", &block.hash.0);
            self.move_out_of_order_blocks_to_staged(&block.hash);
        } else {
            self.out_of_order_blocks
                .entry(block.prev_hash.clone())
                .or_default()
                .push(block.clone());
        }
    }

    fn move_out_of_order_blocks_to_staged(&mut self, prev_hash: &BlockHash) {
        if let Some(block_vec) = self.out_of_order_blocks.remove(prev_hash) {
            for block in block_vec {
                self.staged_blocks.add_block(&block);
                //dbg!("added {}", &block.hash.0);
                self.move_out_of_order_blocks_to_staged(&block.hash);
            }
        }
    }

    fn remove_root_if_ready(&mut self, depth_threshold: u16) -> Option<Block> {
        // when the depth in the whole tree reaches 100, the root block in the tree can migrate to the main chain
        if self.staged_blocks.block_tree_depth >= depth_threshold {
            let block = self.staged_blocks.remove_root();
            Some(block)
        } else {
            None
        }
    }
}

impl StagedBlocks {
    fn new() -> Self {
        StagedBlocks {
            block_tree_root: None,
            block_nodes: HashMap::new(),
            block_tree_depth: 0,
        }
    }

    fn add_block(&mut self, block: &Block) {
        let mut new_node = TreeNode::new(block.clone());

        // if this the tree is empty, this is the first root node
        if self.block_tree_root.is_none() {
            self.block_tree_root = Some(block.hash.clone());
            self.block_nodes.insert(block.hash.clone(), new_node);
        } else if let Some(parent_node) = self.block_nodes.get_mut(&new_node.block.prev_hash) {
            new_node.parent = Some(parent_node.block.hash.clone());
            parent_node.children.insert(block.hash.clone());
            self.block_nodes.insert(block.hash.clone(), new_node);

            let depth = self.calculate_depth_to_node(&block.hash);
            if self.block_tree_depth < depth {
                self.block_tree_depth = depth;
            }
        } else {
            // assert that the tree already has a block with prev_hash, i.e., a parent for the new node
            //TODO assert error
        }
    }

    fn remove_root(&mut self) -> Block {
        let root_hash = self.block_tree_root.as_ref().expect("root hash expected");
        let root_node = self
            .block_nodes
            .remove(root_hash)
            .expect("root node expected");
        let mut new_root_node_opt = None;
        let mut losing_children_opt = None;
        if root_node.children.len() > 1 {
            // if the root has more than one child, leave only the child that has the deepest subtree under it
            let mut child_hash_with_deepest_subtree = None;
            let mut max_subtree_depth = 0;
            for child_hash in root_node.children.iter() {
                let depth = self.calculate_depth_from_node(child_hash);
                if depth > max_subtree_depth {
                    max_subtree_depth = depth;
                    child_hash_with_deepest_subtree = Some(child_hash);
                }
            }
            let winning_child_hash = child_hash_with_deepest_subtree.expect("child hash expected");
            new_root_node_opt = self.block_nodes.get_mut(&winning_child_hash);

            let mut losing_children = root_node.children.clone();
            losing_children.remove(winning_child_hash);
            losing_children_opt = Some(losing_children);
        } else {
            assert_eq!(root_node.children.len(), 1);
            for child_hash in root_node.children.iter() {
                new_root_node_opt = self.block_nodes.get_mut(&child_hash);
            }
        }

        let new_root_node = new_root_node_opt.expect("new root node expected");
        new_root_node.parent = None;
        self.block_tree_root = Some(new_root_node.block.hash.clone());
        self.block_tree_depth -= 1;

        if let Some(losing_children) = losing_children_opt {
            // remove losing branches
            self.purge_nodes(&losing_children);
        }

        root_node.block
    }

    fn purge_nodes(&mut self, block_hashes: &HashSet<BlockHash>) {
        for hash in block_hashes.iter() {
            let node = self.block_nodes.remove(&hash).expect("node expected");
            self.purge_nodes(&node.children);
        }
    }

    fn calculate_depth_from_node(&self, block_hash: &BlockHash) -> u16 {
        let mut max_depth = 0;
        let node = self.block_nodes.get(block_hash).expect("node expected");
        for child_hash in node.children.iter() {
            let depth = self.calculate_depth_from_node(child_hash);
            if depth > max_depth {
                max_depth = depth;
            }
        }
        max_depth + 1
    }

    fn calculate_depth_to_node(&self, block_hash: &BlockHash) -> u16 {
        let mut depth = 0;
        let mut node = self.block_nodes.get(block_hash);
        while let Some(n) = node {
            depth += 1;
            if let Some(hash) = &n.parent {
                node = self.block_nodes.get(&hash);
            } else {
                node = None;
            }
        }
        depth
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_block(height: u32, hash: &str, prev_hash: &str) -> Block {
        Block {
            height,
            hash: BlockHash(hash.to_string()),
            prev_hash: BlockHash(prev_hash.to_string()),
        }
    }

    #[test]
    fn test() {
        let mut block_cache = BlockCache::new();

        // Create an unbalanced tree with branches and a deepest branch of 10 levels
        /*
                              A
                             / \
                            B   C
                           /   / \
                          D   E   F
                         /   /     \
                        G   H       J
                       /     \
                      I       K
                               \
                                L
                                 \
                                  M
        */
        let blocks = vec![
            create_block(0, "A", ""),  // Level 0
            create_block(4, "J", "F"), // Level 4, out of order
            create_block(2, "E", "C"), // Level 2, out of order
            create_block(2, "F", "C"), // Level 2, out of order
            create_block(1, "B", "A"), // Level 1
            create_block(1, "C", "A"), // Level 1
            create_block(4, "K", "H"), // Level 4, out of order
            create_block(3, "H", "E"), // Level 3, out of order
            create_block(4, "I", "G"), // Level 4, out of order
            create_block(2, "D", "B"), // Level 2
            create_block(3, "G", "D"), // Level 3
            create_block(5, "L", "K"), // Level 5
            create_block(6, "M", "L"), // Level 6
        ];

        // Add blocks to the tree
        for block in &blocks {
            block_cache.add_block(block);
        }
        //dbg!(&block_cache);
        assert!(block_cache.out_of_order_blocks.is_empty());
        assert_eq!(block_cache.staged_blocks.block_tree_depth, 7);

        let expected_roots = vec!["A", "C", "E"];
        for expected_root in expected_roots {
            if let Some(removed_block) = block_cache.remove_root_if_ready(4) {
                assert_eq!(
                    removed_block.hash.0, expected_root,
                    "expected root: {}, but got: {:?}",
                    expected_root, removed_block.hash
                );
            } else {
                println!("expected to remove root but none was removed");
            }
        }
        assert_eq!(block_cache.staged_blocks.block_tree_depth, 4);
        let root_hash = block_cache
            .staged_blocks
            .block_tree_root
            .as_ref()
            .expect("root hash expected");
        assert_eq!(
            root_hash.0, "H",
            "expected root: H, but got: {:?}",
            root_hash
        );
        assert_eq!(
            root_hash.0, "H",
            "expected root: H, but got: {:?}",
            root_hash
        );
        let node = block_cache
            .staged_blocks
            .block_nodes
            .get(&BlockHash("K".to_string()))
            .expect("node expected");
        assert_eq!(
            node.parent.as_ref().expect("parent expected"),
            &BlockHash("H".to_string())
        );
        let mut children = HashSet::new();
        children.insert(BlockHash("L".to_string()));
        assert_eq!(&node.children, &children);
        //dbg!(&block_cache);
    }
}
