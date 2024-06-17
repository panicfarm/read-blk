use bitcoin::BlockHash;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

//#[derive(Debug, PartialEq, Eq, Hash, Clone)]
//pub struct BlockHash(String);

#[derive(Debug, Clone)]
pub struct BlockInfo {
    pub hash: BlockHash,
    prev_hash: BlockHash,
}

#[derive(Debug)]
pub struct BlockCache {
    pending_full_blocks: HashMap<BlockHash, bitcoin::block::Block>,
    out_of_order_blocks: HashMap<BlockHash, Vec<BlockInfo>>,
    staged_blocks: StagedBlocks,
}

#[derive(Debug, Clone)]
struct TreeNode {
    block_info: BlockInfo,
    parent: Option<BlockHash>,
    children: HashSet<BlockHash>,
    orig_level: u32,
}

#[derive(Debug)]
struct StagedBlocks {
    tree_root: Option<BlockHash>,
    nodes: HashMap<BlockHash, TreeNode>,
    tree_depth: u32,
    root_removed_cnt: u32,
}

impl BlockInfo {
    pub fn new(hash: &BlockHash, prev_hash: &BlockHash) -> Self {
        BlockInfo {
            hash: hash.clone(),
            prev_hash: prev_hash.clone(),
        }
    }
}

impl TreeNode {
    fn new(block_info: BlockInfo) -> Self {
        TreeNode {
            block_info,
            parent: None,
            children: HashSet::new(),
            orig_level: 0,
        }
    }
}

impl BlockCache {
    pub fn new() -> Self {
        BlockCache {
            pending_full_blocks: HashMap::new(),
            out_of_order_blocks: HashMap::new(),
            staged_blocks: StagedBlocks::new(),
        }
    }

    pub fn add_block(&mut self, block: bitcoin::block::Block) {
        let block_info = BlockInfo::new(&block.block_hash(), &block.header.prev_blockhash);
        self.pending_full_blocks.insert(block_info.hash, block);
        self.add_block_info(&block_info);
    }

    fn add_block_info(&mut self, block_info: &BlockInfo) {
        if self.staged_blocks.tree_root.is_none()
            || self
                .staged_blocks
                .nodes
                .get(&block_info.prev_hash)
                .is_some()
        {
            self.staged_blocks.add_block_info(&block_info);
            //dbg!("added {}", &block_info.hash.to_string());
            self.move_out_of_order_blocks_to_staged(&block_info.hash);
        } else {
            self.out_of_order_blocks
                .entry(block_info.prev_hash.clone())
                .or_default()
                .push(block_info.clone());
        }
    }

    fn move_out_of_order_blocks_to_staged(&mut self, prev_hash: &BlockHash) {
        if let Some(block_info_vec) = self.out_of_order_blocks.remove(prev_hash) {
            for block_info in block_info_vec {
                self.staged_blocks.add_block_info(&block_info);
                //dbg!("added {}", &block_info.hash.to_string());
                self.move_out_of_order_blocks_to_staged(&block_info.hash);
            }
        }
    }

    /// when the depth in the whole tree reaches threshold, the root block_info in the tree can migrate to the main chain
    pub fn remove_block_if_ready(&mut self, depth_threshold: u32) -> Option<bitcoin::Block> {
        if let Some(block_info) = self
            .staged_blocks
            .remove_block_info_if_ready(depth_threshold)
        {
            self.pending_full_blocks.remove(&block_info.hash)
        } else {
            None
        }
    }
}

impl StagedBlocks {
    fn new() -> Self {
        StagedBlocks {
            tree_root: None,
            nodes: HashMap::new(),
            tree_depth: 0,
            root_removed_cnt: 0,
        }
    }

    fn add_block_info(&mut self, block_info: &BlockInfo) {
        let mut new_node = TreeNode::new(block_info.clone());
        if self.tree_root.is_none() {
            // if this the tree is empty, this is the first root node
            new_node.orig_level = 1;
            self.tree_root = Some(block_info.hash.clone());
            self.nodes.insert(block_info.hash.clone(), new_node);
        } else {
            let parent_node = self
                .nodes
                .get_mut(&new_node.block_info.prev_hash)
                .expect("parent node expected");
            new_node.orig_level = parent_node.orig_level + 1;
            new_node.parent = Some(parent_node.block_info.hash.clone());
            parent_node.children.insert(block_info.hash.clone());
            let depth = new_node.orig_level - self.root_removed_cnt;
            self.nodes.insert(block_info.hash.clone(), new_node);
            if self.tree_depth < depth {
                self.tree_depth = depth;
            }
        }
    }

    fn remove_block_info_if_ready(&mut self, depth_threshold: u32) -> Option<BlockInfo> {
        if self.tree_depth < depth_threshold {
            return None;
        }

        let root_hash = self.tree_root.as_ref().expect("root hash expected");
        let root_node = self.nodes.remove(root_hash).expect("root node expected");
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
            new_root_node_opt = self.nodes.get_mut(winning_child_hash);

            let mut losing_children = root_node.children.clone();
            losing_children.remove(winning_child_hash);
            losing_children_opt = Some(losing_children);
        } else {
            assert_eq!(root_node.children.len(), 1);
            for child_hash in root_node.children.iter() {
                new_root_node_opt = self.nodes.get_mut(child_hash);
            }
        }

        let new_root_node = new_root_node_opt.expect("new root node expected");
        new_root_node.parent = None;
        self.tree_root = Some(new_root_node.block_info.hash.clone());
        self.tree_depth -= 1;
        self.root_removed_cnt += 1;

        if let Some(losing_children) = losing_children_opt {
            // remove losing branches
            self.purge_nodes(&losing_children);
        }

        Some(root_node.block_info)
    }

    fn purge_nodes(&mut self, block_hashes: &HashSet<BlockHash>) {
        for hash in block_hashes.iter() {
            let node = self.nodes.remove(hash).expect("node expected");
            self.purge_nodes(&node.children);
        }
    }

    fn calculate_depth_from_node(&self, block_hash: &BlockHash) -> u32 {
        let mut max_depth = 0;
        let node = self.nodes.get(block_hash).expect("node expected");
        for child_hash in node.children.iter() {
            let depth = self.calculate_depth_from_node(child_hash);
            if depth > max_depth {
                max_depth = depth;
            }
        }
        max_depth + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_block_hash(hash: &str) -> BlockHash {
        BlockHash::from_str(&hash.repeat(64)).unwrap()
    }

    fn create_block_info(hash: &str, prev_hash: &str) -> BlockInfo {
        BlockInfo {
            hash: create_block_hash(&hash),
            prev_hash: create_block_hash(&prev_hash),
        }
    }

    #[test]
    fn test() {
        let mut block_cache = BlockCache::new();

        // Create an unbalanced tree with branches and a deepest branch of 10 levels
        /*
                              0
                             / \
                            1   2
                           /   / \
                          3   4   5
                         /   /     \
                        6   7       8
                       /     \
                      9       A
                               \
                                B
                                 \
                                  C
        */
        let blocks = vec![
            create_block_info("0", "0"), // Level 0
            create_block_info("8", "5"), // Level 4, out of order
            create_block_info("4", "2"), // Level 2, out of order
            create_block_info("5", "2"), // Level 2, out of order
            create_block_info("1", "0"), // Level 1
            create_block_info("2", "0"), // Level 1
            create_block_info("A", "7"), // Level 4, out of order
            create_block_info("7", "4"), // Level 3, out of order
            create_block_info("9", "6"), // Level 4, out of order
            create_block_info("3", "1"), // Level 2
            create_block_info("6", "3"), // Level 3
            create_block_info("B", "A"), // Level 5
            create_block_info("C", "B"), // Level 6
        ];

        // Add blocks to the tree
        for block_info in &blocks {
            block_cache.add_block_info(block_info);
        }
        //dbg!(&block_cache);
        assert!(block_cache.out_of_order_blocks.is_empty());
        assert_eq!(block_cache.staged_blocks.tree_depth, 7);

        let expected_roots = vec!["0", "2", "4"];
        for expected_root in expected_roots {
            if let Some(removed_block) = block_cache.staged_blocks.remove_block_info_if_ready(4) {
                assert_eq!(
                    &removed_block.hash,
                    &create_block_hash(expected_root),
                    "expected root: {}, but got: {:?}",
                    expected_root,
                    removed_block.hash
                );
            } else {
                println!("expected to remove root but none was removed");
            }
        }
        assert_eq!(block_cache.staged_blocks.tree_depth, 4);
        assert_eq!(block_cache.staged_blocks.root_removed_cnt, 3);
        let root_hash = block_cache
            .staged_blocks
            .tree_root
            .as_ref()
            .expect("root hash expected");
        assert_eq!(
            root_hash,
            &create_block_hash("7"),
            "expected root: 7, but got: {:?}",
            root_hash
        );
        let node = block_cache
            .staged_blocks
            .nodes
            .get(&create_block_hash("A"))
            .expect("node expected");
        assert_eq!(node.orig_level, 5);
        assert_eq!(
            node.parent.as_ref().expect("parent expected"),
            &create_block_hash("7")
        );
        let mut children = HashSet::new();
        children.insert(create_block_hash("B"));
        assert_eq!(&node.children, &children);
        //dbg!(&block_cache);
    }
}
