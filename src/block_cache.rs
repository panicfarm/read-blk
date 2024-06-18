use bitcoin::BlockHash;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

/*
Before a bitcoin::block::Block can be added to the main chain, the block is added to BlockCache with add_block() method.
While in BlockCache, the block is kept in pending_full_blocks map.
If the block is not out of order, BlockInfo for the block is staged in staged_blocks 'sliding' tree structure.
if the block is out of order, BlockInfo for the block is kept in out_of_order_blocks until the block with hash==prev_hash is staged.
Whenever the staged_blocks tree is deep-enough (e.g., 100 levels deep), the block correspending to the root node's BlockInfo can
migrate to the main chain. Such a block is returned from remove_block_if_ready() method.
When root is removed from the staged_blocks 'slding' tree, potential off-the-root re-org losing branched are purged,
i.e., branches with less work, which is equivalent to keeping the deepest subtree off-the-root.
*/

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
    // orig_level stars from 1 for the first node added to the tree.
    // new node's orig_level is parent node's orig_level+1.
    // new node's depth is calculated as: orig_level - root_removed_cnt.
    orig_level: u32,
}

#[derive(Debug)]
struct StagedBlocks {
    tree_root: Option<BlockHash>,
    nodes: HashMap<BlockHash, TreeNode>,
    // maintained every time a new node is added to the tree
    tree_depth: u32,
    // incremented every time a root node is removed
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

    pub fn pending_cnt(&self) -> usize {
        self.pending_full_blocks.len()
    }

    pub fn staged_cnt(&self) -> usize {
        self.staged_blocks.nodes.len()
    }

    pub fn out_of_order_cnt(&self) -> usize {
        self.out_of_order_blocks.len()
    }

    pub fn add_block(&mut self, block: bitcoin::block::Block) {
        let block_info = BlockInfo::new(&block.block_hash(), &block.header.prev_blockhash);
        self.add_block_impl(&block_info, block);
    }

    fn add_block_impl(&mut self, block_info: &BlockInfo, block: bitcoin::block::Block) {
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
        let (_, block_opt) = self.remove_block_if_ready_impl(depth_threshold);
        block_opt
    }

    fn remove_block_if_ready_impl(
        &mut self,
        depth_threshold: u32,
    ) -> (Option<BlockInfo>, Option<bitcoin::Block>) {
        let (block_info_opt, losing_children_opt) = self
            .staged_blocks
            .remove_block_info_if_ready(depth_threshold);
        if let Some(block_info) = block_info_opt {
            if let Some(losing_children) = losing_children_opt {
                self.purge_losing_blocks(&losing_children);
            }
            let block_opt = self.pending_full_blocks.remove(&block_info.hash);
            (Some(block_info), block_opt)
        } else {
            (None, None)
        }
    }

    // Staging tree's nodes from the losing off-the-removed-root subtrees are removed from the nodes map and
    // the corresponding blocks are removed from the pending blocks map
    fn purge_losing_blocks(&mut self, block_hashes: &HashSet<BlockHash>) {
        for hash in block_hashes.iter() {
            let block = self
                .pending_full_blocks
                .remove(hash)
                .expect("full block expected");
            let node = self
                .staged_blocks
                .nodes
                .remove(hash)
                .expect("node expected");
            //TODO change to logger
            println!(
                "xxx purged losing block {:?} {} header: work {} prev_hash {:?}",
                hash,
                block.bip34_block_height().unwrap_or(0),
                block.header.work(),
                block.header.prev_blockhash
            );
            self.purge_losing_blocks(&node.children);
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

    // A new tree node is created for the provided block_info. If the tree is empty, the new new becomes the root.
    // Otherwise, the new node becomes the child of the node with hash equal to block_info.prev_hash.
    // Tree depth is adjusted if the addition of the new node makese the tree deeper.
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

    // When the depth in the whole tree reaches threshold, the root of the tree is removed and the tree shifts up.
    // The root's child node that has the deepest subtree becomes new root.
    // The block correspnding to the removed root can migrate to the main chain.
    // If the root is removed, returns BlockInfo of the removed root and HashSet of block hashes of the losing children under the root.
    fn remove_block_info_if_ready(
        &mut self,
        depth_threshold: u32,
    ) -> (Option<BlockInfo>, Option<HashSet<BlockHash>>) {
        if self.tree_depth < depth_threshold || self.tree_depth == 0 {
            return (None, None);
        }

        let root_hash = self.tree_root.as_ref().expect("root hash expected");
        let root_node = self.nodes.remove(root_hash).expect("root node expected");
        let mut new_root_node_opt = None;
        let mut losing_children_opt = None;
        let child_cnt = root_node.children.len();
        if child_cnt > 1 {
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
        } else if child_cnt == 1 {
            let child_hash = root_node
                .children
                .iter()
                .last()
                .expect("child hash expected");
            new_root_node_opt = self.nodes.get_mut(child_hash);
        }

        self.tree_depth -= 1;
        self.root_removed_cnt += 1;

        if let Some(new_root_node) = new_root_node_opt {
            new_root_node.parent = None;
            self.tree_root = Some(new_root_node.block_info.hash.clone());
        } else {
            self.tree_root = None;
        }

        (Some(root_node.block_info), losing_children_opt)
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
    use bitcoin::consensus::encode::deserialize;
    use hex_lit::hex;

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
        const BLOCK_HEX: &str = "0200000035ab154183570282ce9afc0b494c9fc6a3cfea05aa8c1add2ecc56490000000038ba3d78e4500a5a7570dbe61960398add4410d278b21cd9708e6d9743f374d544fc055227f1001c29c1ea3b0101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff3703a08601000427f1001c046a510100522cfabe6d6d0000000000000000000068692066726f6d20706f6f6c7365727665726aac1eeeed88ffffffff0100f2052a010000001976a914912e2b234f941f30b18afbb4fa46171214bf66c888ac00000000";
        let dummy_block: bitcoin::Block = deserialize(&hex!(BLOCK_HEX)).unwrap();

        // Add blocks to the tree
        for block_info in &blocks {
            block_cache.add_block_impl(block_info, dummy_block.clone());
        }
        //dbg!(&block_cache);
        assert_eq!(block_cache.staged_blocks.tree_depth, 7);
        assert_eq!(block_cache.staged_cnt(), 13);
        assert_eq!(block_cache.out_of_order_cnt(), 0);

        let expected_roots = vec!["0", "2", "4"];
        for expected_root in expected_roots {
            let (block_info_opt, _block_opt) = block_cache.remove_block_if_ready_impl(4);
            let block_info = block_info_opt.expect("root removal expected");
            assert_eq!(
                &block_info.hash,
                &create_block_hash(expected_root),
                "expected root: {}, but got: {:?}",
                expected_root,
                block_info.hash
            );
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
