mod block_cache;

use bitcoin::block::Block;
use bitcoin::consensus::Decodable;
use std::fs::File;
use std::io::Read;
use std::path::Path;

struct Importer {
    block_cache: block_cache::BlockCache,
    prev_block_hash: Option<bitcoin::BlockHash>,
    prev_block_height: u64,
}

fn main() {
    let _dir_path = "/home/ghost/dat/bitcoin/blocks/"; //bitcoin core leveldb
    let dir_path = "/fusionio0/btccore/dat/blocks/";
    let mut file_num = 0; //1328;
    let mut importer = Importer::new();
    loop {
        let file_name = format!("blk{:05}.dat", file_num);
        let file_path = Path::new(dir_path).join(&file_name);
        match File::open(&file_path) {
            Ok(mut file) => {
                let mut contents = Vec::new();
                file.read_to_end(&mut contents).unwrap();
                println!("File {}: {} bytes", file_name, contents.len());
                importer.read_blocks(contents);
            }
            Err(err) => {
                // file not found, assume it's the last file
                println!("err {}", err);
                break;
            }
        }
        file_num += 1;

        /*if file_num == 2 {
            break;
        }*/
    }

    while importer.block_cache.staged_cnt() > 0 {
        importer.import_block_if_ready(0);
    }

    if importer.block_cache.out_of_order_cnt() > 0 {
        println!(
            "!!! WARNING: {} out of order blocks remained",
            importer.block_cache.out_of_order_cnt()
        );
        assert_eq!(
            importer.block_cache.pending_cnt(),
            importer.block_cache.out_of_order_cnt()
        );
    } else {
        assert_eq!(importer.block_cache.pending_cnt(), 0);
    }
}

impl Importer {
    fn new() -> Self {
        Importer {
            block_cache: block_cache::BlockCache::new(),
            prev_block_hash: None,
            prev_block_height: 0,
        }
    }

    fn read_blocks(&mut self, file_bytes: Vec<u8>) {
        let mut i = 0;
        loop {
            if i >= file_bytes.len() {
                break;
            }

            let len = u32::from_le_bytes(file_bytes[i + 4..i + 8].try_into().unwrap()) as usize;
            //println!("read {} {}", i, len);
            if len > 0 {
                let bytes = &file_bytes[i + 8..i + 8 + len];
                assert_eq!(
                    &file_bytes[i..i + 4],
                    &[0xf9, 0xbe, 0xb4, 0xd9],
                    "{}, {}, {}",
                    i,
                    len,
                    hex::encode(&bytes),
                );
                let block = Block::consensus_decode(&mut bytes.to_vec().as_slice()).unwrap();
                println!(
                    "...read block {:?} {} header: work {} prev_hash {:?}",
                    block.block_hash(),
                    block.bip34_block_height().unwrap_or(0),
                    block.header.work(),
                    block.header.prev_blockhash
                );
                self.block_cache.add_block(block);
            }

            i += 8 + len;

            self.import_block_if_ready(100);
        }
    }

    fn import_block_if_ready(&mut self, cache_threshold: u32) {
        // check if the top (FIFO) block in the cache is ready for import
        if let Some(block) = self.block_cache.remove_block_if_ready(cache_threshold) {
            let block_hash = block.block_hash();
            let block_height = block.bip34_block_height().unwrap_or(0);
            println!(
                "*** ready to import block {:?} {} header: work {} prev_hash {:?}",
                block_hash,
                block_height,
                block.header.work(),
                block.header.prev_blockhash
            );
            if let Some(prev_block_hash) = self.prev_block_hash {
                if block_height > 0 && self.prev_block_height > 0 {
                    if self.prev_block_height + 1 != block_height {
                        println!(
                            "!!! WARNING: prev imported block {:?} {}, current block {:?} {} prev_hash {:?}",
                            prev_block_hash, self.prev_block_height,
                            block_hash, block_height, block.header.prev_blockhash
                        );
                    }
                }
                assert_eq!(prev_block_hash, block.header.prev_blockhash);
            }
            self.prev_block_hash = Some(block_hash);
            self.prev_block_height = block_height;
        }
    }
}
