mod block_cache;

use bitcoin::block::Block;
use bitcoin::consensus::Decodable;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;

struct Importer {
    block_cache: block_cache::BlockCache,
}

fn main() {
    let dir_path = "/home/ghost/dat/bitcoin/blocks/"; //bitcoin core leveldb
    let mut file_num = 1328;
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
    }
}

impl Importer {
    fn new() -> Self {
        Importer {
            block_cache: block_cache::BlockCache::new(),
        }
    }

    fn read_blocks(&mut self, file_bytes: Vec<u8>) {
        let mut i = 0;
        loop {
            if i == file_bytes.len() {
                break;
            }

            let len = u32::from_le_bytes(file_bytes[i + 4..i + 8].try_into().unwrap()) as usize;
            println!("read {} {}", i, len);
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
                    "read block {:?} {} header: work {} prev_hash {:?}",
                    block.block_hash(),
                    block.bip34_block_height().unwrap_or(0),
                    block.header.work(),
                    block.header.prev_blockhash
                );
                self.block_cache.add_block(block);
                if let Some(_block_ready_to_import) = self.block_cache.remove_block_if_ready(100) {
                    //TODO add to main chain
                }
            }

            i += 8 + len;
        }
    }
}
