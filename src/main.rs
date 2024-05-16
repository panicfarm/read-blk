use std::fs::File;
use std::io::Read;
use std::path::Path;

fn main() {
    let dir_path = "/home/ghost/dat/bitcoin/blocks/"; //bitcoin core leveldb
    let mut file_num = 1;
    loop {
        let file_name = format!("blk{:05}.dat", file_num);
        let file_path = Path::new(dir_path).join(&file_name);
        match File::open(&file_path) {
            Ok(mut file) => {
                let mut contents = Vec::new();
                file.read_to_end(&mut contents).unwrap();
                println!("File {}: {} bytes", file_name, contents.len());
            }
            Err(_) => {
                // file not found, assume it's the last file
                println!("last File {}", file_name);
                break;
            }
        }
        file_num += 1;
    }
}
