use std::{
    fs::read_dir,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

/// Public modules
pub mod crc32;
pub mod memtable;
pub mod sstable;
pub mod wal;

fn get_current_timestamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}
/// Gets the set of files with an extension for a given directory.
fn files_with_ext(dir: &Path, ext: &str) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for file in read_dir(dir).unwrap() {
        let path = file.unwrap().path();
        if path.extension().unwrap() == ext {
            files.push(path);
        }
    }

    files
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
