#![allow(dead_code)]
use std::fs::File;

/// SSTABLE_MIN_SIZE specifies the number of records in a single SSTable.
const SSTABLE_MIN_SIZE: usize = 1024;

/// SSTable represents an on-disk MemTable, with keys and values laid out
/// in order. SSTables are fixed size and when a level threshold is reached
/// SSTables are compacted to a higher level.
/// The compaction process frees up disk space by removing deleted key-value
/// pairs.
/// The file format used for SSTables is very simple :
/// [HEADER][DATA BLOCK][INDEX BLOCK][META BLOCK]
/// The [HEADER] holds a magic number and a version number.
/// The [DATA BLOCK] is a segment of key-value pairs.
/// The [INDEX BLOCK] is used as a hint file to create sparse indexes (key => offset).
/// The [META BLOCK] holds metadata about this SSTable such as the number of entries
/// and lowest/highest key ranges in the SSTable.
/// TODO: optimizations such as prefix encoding and compression.
pub struct SSTable {
    pub metadata: SSTableMetadata,
    file: File, // Physical file where the SSTable data is stored.
                // SSTSparseIndex a sparse index of (key => offset)
                // Bloom<Key> a bloomfilter of all keys in the SSTable
}

/// SSTableMetadata holds important metadata about an SSTable.
pub struct SSTableMetadata {
    id: u64,                   // Unique identifier for this SSTable
    first_key: (Vec<u8>, u64), // First key in this SSTable
    last_key: (Vec<u8>, u64),  // Last key in this SSTable
    total_size: usize,         // Total size of the table in bytes usually < 4MB
    num_entries: usize,        // Number of unique key-value pairs in this SSTable.
}
#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(test)]
    fn test_sstable_record() {
        assert_eq!(SSTABLE_MIN_SIZE, 1024)
    }
}
