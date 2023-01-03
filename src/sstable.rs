/// SSTABLE_MIN_SIZE specifies the number of records in a single SSTable.
const SSTABLE_MIN_SIZE: usize = 1024;

/// SSTable records are tuples of (key, key_length, value, value_length).
pub struct SSTableRecord {
    key: Vec<u8>,
    size: usize,
    value_offset: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(test)]
    fn test_sstable_record() {
        assert_eq!(SSTABLE_MIN_SIZE, 1024)
    }
}
