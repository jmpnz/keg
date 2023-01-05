/// MemTableSize is set to 4kb.
pub const MEM_TABLE_SIZE: usize = 4096;

/// MemTable holds a sorted list of the latest written records.
///
/// Writes are duplicated to the WAL for recovery of the MemTable in case of failures.
///
/// MemTables have a fixed capacity and when it is reached, we flush the MemTable
/// to disk as an SSTable.
pub struct MemTable {
    entries: Vec<MemTableEntry>,
    size: usize,
    cap: usize,
}

pub struct MemTableEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
    pub deleted: bool,
}

impl MemTable {
    /// Create an owned MemTable with `MEM_TABLE_SIZE` capacity.
    pub fn new() -> MemTable {
        MemTable {
            entries: Vec::with_capacity(MEM_TABLE_SIZE),
            size: 0,
            cap: MEM_TABLE_SIZE,
        }
    }

    /// Capacity of this MemTable (always equal to `[MEM_TABLE_SIZE]`)
    pub fn cap(&self) -> usize {
        self.cap
    }

    /// Number of entries in the MemTable.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the MemTable is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Perform a binary search on the entries to find a given record's index.
    /// If the record is not find a `[Result::Err]` is returned with the index to insert at.
    fn get_index(&self, key: &[u8]) -> Result<usize, usize> {
        self.entries
            .binary_search_by_key(&key, |e| e.key.as_slice())
    }

    /// Set a key-value pair in the MemTable.
    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) {
        let entry = MemTableEntry {
            key: key.to_owned(),
            value: Some(value.to_owned()),
            timestamp,
            deleted: false,
        };

        match self.get_index(key) {
            Ok(idx) => {
                if let Some(v) = self.entries[idx].value.as_ref() {
                    if value.len() < v.len() {
                        self.size -= v.len() - value.len();
                    } else {
                        self.size += value.len() - v.len();
                    }
                }
                self.entries[idx] = entry;
            }
            Err(idx) => {
                self.size += key.len() + value.len() + 16 + 1;
                self.entries.insert(idx, entry)
            }
        }
    }

    /// Delete a key-value pair from the MemTable using tombstones.
    pub fn delete(&mut self, key: &[u8], timestamp: u128) {
        let entry = MemTableEntry {
            key: key.to_owned(),
            value: None,
            timestamp,
            deleted: true,
        };

        match self.get_index(key) {
            Ok(idx) => {
                if let Some(value) = self.entries[idx].value.as_ref() {
                    self.size -= value.len();
                }
                self.entries[idx] = entry;
            }
            Err(idx) => {
                self.size += key.len() + 16 + 1;
                self.entries.insert(idx, entry)
            }
        }
    }

    /// Get an entry from the MemTable.
    pub fn get(&self, key: &[u8]) -> Option<&MemTableEntry> {
        if let Ok(idx) = self.get_index(key) {
            return Some(&self.entries[idx]);
        }
        None
    }

    /// Return a reference to the entries in the MemTable.
    pub fn entries(&self) -> &[MemTableEntry] {
        &self.entries
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {

    use crate::get_current_timestamp;

    use super::*;

    #[test]
    fn test_get_set() {
        let mut tbl = MemTable::new();
        let kv = (
            "Alice".as_bytes(),
            "Works as a product manager at Google".as_bytes(),
        );
        let ts = get_current_timestamp();
        tbl.set(kv.0, kv.1, ts);

        let actual = tbl.get(kv.0).unwrap();
        assert_eq!(actual.value.as_ref().unwrap(), kv.1);
    }
    #[test]
    fn test_multi_set_get() {
        let mut tbl = MemTable::new();
        let records = vec![
            ("Carl", "Works as a kernel engineer at Google"),
            ("Bob", "Works as reliability engineer at GCP"),
            ("Alice", "Works as a product manager at Google"),
        ];

        for record in records.iter() {
            tbl.set(
                record.0.as_bytes(),
                record.1.as_bytes(),
                get_current_timestamp(),
            );
        }

        assert_eq!(tbl.cap(), MEM_TABLE_SIZE);
        assert_eq!(tbl.len(), 3);
        // ensure keys are stored in order
        for (actual, expected) in tbl.entries().iter().zip(records.iter().rev()) {
            assert_eq!(actual.key, expected.0.as_bytes());
        }
    }
}
