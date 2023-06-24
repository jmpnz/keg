use std::{
    fs::{remove_file, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use crate::{files_with_ext, get_current_timestamp, memtable::MemTable};

/// Write Ahead Log (WAL) is a technique in database recovery management to ensure
/// that data that was stored at any point will persist post-failure.
///
/// The principle is very simple, create an append mode file (`O_APPEND`) on disk.
/// On each write to the current live `[MemTable]` append the entire entry to the file.
///
/// This helps recover the entire `[MemTable]` even in worst-case scenarios since.
///
/// On important detail that was omitted is that in general when you write to a file
/// the data written will live in the Kernel's page cache (in-memory) for sometime
/// before the actual write to disk is done. The Kernel will often flush the buffer
/// periodically or when a shutdown signal is captured.
///
/// This operation is commonly called "flushing" or "fsyncing" due to the API
/// responsible for this in Unix `fsync`.
///
/// You can force the Kernel to flush the write from the buffer to the disk by calling
/// `fsync` if you choose not to then you have no guarentees on whether your writes will
/// be reflected on-disk.
///
/// One tidbit to remember is that `fsync` is not a holy solution you can read more
/// here: https://www.evanjones.ca/durability-filesystem.html
///
/// Always flushing to disk can come with some performance hits. Because the Kernel addresses
/// the disk as a block device (I/O operations are done in chunks called blocks).
///
/// Even if you write 80 bytes to a file the OS will write 4KB of data (80 bytes + padding).
/// This can introduce serious "Write Amplification" that can be quantified by dividing
/// the block size for the operation system by the size of the data you write.
///
/// In the example above the write amplification is 4096/80 ~ 51.
///
/// "Write Amplification" will cause wear on most SSDs decreasing their lifespan along the way.
#[derive(Debug)]
pub struct WAL {
    path: PathBuf,
    file: BufWriter<File>,
}

impl WAL {
    /// Create a new WAL file in a given directory.
    pub fn new(dir: &Path) -> io::Result<Self> {
        let timestamp = get_current_timestamp();
        let path = Path::new(dir).join(timestamp.to_string() + ".wal");
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);

        Ok(Self { path, file })
    }

    /// Open a WAL file from an existing file path.
    pub fn from(path: &Path) -> io::Result<WAL> {
        let file = OpenOptions::new().append(true).create(true).open(path)?;
        let file = BufWriter::new(file);

        Ok(WAL {
            path: path.to_owned(),
            file,
        })
    }
    /// Append a new entry in the WAL for a set operation.
    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> io::Result<()> {
        self.file.write_all(&key.len().to_le_bytes())?;
        self.file.write_all(&(false as u8).to_le_bytes())?;
        self.file.write_all(&value.len().to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(value)?;
        self.file.write_all(&timestamp.to_le_bytes())?;

        Ok(())
    }
    /// Append a new entry in the WAL for a delete operation.
    pub fn delete(&mut self, key: &[u8], timestamp: u128) -> io::Result<()> {
        self.file.write_all(&key.len().to_le_bytes())?;
        self.file.write_all(&(true as u8).to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(&timestamp.to_le_bytes())?;

        Ok(())
    }
    /// Flush the WAL to disk.
    ///
    /// As mentionned above calling Flush will explicity write all changes
    /// currently in-memory to disk, allowing the caller to batch writes
    /// to the WAL.
    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
    /// Load the WAL(s) in a directory.
    pub fn load_from_dir(dir: &Path) -> io::Result<(WAL, MemTable)> {
        let mut wal_files = files_with_ext(dir, "wal");
        // WAL files are numbered by microsecond timestamps.
        wal_files.sort();

        let mut new_tbl = MemTable::new();
        let mut new_wal = WAL::new(dir)?;

        for wal_file in wal_files.iter() {
            if let Ok(wal) = WAL::from(wal_file) {
                for entry in wal.into_iter() {
                    if entry.deleted {
                        new_tbl.delete(entry.key.as_slice(), entry.timestamp);
                        new_wal.delete(entry.key.as_slice(), entry.timestamp)?;
                    } else {
                        new_tbl.set(
                            entry.key.as_slice(),
                            entry.value.as_ref().unwrap().as_slice(),
                            entry.timestamp,
                        );
                        new_wal.set(
                            entry.key.as_slice(),
                            entry.value.unwrap().as_slice(),
                            entry.timestamp,
                        )?;
                    }
                }
            }
        }
        new_wal.flush().unwrap();
        wal_files.into_iter().for_each(|f| remove_file(f).unwrap());
        Ok((new_wal, new_tbl))
    }
}

impl IntoIterator for WAL {
    type IntoIter = WALIterator;
    type Item = WALEntry;

    fn into_iter(self) -> Self::IntoIter {
        WALIterator::new(self.path).unwrap()
    }
}

#[derive(Debug)]
pub struct WALEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
    pub deleted: bool,
}

pub struct WALIterator {
    reader: BufReader<File>,
}

impl WALIterator {
    /// Create a WAL iterator from a path to a WAL file.
    pub fn new(path: PathBuf) -> io::Result<WALIterator> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);

        Ok(WALIterator { reader })
    }
}

/// Implementation of the iterator trait for WALIterator.
/// WALEntry is laid out on disk as
///
/// +---------------+---------------+-----------------+-----+-------+-----------------+
/// | Key Size (8B) | Tombstone(1B) | Value Size (8B) | Key | Value | Timestamp (16B) |
/// +---------------+---------------+-----------------+-----+-------+-----------------+
/// Key Size = Length of the Key data
/// Tombstone = If this record was deleted and has a value
/// Value Size = Length of the Value data
/// Key = Key data
/// Value = Value data
/// Timestamp = Timestamp of the operation in microseconds
impl Iterator for WALIterator {
    type Item = WALEntry;

    /// Get the next entry in the WAL.
    fn next(&mut self) -> Option<WALEntry> {
        let mut len_buffer = [0; 8];
        if self.reader.read_exact(&mut len_buffer).is_err() {
            return None;
        }

        let key_len = usize::from_le_bytes(len_buffer);

        let mut bool_buffer = [0; 1];
        if self.reader.read_exact(&mut bool_buffer).is_err() {
            return None;
        }
        let deleted = bool_buffer[0] != 0;

        let mut key = vec![0; key_len];
        let mut value = None;
        if deleted {
            if self.reader.read_exact(&mut key).is_err() {
                return None;
            }
        } else {
            if self.reader.read_exact(&mut len_buffer).is_err() {
                return None;
            }

            let value_len = usize::from_le_bytes(len_buffer);
            if self.reader.read_exact(&mut key).is_err() {
                return None;
            }

            let mut value_buf = vec![0; value_len];
            if self.reader.read_exact(&mut value_buf).is_err() {
                return None;
            }

            value = Some(value_buf);
        }

        let mut timestamp_buffer = [0; 16];
        if self.reader.read_exact(&mut timestamp_buffer).is_err() {
            return None;
        }
        let timestamp = u128::from_le_bytes(timestamp_buffer);

        Some(WALEntry {
            key,
            value,
            timestamp,
            deleted,
        })
    }
}
