# Keg

`keg` is a rust implementation of a trimmed down [LevelDB](https://github.com/google/leveldb).

Unlike [caskdb-cpp](https://github.com/jmpnz/caskdb-cpp) which was written in C++14, I preferred using
[Rust](https://www.rust-lang.org/) for `keg`. While the first follows from the design ideas introduced
in the [Bitcask](https://riak.com/assets/bitcask-intro.pdf) `keg` uses the MemTable/SSTable + WAL
approach used in both [LevelDB](https://github.com/google/leveldb) and its successor [RocksDB](https://github.com/facebook/rocksdb).

My goal in these two projects is to have a small set of projects that are ideal to introduce software engineers
in general to database systems design and implementation.
