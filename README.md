[![GitHub Actions CI](https://github.com/VanyaGlazunov/lsm-tree/actions/workflows/rust.yml/badge.svg)](https://github.com/VanyaGlazunov/lsm-tree/actions/workflows/rust.yml)


# LSM Tree Storage 
A Rust implementation of a Log-Structured Merge Tree (LSM) storage engine initially designed to replace HashMap in Chunkfs(https://github.com/Piletskii-Oleg/chunkfs)

This LSM Storage implementaion takes inspiration and some features from LSM in a week course (https://github.com/skyzh/mini-lsm)

## Architecture Overview

TODO

## Roadmap

### Refactoring
- [] Propper error handling
- [x] Tombstone logic is week (i.e user can replicate tombstone)
- [x] Manual flush

### Priority features
- [] Write ahead log for memtables
- [] Compaction system (leveled compaction strategy)
- [] Transaction support (MVCC + SSI)

### Performance optimizations
- [] Skip list based Memtable implementaion (lock-free)
- [x] Bloom filters for SSTables
- [] Compression for SSTables

### Administration features
- [] Collect metrics