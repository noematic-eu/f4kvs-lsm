# f4kvs-lsm

Canonical LSM-tree storage engine for the F4KVS ecosystem.

## Crates

| Crate | Role |
|-------|------|
| `f4kvs-value` | `Value`, `F4KvsError`, `Result` |
| `f4kvs-storage-core` | `StorageEngine` trait and shared config |
| `f4kvs-lsm` | Persistent LSM engine (WAL, memtable, SSTables, compaction) |

## Consumers

- [f4kvs-v2](https://github.com/f4kvs/f4kvs-v2) — server and storage facade
- [f4kvs-ffi](https://github.com/f4kvs/f4kvs-ffi) — C ABI for embedders

Pin the same `f4kvs-lsm` tag in both repos. See [RELEASING.md](RELEASING.md).

## Build & test

```bash
cargo test --workspace
```

## Local path dependency

```toml
f4kvs-lsm = { path = "../f4kvs-lsm/crates/f4kvs-lsm" }
f4kvs-value = { path = "../f4kvs-lsm/crates/f4kvs-value" }
f4kvs-storage-core = { path = "../f4kvs-lsm/crates/f4kvs-storage-core" }
```