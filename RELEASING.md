# Release process

## Rule

One engine release, two consumer bumps.

1. Land changes in `f4kvs-lsm` on `main`.
2. Run `cargo test --workspace`.
3. Tag `v0.3.x` on this repo.
4. Open PRs in **f4kvs-v2** and **f4kvs-ffi** bumping the `f4kvs-lsm` git/path pin to that tag.
5. Run consumer CI:
   - v2: `cargo nextest run -p f4kvs-storage wal_crash` and `cargo nextest run -p f4kvs-e2e --test-threads 1`
   - ffi: `cargo test -p f4kvs-ffi`
6. If `f4kvs.h` changed, sync to downstream embedders (e.g. ai-rag-agent).

## Pin example

```toml
f4kvs-lsm = { git = "https://github.com/f4kvs/f4kvs-lsm", tag = "v0.3.0" }
f4kvs-value = { git = "https://github.com/f4kvs/f4kvs-lsm", tag = "v0.3.0" }
f4kvs-storage-core = { git = "https://github.com/f4kvs/f4kvs-lsm", tag = "v0.3.0" }
```

During local development, use path dependencies to `../f4kvs-lsm/crates/...`.