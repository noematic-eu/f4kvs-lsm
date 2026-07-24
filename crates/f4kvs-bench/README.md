
## fair-vs-sqlite (native, no FFI)

Product-shaped one-shot ingest: f4kvs-lsm vs SQLite WAL FULL, same durable unit.

```bash
cargo run -p f4kvs-bench --release --bin fair-vs-sqlite -- --chunks 100000
```

Flags: `--chunks`, `--chunk-bytes`, `--random-gets`, `--seed`, `--skip-sqlite`.
