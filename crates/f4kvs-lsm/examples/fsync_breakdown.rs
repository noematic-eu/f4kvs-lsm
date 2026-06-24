//! Isolate per-put cost: memtable-only, WAL without fsync, WAL+fsync, raw append+fsync.
use f4kvs_lsm::core::config::WalSyncMode;
use f4kvs_lsm::{LsmConfig, LsmTreeEngine};
use f4kvs_storage_core::traits::StorageEngine;
use f4kvs_value::Value;
use std::fs::OpenOptions;
use std::io::Write;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n: usize = std::env::var("N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2000);
    let chunk_bytes = 4096usize;
    let payload = Value::Bytes(vec![b'x'; chunk_bytes]);
    // ~size of one WAL Put entry (key + 4KB value + bincode overhead)
    let wal_entry_bytes = 4200usize;

    let keys: Vec<String> = (0..n)
        .map(|i| format!("chunk:legal:doc-{i:04}:chunk-{i:06}"))
        .collect();

    println!("fsync_breakdown: n={n} chunk_bytes={chunk_bytes} approx_wal_entry_bytes={wal_entry_bytes}\n");

    // memtable only (WAL disabled)
    {
        let dir = tempfile::tempdir()?;
        let mut cfg = LsmConfig::default();
        cfg.data_dir = dir.path().to_path_buf();
        cfg.wal.enabled = false;
        let engine = LsmTreeEngine::new(cfg).await?;
        let t0 = Instant::now();
        for key in &keys {
            engine.put(key, &payload).await?;
        }
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        println!("memtable_only (wal disabled): {ms:.1} ms ({:.3} ms/op)", ms / n as f64);
    }

    // WAL sync None
    {
        let dir = tempfile::tempdir()?;
        let mut cfg = LsmConfig::default();
        cfg.data_dir = dir.path().to_path_buf();
        cfg.wal.sync_mode = WalSyncMode::None;
        let engine = LsmTreeEngine::new(cfg).await?;
        let t0 = Instant::now();
        for key in &keys {
            engine.put(key, &payload).await?;
        }
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        println!("wal_sync_none:            {ms:.1} ms ({:.3} ms/op)", ms / n as f64);
    }

    // WAL sync Flush (no fsync)
    {
        let dir = tempfile::tempdir()?;
        let mut cfg = LsmConfig::default();
        cfg.data_dir = dir.path().to_path_buf();
        cfg.wal.sync_mode = WalSyncMode::Flush;
        let engine = LsmTreeEngine::new(cfg).await?;
        let t0 = Instant::now();
        for key in &keys {
            engine.put(key, &payload).await?;
        }
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        println!("wal_sync_flush:           {ms:.1} ms ({:.3} ms/op)", ms / n as f64);
    }

    // default Fsync
    {
        let dir = tempfile::tempdir()?;
        let engine = LsmTreeEngine::new(LsmConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        })
        .await?;
        let t0 = Instant::now();
        for key in &keys {
            engine.put(key, &payload).await?;
        }
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        println!("wal_sync_fsync (default): {ms:.1} ms ({:.3} ms/op)", ms / n as f64);
    }

    // raw append + fsync (no bincode/LSM)
    {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("raw.wal");
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .open(&path)?;
        let buf = vec![b'x'; wal_entry_bytes];

        let t0 = Instant::now();
        for _ in 0..n {
            file.write_all(&buf)?;
            file.sync_all()?;
        }
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        println!("raw_append+sync_all:      {ms:.1} ms ({:.3} ms/op)", ms / n as f64);
    }

    // raw append + fdatasync via std (macOS: sync_data)
    {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("raw2.wal");
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .open(&path)?;
        let buf = vec![b'x'; wal_entry_bytes];

        let t0 = Instant::now();
        for _ in 0..n {
            file.write_all(&buf)?;
            file.sync_data()?;
        }
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        println!("raw_append+sync_data:     {ms:.1} ms ({:.3} ms/op)", ms / n as f64);
    }

    Ok(())
}