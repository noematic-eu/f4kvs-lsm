//! Per-commit put latency: fsync histogram + non-fsync WAL write cost.
use f4kvs_lsm::core::config::WalSyncMode;
use f4kvs_lsm::{LsmConfig, LsmTreeEngine};
use f4kvs_storage_core::traits::StorageEngine;
use f4kvs_value::Value;
use std::fs::OpenOptions;
use std::io::Write;
use std::time::Instant;

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() - 1) as f64 * p / 100.0).round() as usize;
    sorted[idx]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n: usize = std::env::var("N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);
    let chunk_bytes = 4096usize;
    let payload = Value::Bytes(vec![b'x'; chunk_bytes]);
    let wal_entry_bytes = 4200usize;

    let keys: Vec<String> = (0..n)
        .map(|i| format!("chunk:legal:doc-{i:04}:chunk-{i:06}"))
        .collect();

    println!("per_commit_breakdown: n={n} (use N=2000 for full scale)\n");

    // --- fsync latency distribution on growing WAL-shaped file ---
    {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("wal_growth.wal");
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .open(&path)?;
        let buf = vec![b'x'; wal_entry_bytes];
        let mut latencies_ms = Vec::with_capacity(n);

        for _ in 0..n {
            file.write_all(&buf)?;
            let t0 = Instant::now();
            file.sync_all()?;
            latencies_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
        }

        let total: f64 = latencies_ms.iter().sum();
        let mut sorted = latencies_ms.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        println!("fsync_all after each ~{wal_entry_bytes}B append (raw file):");
        println!("  total: {:.1} ms  mean: {:.3} ms  min: {:.3}  p50: {:.3}  p99: {:.3}  max: {:.3}",
            total, total / n as f64, sorted[0], pct(&sorted, 50.0), pct(&sorted, 99.0), sorted[n - 1]);
        println!();
    }

    // --- per-put latency distribution (engine, Fsync) ---
    {
        let dir = tempfile::tempdir()?;
        let engine = LsmTreeEngine::new(LsmConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        })
        .await?;

        let mut latencies_ms = Vec::with_capacity(n);
        for key in &keys {
            let t0 = Instant::now();
            engine.put(key, &payload).await?;
            latencies_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
        }

        let total: f64 = latencies_ms.iter().sum();
        let mut sorted = latencies_ms.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        println!("engine.put (WalSyncMode::Fsync, default):");
        println!("  total: {:.1} ms  mean: {:.3} ms  min: {:.3}  p50: {:.3}  p99: {:.3}  max: {:.3}",
            total, total / n as f64, sorted[0], pct(&sorted, 50.0), pct(&sorted, 99.0), sorted[n - 1]);
        println!();
    }

    // --- same keys, no fsync ---
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
        println!("engine.put (WalSyncMode::None): {ms:.1} ms ({:.3} ms/op)", ms / n as f64);
    }

    Ok(())
}