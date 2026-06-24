//! Isolate chunk-batch put cost: single put vs engine batch_put vs WAL batch only.
use f4kvs_lsm::{LsmConfig, LsmTreeEngine};
use f4kvs_storage_core::traits::StorageEngine;
use f4kvs_value::Value;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n: usize = std::env::var("N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2000);
    let chunk_bytes = 4096usize;
    let payload = Value::Bytes(vec![b'x'; chunk_bytes]);

    let keys: Vec<String> = (0..n)
        .map(|i| format!("chunk:legal:doc-{i:04}:chunk-{i:06}"))
        .collect();

    println!("write_breakdown: n={n} chunk_bytes={chunk_bytes}");
    println!("engine default: WAL enabled, WalSyncMode::Fsync\n");

    // --- single put ---
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
        println!("single_put x{n}: {ms:.1} ms ({:.3} ms/op)", ms / n as f64);
    }

    // --- batch_put (one WAL fsync for whole batch) ---
    {
        let dir = tempfile::tempdir()?;
        let engine = LsmTreeEngine::new(LsmConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        })
        .await?;
        let items: Vec<(String, Value)> = keys
            .iter()
            .map(|k| (k.clone(), payload.clone()))
            .collect();
        let t0 = Instant::now();
        engine.batch_put(items).await?;
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        println!("batch_put x{n} (1 WAL batch): {ms:.1} ms ({:.3} ms/op)", ms / n as f64);
    }

    // --- batch_put chunks of 100 ---
    {
        let dir = tempfile::tempdir()?;
        let engine = LsmTreeEngine::new(LsmConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        })
        .await?;
        let t0 = Instant::now();
        for chunk in keys.chunks(100) {
            let items: Vec<(String, Value)> = chunk
                .iter()
                .map(|k| (k.clone(), payload.clone()))
                .collect();
            engine.batch_put(items).await?;
        }
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        println!(
            "batch_put x{n} ({} batches of 100): {ms:.1} ms ({:.3} ms/op)",
            n.div_ceil(100),
            ms / n as f64
        );
    }

    Ok(())
}