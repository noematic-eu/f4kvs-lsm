//! Compare SSTable read latency across [`SstableReadMode`] strategies.
//!
//! ```bash
//! cargo run -p f4kvs-lsm --example sstable_read_mode_bench --release
//! N=5000 CONCURRENCY=8 cargo run -p f4kvs-lsm --example sstable_read_mode_bench --release
//! ```

use f4kvs_lsm::core::config::{SstableConfig, SstableReadMode};
use f4kvs_lsm::storage::sstable::{SSTable, SSTableEntry};
use f4kvs_value::Value;
use std::sync::Arc;
use std::time::Instant;

struct BenchFixture {
    _dir: tempfile::TempDir,
    sstable: Arc<SSTable>,
    keys: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let entry_count: usize = std::env::var("N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000);
    let concurrency: usize = std::env::var("CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);
    let payload_size: usize = std::env::var("PAYLOAD")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(256);

    println!(
        "sstable_read_mode_bench: entries={entry_count} concurrency={concurrency} payload={payload_size}B\n"
    );

    let modes = [
        SstableReadMode::SeekRead,
        SstableReadMode::PositionedRead,
        SstableReadMode::MmapHybrid,
    ];

    for mode in modes {
        let fixture = build_sstable(entry_count, payload_size, mode).await?;
        bench_sequential(&fixture.sstable, &fixture.keys, mode).await?;
        bench_concurrent(fixture.sstable.clone(), &fixture.keys, mode, concurrency).await?;
        println!();
    }

    Ok(())
}

async fn build_sstable(
    entry_count: usize,
    payload_size: usize,
    mode: SstableReadMode,
) -> Result<BenchFixture, Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join(format!("bench-{mode:?}.sst"));
    let payload = Value::Bytes(vec![b'x'; payload_size]);

    let entries: Vec<SSTableEntry> = (0..entry_count)
        .map(|i| SSTableEntry {
            key: format!("key-{i:08}"),
            value: payload.clone(),
            timestamp: i as u64,
            deleted: false,
        })
        .collect();

    let keys: Vec<String> = entries.iter().map(|e| e.key.clone()).collect();

    let config = SstableConfig {
        read_mode: mode,
        ..Default::default()
    };

    let mut sstable = SSTable::new(path, config, 0)?;
    sstable.write_entries(entries).await?;

    Ok(BenchFixture {
        _dir: dir,
        sstable: Arc::new(sstable),
        keys,
    })
}

async fn bench_sequential(
    sstable: &Arc<SSTable>,
    keys: &[String],
    mode: SstableReadMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let mut hits = 0usize;
    for key in keys {
        if sstable.get(key, None).await?.is_some() {
            hits += 1;
        }
    }
    let elapsed = start.elapsed();
    let ops = keys.len() as f64;
    println!(
        "{mode:?} sequential: {:.1} ms total | {:.3} ms/op | {:.0} ops/s | hits={hits}",
        elapsed.as_secs_f64() * 1000.0,
        elapsed.as_secs_f64() * 1000.0 / ops,
        ops / elapsed.as_secs_f64(),
        hits = hits
    );
    Ok(())
}

async fn bench_concurrent(
    sstable: Arc<SSTable>,
    keys: &[String],
    mode: SstableReadMode,
    concurrency: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let keys = Arc::new(keys.to_vec());
    let chunk = keys.len().div_ceil(concurrency);
    let start = Instant::now();

    let mut handles = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let begin = worker * chunk;
        if begin >= keys.len() {
            break;
        }
        let end = (begin + chunk).min(keys.len());
        let sstable = Arc::clone(&sstable);
        let keys = Arc::clone(&keys);
        handles.push(tokio::spawn(async move {
            let mut hits = 0usize;
            for key in &keys[begin..end] {
                if sstable.get(key, None).await.ok().flatten().is_some() {
                    hits += 1;
                }
            }
            hits
        }));
    }

    let mut total_hits = 0usize;
    for handle in handles {
        total_hits += handle.await?;
    }

    let elapsed = start.elapsed();
    let ops = keys.len() as f64;
    println!(
        "{mode:?} concurrent({concurrency}): {:.1} ms total | {:.3} ms/op | {:.0} ops/s | hits={total_hits}",
        elapsed.as_secs_f64() * 1000.0,
        elapsed.as_secs_f64() * 1000.0 / ops,
        ops / elapsed.as_secs_f64(),
    );
    Ok(())
}