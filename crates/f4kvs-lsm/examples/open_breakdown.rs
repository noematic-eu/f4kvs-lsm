//! Temporary open-cost breakdown: open every .sst in a shard dir, then the
//! full engine, printing wall times. Run with F4KVS_OPEN_TIMING=1 for
//! per-phase detail from SSTable::open_sync.

use f4kvs_lsm::core::config::SstableConfig;
use f4kvs_lsm::storage::sstable::SSTable;
use f4kvs_lsm::{LsmConfig, LsmTreeEngine};
use std::time::Instant;

fn main() {
    let dir = std::env::args()
        .nth(1)
        .expect("usage: open_breakdown <shard_dir>");

    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("sst"))
        .collect();
    files.sort();

    let t_all = Instant::now();
    let mut total = std::time::Duration::ZERO;
    for path in &files {
        let t = Instant::now();
        let mut sst = SSTable::new(path.clone(), SstableConfig::default(), 0).unwrap();
        sst.open_sync().unwrap();
        let d = t.elapsed();
        total += d;
        eprintln!("open_sync {:?}: {:?}", path.file_name().unwrap(), d);
    }
    eprintln!(
        "== sequential open_sync (metadata only): {} files in {:?} (sum {:?})",
        files.len(),
        t_all.elapsed(),
        total
    );

    for round in 0..3 {
        let t = Instant::now();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut config = LsmConfig::default();
        config.data_dir = dir.clone().into();
        config.wal.dir = std::path::Path::new(&dir).join("wal");
        let engine = rt.block_on(LsmTreeEngine::new(config)).unwrap();
        eprintln!("== engine open round {}: {:?}", round, t.elapsed());
        drop(engine);
        drop(rt);
    }
}
