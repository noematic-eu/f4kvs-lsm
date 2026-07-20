use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use f4kvs_lsm::core::config::{SstableConfig, SstableReadMode};
use f4kvs_lsm::storage::sstable::{SSTable, SSTableEntry};
use f4kvs_value::Value;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn build_sstable(
    rt: &Runtime,
    entry_count: usize,
    mode: SstableReadMode,
) -> (tempfile::TempDir, Arc<SSTable>, Vec<String>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join(format!("bench-{mode:?}.sst"));
    let payload = Value::Bytes(vec![b'x'; 256]);

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

    let mut sstable = SSTable::new(path, config, 0).expect("sstable");
    rt.block_on(sstable.write_entries(entries))
        .expect("write entries");

    (dir, Arc::new(sstable), keys)
}

fn bench_mode(c: &mut Criterion, mode: SstableReadMode) {
    let rt = Runtime::new().expect("runtime");
    let entry_count = 1_000;
    let (_dir, sstable, keys) = build_sstable(&rt, entry_count, mode);

    let mut group = c.benchmark_group(format!("sstable_read_{mode:?}"));
    group.throughput(Throughput::Elements(entry_count as u64));

    group.bench_function("sequential_get", |b| {
        b.to_async(&rt).iter(|| async {
            for key in &keys {
                let _ = black_box(sstable.get(key, None).await);
            }
        });
    });

    group.bench_function("concurrent_get_x4", |b| {
        b.to_async(&rt).iter(|| async {
            let chunk = keys.len().div_ceil(4);
            let mut handles = Vec::new();
            for worker in 0..4 {
                let begin = worker * chunk;
                if begin >= keys.len() {
                    break;
                }
                let end = (begin + chunk).min(keys.len());
                let sstable = Arc::clone(&sstable);
                let keys: Vec<String> = keys[begin..end].to_vec();
                handles.push(tokio::spawn(async move {
                    for key in keys {
                        let _ = black_box(sstable.get(&key, None).await);
                    }
                }));
            }
            for handle in handles {
                let _ = handle.await;
            }
        });
    });

    group.finish();
}

fn sstable_read_modes(c: &mut Criterion) {
    bench_mode(c, SstableReadMode::SeekRead);
    bench_mode(c, SstableReadMode::PositionedRead);
    bench_mode(c, SstableReadMode::MmapHybrid);
}

criterion_group!(benches, sstable_read_modes);
criterion_main!(benches);