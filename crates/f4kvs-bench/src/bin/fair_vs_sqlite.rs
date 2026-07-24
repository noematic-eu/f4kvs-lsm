//! Fair product-shaped ingest: f4kvs-lsm (native Rust) vs SQLite WAL FULL.
//!
//! Same durable unit as SQLite mono-txn: one `batch_put` of N keys + `flush_wal`,
//! vs one `BEGIN`…`COMMIT`. No Go/CGO/FFI.
//!
//! ```bash
//! cargo run -p f4kvs-bench --release --bin fair-vs-sqlite -- --chunks 100000
//! ```

use f4kvs_lsm::core::config::LsmConfig;
use f4kvs_lsm::LsmTreeEngine;
use f4kvs_storage_core::traits::StorageEngine;
use f4kvs_value::Value;
use rusqlite::{Connection, OptionalExtension};
use std::env;
use std::path::Path;
use std::process;
use std::time::Instant;
use tempfile::TempDir;

#[derive(Clone, Copy)]
struct Args {
    chunks: usize,
    chunk_bytes: usize,
    memoirs: usize,
    memoir_bytes: usize,
    random_gets: usize,
    seed: u64,
    skip_sqlite: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            chunks: 100_000,
            chunk_bytes: 4096,
            memoirs: 50,
            memoir_bytes: 200_000,
            random_gets: 5_000,
            seed: 42,
            skip_sqlite: false,
        }
    }
}

fn parse_args() -> Args {
    let mut a = Args::default();
    let mut it = env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--chunks" => a.chunks = it.next().and_then(|s| s.parse().ok()).unwrap_or(a.chunks),
            "--chunk-bytes" => {
                a.chunk_bytes = it.next().and_then(|s| s.parse().ok()).unwrap_or(a.chunk_bytes)
            }
            "--memoirs" => a.memoirs = it.next().and_then(|s| s.parse().ok()).unwrap_or(a.memoirs),
            "--memoir-bytes" => {
                a.memoir_bytes = it
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(a.memoir_bytes)
            }
            "--random-gets" => {
                a.random_gets = it.next().and_then(|s| s.parse().ok()).unwrap_or(a.random_gets)
            }
            "--seed" => a.seed = it.next().and_then(|s| s.parse().ok()).unwrap_or(a.seed),
            "--skip-sqlite" => a.skip_sqlite = true,
            "--help" | "-h" => {
                eprintln!(
                    "fair-vs-sqlite — native f4kvs-lsm vs SQLite FULL (no FFI)\n\
                     \n\
                     Options:\n\
                       --chunks N          default 100000\n\
                       --chunk-bytes N     default 4096\n\
                       --memoirs N         default 50\n\
                       --memoir-bytes N    default 200000\n\
                       --random-gets N     default 5000\n\
                       --seed N            default 42\n\
                       --skip-sqlite       f4kvs only\n"
                );
                process::exit(0);
            }
            other => {
                eprintln!("unknown arg: {other} (try --help)");
                process::exit(2);
            }
        }
    }
    a
}

fn sample_payload(n: usize, seed: u64) -> Vec<u8> {
    let head = br#"{"v":1,"title":"bench","body":""#;
    let tail = br#""}"#;
    let mut out = vec![0u8; n];
    if n <= head.len() + tail.len() {
        out.resize(n, b'x');
        return out;
    }
    out[..head.len()].copy_from_slice(head);
    let fill = n - head.len() - tail.len();
    let mut state = seed;
    for i in 0..fill {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out[head.len() + i] = b'a' + (state % 26) as u8;
    }
    out[head.len() + fill..].copy_from_slice(tail);
    out
}

fn chunk_key(i: usize) -> String {
    format!("chunk:legal:doc-{:04}:chunk-{:06}", i / 10, i)
}

fn memoir_key(i: usize) -> String {
    format!("memoir:{i:04}")
}

fn ops_per_s(n: usize, ms: f64) -> f64 {
    if ms <= 0.0 {
        return 0.0;
    }
    (n as f64) / (ms / 1000.0)
}

struct Phase {
    engine: &'static str,
    phase: &'static str,
    ops: usize,
    ms: f64,
    notes: String,
}

fn print_table(rows: &[Phase]) {
    println!();
    println!(
        "{:<28} {:<22} {:>10} {:>12} {:>12}  {}",
        "phase", "engine", "ops", "ms", "ops/s", "notes"
    );
    for r in rows {
        println!(
            "{:<28} {:<22} {:>10} {:>12.1} {:>12.0}  {}",
            r.phase,
            r.engine,
            r.ops,
            r.ms,
            ops_per_s(r.ops, r.ms),
            r.notes
        );
    }
}

async fn run_f4kvs(dir: &Path, args: Args, chunk_payload: &[u8], memoir_payload: &[u8]) -> Vec<Phase> {
    let mut cfg = LsmConfig::default();
    cfg.data_dir = dir.to_path_buf();
    cfg.wal.dir = dir.join("wal");
    // One durable unit for all keys (fair vs SQLite single COMMIT).
    cfg.performance.max_batch_size = args.chunks.max(args.memoirs).max(1);
    // Blocking fsync like SQLite FULL commit.
    cfg.wal.sync_mode = f4kvs_lsm::core::config::WalSyncMode::Fsync;
    cfg.wal.group_commit_enabled = false;

    let engine = LsmTreeEngine::new(cfg).await.expect("lsm open");

    let mut rows = Vec::new();

    // Memoirs: small, not the fair focus.
    if args.memoirs > 0 {
        let t0 = Instant::now();
        for i in 0..args.memoirs {
            engine
                .put(
                    &memoir_key(i),
                    &Value::Bytes(memoir_payload.to_vec()),
                )
                .await
                .expect("memoir put");
        }
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        rows.push(Phase {
            engine: "f4kvs_lsm_native",
            phase: "memoir_put",
            ops: args.memoirs,
            ms,
            notes: "per-put (not fair focus)".into(),
        });
    }

    // Fair bulk: one batch_put for all chunks.
    let items: Vec<(String, Value)> = (0..args.chunks)
        .map(|i| (chunk_key(i), Value::Bytes(chunk_payload.to_vec())))
        .collect();

    let t0 = Instant::now();
    engine.batch_put(items).await.expect("batch_put");
    engine.flush_wal().await.expect("flush_wal");
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    rows.push(Phase {
        engine: "f4kvs_lsm_native",
        phase: "chunk_batch_put_one_shot",
        ops: args.chunks,
        ms,
        notes: format!(
            "1× batch_put(n={}) + flush_wal; max_batch_size={}",
            args.chunks, args.chunks
        ),
    });

    // Prefix scan
    let t0 = Instant::now();
    let keys = engine.scan_prefix("chunk:legal:").await.expect("scan");
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    rows.push(Phase {
        engine: "f4kvs_lsm_native",
        phase: "chunk_prefix_scan",
        ops: keys.len(),
        ms,
        notes: format!("keys={}", keys.len()),
    });

    // Random get
    let t0 = Instant::now();
    for i in 0..args.random_gets {
        let k = chunk_key(i % args.chunks);
        let _ = engine.get(&k).await.expect("get");
    }
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    rows.push(Phase {
        engine: "f4kvs_lsm_native",
        phase: "chunk_random_get",
        ops: args.random_gets,
        ms,
        notes: "point reads".into(),
    });

    // Restart integrity
    drop(engine);
    let mut cfg2 = LsmConfig::default();
    cfg2.data_dir = dir.to_path_buf();
    cfg2.wal.dir = dir.join("wal");
    cfg2.performance.max_batch_size = args.chunks.max(1);
    cfg2.wal.sync_mode = f4kvs_lsm::core::config::WalSyncMode::Fsync;
    let t0 = Instant::now();
    let reopened = LsmTreeEngine::new(cfg2).await.expect("reopen");
    let m = reopened.scan_prefix("memoir:").await.expect("scan m").len();
    let c = reopened.scan_prefix("chunk:").await.expect("scan c").len();
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    let expected = args.memoirs + args.chunks;
    let ok = m + c == expected;
    rows.push(Phase {
        engine: "f4kvs_lsm_native",
        phase: "post_restart_row_count",
        ops: expected,
        ms,
        notes: format!(
            "counted={} expected={} integrity_ok={}",
            m + c,
            expected,
            if ok { 1 } else { 0 }
        ),
    });
    if !ok {
        eprintln!("FATAL: f4kvs integrity_ok=0");
        process::exit(1);
    }
    drop(reopened);
    rows
}

fn run_sqlite(dir: &Path, args: Args, chunk_payload: &[u8], memoir_payload: &[u8]) -> Vec<Phase> {
    let path = dir.join("kv.sqlite");
    let conn = Connection::open(&path).expect("sqlite open");
    // Match embed_vs_sqlite Go harness: WAL + FULL. Extra cache so bulk insert
    // is not artificially throttled vs f4kvs memtable buffering.
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=FULL;
         PRAGMA foreign_keys=ON;
         PRAGMA temp_store=MEMORY;
         PRAGMA cache_size=-262144;
         PRAGMA mmap_size=268435456;
         CREATE TABLE kv (
           key TEXT PRIMARY KEY,
           value BLOB NOT NULL
         ) WITHOUT ROWID;",
    )
    .expect("sqlite setup");

    let mut rows = Vec::new();

    if args.memoirs > 0 {
        let t0 = Instant::now();
        let tx = conn.unchecked_transaction().expect("tx");
        {
            let mut stmt = tx
                .prepare("INSERT INTO kv (key, value) VALUES (?1, ?2)")
                .expect("prep");
            for i in 0..args.memoirs {
                stmt.execute(rusqlite::params![memoir_key(i), memoir_payload])
                    .expect("insert memoir");
            }
        }
        tx.commit().expect("commit memoirs");
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        rows.push(Phase {
            engine: "sqlite_wal_full",
            phase: "memoir_put",
            ops: args.memoirs,
            ms,
            notes: "single txn".into(),
        });
    }

    // Fair: one transaction for all chunks (same durable unit as f4kvs one-shot).
    let t0 = Instant::now();
    let tx = conn.unchecked_transaction().expect("tx chunks");
    {
        let mut stmt = tx
            .prepare("INSERT INTO kv (key, value) VALUES (?1, ?2)")
            .expect("prep");
        for i in 0..args.chunks {
            stmt.execute(rusqlite::params![chunk_key(i), chunk_payload])
                .expect("insert chunk");
        }
    }
    tx.commit().expect("commit chunks");
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    rows.push(Phase {
        engine: "sqlite_wal_full",
        phase: "chunk_batch_put_one_shot",
        ops: args.chunks,
        ms,
        notes: "1× BEGIN..COMMIT synchronous=FULL".into(),
    });

    let t0 = Instant::now();
    let mut stmt = conn
        .prepare("SELECT key FROM kv WHERE key LIKE 'chunk:legal:%'")
        .expect("scan prep");
    let keys: Vec<String> = stmt
        .query_map([], |r| r.get(0))
        .expect("scan")
        .map(|r| r.expect("row"))
        .collect();
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    rows.push(Phase {
        engine: "sqlite_wal_full",
        phase: "chunk_prefix_scan",
        ops: keys.len(),
        ms,
        notes: format!("keys={}", keys.len()),
    });

    let t0 = Instant::now();
    let mut get = conn
        .prepare("SELECT value FROM kv WHERE key = ?1")
        .expect("get prep");
    for i in 0..args.random_gets {
        let k = chunk_key(i % args.chunks);
        let _: Option<Vec<u8>> = get
            .query_row(rusqlite::params![k], |r| r.get(0))
            .optional()
            .expect("get");
    }
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    rows.push(Phase {
        engine: "sqlite_wal_full",
        phase: "chunk_random_get",
        ops: args.random_gets,
        ms,
        notes: "point reads".into(),
    });

    // Restart integrity
    drop(get);
    drop(stmt);
    drop(conn);
    let t0 = Instant::now();
    let conn2 = Connection::open(&path).expect("reopen");
    let count: i64 = conn2
        .query_row("SELECT COUNT(*) FROM kv", [], |r| r.get(0))
        .expect("count");
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    let expected = (args.memoirs + args.chunks) as i64;
    let ok = count == expected;
    rows.push(Phase {
        engine: "sqlite_wal_full",
        phase: "post_restart_row_count",
        ops: expected as usize,
        ms,
        notes: format!(
            "counted={} expected={} integrity_ok={}",
            count,
            expected,
            if ok { 1 } else { 0 }
        ),
    });
    if !ok {
        eprintln!("FATAL: sqlite integrity_ok=0");
        process::exit(1);
    }
    rows
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    let chunk_payload = sample_payload(args.chunk_bytes, args.seed.wrapping_add(1));
    let memoir_payload = sample_payload(args.memoir_bytes, args.seed);

    println!("fair-vs-sqlite (native Rust, no Go/FFI)");
    println!(
        "scale: chunks={}×{}B memoirs={} random_gets={} seed={}",
        args.chunks, args.chunk_bytes, args.memoirs, args.random_gets, args.seed
    );
    println!("durability: f4kvs 1×batch_put+flush_wal (Fsync) | sqlite 1×COMMIT (WAL FULL)");
    println!();

    let mut all = Vec::new();

    let f4_dir = TempDir::new().expect("tmp f4");
    println!("=== f4kvs-lsm native @ {} ===", f4_dir.path().display());
    all.extend(run_f4kvs(f4_dir.path(), args, &chunk_payload, &memoir_payload).await);

    if !args.skip_sqlite {
        let sql_dir = TempDir::new().expect("tmp sql");
        println!("=== sqlite_wal_full @ {} ===", sql_dir.path().display());
        all.extend(run_sqlite(
            sql_dir.path(),
            args,
            &chunk_payload,
            &memoir_payload,
        ));
    }

    print_table(&all);

    // Head-to-head ingest summary
    let f4_ingest = all
        .iter()
        .find(|p| p.engine == "f4kvs_lsm_native" && p.phase == "chunk_batch_put_one_shot");
    let sql_ingest = all
        .iter()
        .find(|p| p.engine == "sqlite_wal_full" && p.phase == "chunk_batch_put_one_shot");
    if let (Some(f), Some(s)) = (f4_ingest, sql_ingest) {
        let ratio = ops_per_s(s.ops, s.ms) / ops_per_s(f.ops, f.ms).max(1e-9);
        println!();
        println!(
            "ingest one-shot: f4kvs {:.0} ops/s  |  sqlite {:.0} ops/s  |  sqlite/f4kvs = {:.2}×",
            ops_per_s(f.ops, f.ms),
            ops_per_s(s.ops, s.ms),
            ratio
        );
    }
}
