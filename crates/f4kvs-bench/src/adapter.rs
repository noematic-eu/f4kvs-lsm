use crate::harness::{
    BenchDatabase, BenchDatabaseConnection, BenchInserter, BenchReadTransaction, BenchReader,
    BenchWriteTransaction,
};
use f4kvs_lsm::{LsmConfig, LsmTreeEngine};
use f4kvs_storage_core::traits::StorageEngine;
use f4kvs_value::Value;
use std::collections::HashSet;
use std::fmt::Write as _;
use std::path::Path;
use std::sync::{mpsc, Arc, OnceLock};
use tokio::runtime::Handle;

fn runtime_handle() -> &'static Handle {
    static HANDLE: OnceLock<Handle> = OnceLock::new();
    HANDLE.get_or_init(|| {
        let (tx, rx) = mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("f4kvs-bench-runtime".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(4)
                    .enable_all()
                    .build()
                    .expect("tokio runtime");
                tx.send(rt.handle().clone()).expect("runtime handle");
                rt.block_on(std::future::pending::<()>());
            })
            .expect("spawn runtime thread");
        rx.recv().expect("runtime handle")
    })
}

fn block_on<F, T>(future: F) -> T
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    runtime_handle().block_on(future)
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(s, "{b:02x}").unwrap();
    }
    s
}

const MAX_BATCH_SIZE: usize = 10_000;

#[derive(Clone)]
pub struct F4kvsBytes(pub Vec<u8>);

impl AsRef<[u8]> for F4kvsBytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

pub struct F4kvsBenchDatabase {
    engine: Arc<LsmTreeEngine>,
}

impl F4kvsBenchDatabase {
    pub fn new(path: &Path) -> Self {
        let mut config = LsmConfig::default();
        config.data_dir = path.to_path_buf();
        config.wal.dir = path.join("wal");
        config.wal.enabled = false;
        config.memtable.max_size = 256 * 1024 * 1024;
        config.compaction.background_enabled = false;
        let engine = block_on(async move { LsmTreeEngine::new(config).await })
            .expect("open f4kvs engine");
        Self {
            engine: Arc::new(engine),
        }
    }
}

impl BenchDatabase for F4kvsBenchDatabase {
    type C<'db> = F4kvsBenchDatabaseConnection
    where
        Self: 'db;

    fn db_type_name() -> &'static str {
        "f4kvs"
    }

    fn connect(&self) -> Self::C<'_> {
        F4kvsBenchDatabaseConnection {
            engine: Arc::clone(&self.engine),
            sync: true,
        }
    }

    fn compact(&mut self) -> bool {
        let engine = Arc::clone(&self.engine);
        block_on(async move { engine.compact().await }).expect("compact");
        true
    }
}

pub struct F4kvsBenchDatabaseConnection {
    engine: Arc<LsmTreeEngine>,
    sync: bool,
}

impl BenchDatabaseConnection for F4kvsBenchDatabaseConnection {
    type W<'db> = F4kvsBenchWriteTransaction
    where
        Self: 'db;
    type R<'db> = F4kvsBenchReadTransaction
    where
        Self: 'db;

    fn set_sync(&mut self, sync: bool) -> bool {
        self.sync = sync;
        true
    }

    fn write_transaction(&self) -> Self::W<'_> {
        F4kvsBenchWriteTransaction {
            engine: Arc::clone(&self.engine),
            sync: self.sync,
            pending_puts: Vec::new(),
            pending_deletes: HashSet::new(),
        }
    }

    fn read_transaction(&self) -> Self::R<'_> {
        F4kvsBenchReadTransaction {
            engine: Arc::clone(&self.engine),
        }
    }
}

pub struct F4kvsBenchReadTransaction {
    engine: Arc<LsmTreeEngine>,
}

impl BenchReadTransaction for F4kvsBenchReadTransaction {
    type T<'txn> = F4kvsBenchReader
    where
        Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        F4kvsBenchReader {
            engine: Arc::clone(&self.engine),
        }
    }
}

pub struct F4kvsBenchReader {
    engine: Arc<LsmTreeEngine>,
}

impl BenchReader for F4kvsBenchReader {
    type Output<'out> = F4kvsBytes
    where
        Self: 'out;

    fn get<'a>(&'a mut self, key: &[u8]) -> Option<Self::Output<'a>> {
        let engine = Arc::clone(&self.engine);
        let key = bytes_to_hex(key);
        match block_on(async move { engine.get(&key).await }).expect("get") {
            Some(Value::Bytes(bytes)) => Some(F4kvsBytes(bytes)),
            Some(other) => Some(F4kvsBytes(value_to_bytes(other))),
            None => None,
        }
    }

    fn len(&mut self) -> u64 {
        let engine = Arc::clone(&self.engine);
        block_on(async move { engine.count().await }).expect("count")
    }
}

fn value_to_bytes(value: Value) -> Vec<u8> {
    match value {
        Value::Bytes(b) => b,
        Value::String(s) => s.into_bytes(),
        Value::Json(j) => j.to_string().into_bytes(),
        Value::Int64(n) => n.to_string().into_bytes(),
        Value::UInt64(n) => n.to_string().into_bytes(),
        Value::Float64(n) => n.to_string().into_bytes(),
        Value::Bool(b) => b.to_string().into_bytes(),
        Value::Null => Vec::new(),
    }
}

pub struct F4kvsBenchWriteTransaction {
    engine: Arc<LsmTreeEngine>,
    sync: bool,
    pending_puts: Vec<(String, Value)>,
    pending_deletes: HashSet<String>,
}

impl F4kvsBenchWriteTransaction {
    fn flush_pending_writes(&mut self) -> Result<(), ()> {
        while !self.pending_puts.is_empty() {
            let chunk_len = self.pending_puts.len().min(MAX_BATCH_SIZE);
            let batch: Vec<_> = self.pending_puts.drain(..chunk_len).collect();
            let engine = Arc::clone(&self.engine);
            block_on(async move { engine.batch_put(batch).await }).map_err(|_| ())?;
        }
        for key in self.pending_deletes.drain() {
            let engine = Arc::clone(&self.engine);
            block_on(async move { engine.delete(&key).await }).map_err(|_| ())?;
        }
        Ok(())
    }

    fn flush_pending(&mut self) -> Result<(), ()> {
        self.flush_pending_writes()?;
        if self.sync {
            let engine = Arc::clone(&self.engine);
            block_on(async move { engine.flush().await }).map_err(|_| ())?;
        }
        Ok(())
    }
}

impl BenchWriteTransaction for F4kvsBenchWriteTransaction {
    type W<'txn> = F4kvsBenchInserter<'txn>
    where
        Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        F4kvsBenchInserter { txn: self }
    }

    fn commit(mut self) -> Result<(), ()> {
        self.flush_pending()
    }
}

pub struct F4kvsBenchInserter<'a> {
    txn: &'a mut F4kvsBenchWriteTransaction,
}

impl BenchInserter for F4kvsBenchInserter<'_> {
    type Output<'out> = F4kvsBytes
    where
        Self: 'out;

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        let key = bytes_to_hex(key);
        self.txn.pending_deletes.remove(&key);
        self.txn
            .pending_puts
            .push((key, Value::Bytes(value.to_vec())));
        if self.txn.pending_puts.len() >= MAX_BATCH_SIZE {
            self.txn.flush_pending_writes()?;
        }
        Ok(())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        let key = bytes_to_hex(key);
        self.txn.pending_puts.retain(|(k, _)| k != &key);
        self.txn.pending_deletes.insert(key);
        Ok(())
    }
}

