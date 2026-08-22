use f4kvs_lsm::{LsmConfig, LsmTreeEngine};
use f4kvs_storage_core::traits::StorageEngine;
use f4kvs_value::Value;
use std::fs;
use tempfile::TempDir;

#[tokio::main]
async fn main() {
    let dir = TempDir::new().unwrap();
    let mut cfg = LsmConfig::default();
    cfg.data_dir = dir.path().to_path_buf();
    cfg.wal.dir = dir.path().join("wal");
    let eng = LsmTreeEngine::new(cfg).await.unwrap();
    eng.put("k", &Value::Bytes(b"x".to_vec())).await.unwrap();
    eng.flush().await.unwrap();
    println!("after put+flush: {:?}", eng.get("k").await.unwrap());
    eng.delete("k").await.unwrap();
    println!("after delete: {:?}", eng.get("k").await.unwrap());
    eng.flush().await.unwrap();
    println!("after delete+flush: {:?}", eng.get("k").await.unwrap());
    // list SSTables
    for e in fs::read_dir(dir.path()).unwrap() {
        let e = e.unwrap();
        println!("file: {:?} size={}", e.path(), e.metadata().unwrap().len());
    }
}
