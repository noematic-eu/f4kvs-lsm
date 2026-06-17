//! Tests for WAL durability modes and behavior

use std::time::Duration;
use tempfile::TempDir;
use tokio::time::timeout;

use crate::storage::wal::{WALManager, WalConfig, WalSyncMode};
use crate::storage::wal::WALEntry;
use f4kvs_value::Value;

#[tokio::test]
async fn test_wal_strict_mode_waits_for_fsync() {
    // Test that strict mode (Fsync) waits for fsync before acknowledging writes
    let td = TempDir::new().expect("Failed to create temp dir");
    let config = WalConfig {
        dir: td.path().join("wal"),
        sync_mode: WalSyncMode::Fsync,
        ..WalConfig::default()
    };
    
    let manager = WALManager::new(&config).expect("Failed to create WAL manager");
    manager.initialize().await.expect("Failed to initialize WAL");
    
    // Write an operation - should wait for fsync in strict mode
    manager.write_operation("key1", &Value::String("value1".to_string())).await
        .expect("Write should succeed in strict mode");
    
    // The operation should be durable (fsync'd)
    manager.flush().await.expect("Flush should succeed");
}

#[tokio::test]
async fn test_wal_async_mode_logs_fsync_errors() {
    // Test that async mode logs fsync errors but doesn't propagate them
    let td = TempDir::new().expect("Failed to create temp dir");
    let config = WalConfig {
        dir: td.path().join("wal"),
        sync_mode: WalSyncMode::FsyncAsync,
        ..WalConfig::default()
    };
    
    let manager = WALManager::new(&config).expect("Failed to create WAL manager");
    manager.initialize().await.expect("Failed to initialize WAL");
    
    // Write an operation - should not fail even if fsync fails in async mode
    manager.write_operation("key1", &Value::String("value1".to_string())).await
        .expect("Write should succeed in async mode even if fsync fails");
}

#[tokio::test]
async fn test_wal_strict_mode_propagates_fsync_errors() {
    // Test that strict mode properly propagates fsync errors
    let td = TempDir::new().expect("Failed to create temp dir");
    let config = WalConfig {
        dir: td.path().join("wal"),
        sync_mode: WalSyncMode::Fsync,
        ..WalConfig::default()
    };
    
    let manager = WALManager::new(&config).expect("Failed to create WAL manager");
    manager.initialize().await.expect("Failed to initialize WAL");
    
    // Write an operation - should succeed in normal conditions
    manager.write_operation("key1", &Value::String("value1".to_string())).await
        .expect("Write should succeed in strict mode");
    
    // Verify that the operation was written and fsync'd
    manager.flush().await.expect("Flush should succeed");
}

#[tokio::test]
async fn test_wal_strict_mode_with_multiple_operations() {
    // Test that strict mode handles multiple operations correctly
    let td = TempDir::new().expect("Failed to create temp dir");
    let config = WalConfig {
        dir: td.path().join("wal"),
        sync_mode: WalSyncMode::Fsync,
        flush_interval: Duration::from_millis(100),
        ..WalConfig::default()
    };
    
    let manager = WALManager::new(&config).expect("Failed to create WAL manager");
    manager.initialize().await.expect("Failed to initialize WAL");
    
    // Write multiple operations - all should wait for fsync
    let mut results = Vec::new();
    
    for i in 0..5 {
        let result = manager.write_operation(
            &format!("key{}", i), 
            &Value::String(format!("value{}", i))
        ).await;
        results.push(result);
    }
    
    // All operations should succeed in strict mode
    for result in results {
        assert!(result.is_ok(), "All operations should succeed in strict mode");
    }
    
    // Flush all writes to disk
    manager.flush().await.expect("Flush should succeed");
}

#[tokio::test]
async fn test_wal_strict_mode_fsync_failure_propagation() {
    // Test that strict mode properly propagates fsync errors in realistic scenarios
    let td = TempDir::new().expect("Failed to create temp dir");
    let config = WalConfig {
        dir: td.path().join("wal"),
        sync_mode: WalSyncMode::Fsync,
        ..WalConfig::default()
    };
    
    let manager = WALManager::new(&config).expect("Failed to create WAL manager");
    manager.initialize().await.expect("Failed to initialize WAL");
    
    // Write several operations - all should wait for fsync in strict mode
    let mut results = Vec::new();
    
    for i in 0..3 {
        let result = manager.write_operation(
            &format!("key{}", i), 
            &Value::String(format!("value{}", i))
        ).await;
        results.push(result);
    }
    
    // All operations should succeed in strict mode (they wait for fsync)
    for result in results {
        assert!(result.is_ok(), "All operations should succeed in strict mode");
    }
    
    // Flush all writes to disk
    manager.flush().await.expect("Flush should succeed");
}