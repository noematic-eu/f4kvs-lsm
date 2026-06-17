//! Storage adapter for converting f4kvs_storage::StorageEngine to f4kvs_value::StorageEngine
//!
//! This module provides an adapter that bridges the gap between the storage layer's
//! `StorageEngine` trait and the core layer's `StorageEngine` trait, allowing
//! persistent storage engines to be used with components that expect the core interface.

use crate::traits::StorageEngine as StorageEngineTrait;
use async_trait::async_trait;
use crate::storage_traits::{StorageEngine as CoreStorageEngine, StorageStats};
use f4kvs_value::{F4KvsError, Result, Value};
use std::sync::Arc;

/// Adapter to convert f4kvs_storage::StorageEngine to f4kvs_value::StorageEngine
///
/// This adapter wraps a storage layer engine and implements the core layer's
/// `StorageEngine` trait, enabling persistent storage engines to be used
/// with components that expect the core interface (such as the server and QL).
///
/// # Example
///
/// ```rust,ignore
/// // Requires `f4kvs-storage` from the f4kvs-v2 monorepo.
/// use f4kvs_storage_core::adapter::StorageAdapter;
/// use f4kvs_storage_core::storage_traits::StorageEngine;
/// use f4kvs_storage::{create_storage, StorageBackend, StorageConfig};
/// use f4kvs_value::Value;
/// use std::sync::Arc;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = StorageConfig {
///     backend: StorageBackend::LsmTree,
///     data_dir: std::path::PathBuf::from("./data"),
///     ..Default::default()
/// };
/// let storage_engine = create_storage(&config).await?;
/// let adapter = StorageAdapter::new(Arc::new(storage_engine));
/// adapter.put("key", &Value::String("value".to_string())).await?;
/// # Ok(())
/// # }
/// ```
pub struct StorageAdapter {
    inner: Arc<Box<dyn StorageEngineTrait + Send + Sync>>,
}

impl StorageAdapter {
    /// Create a new storage adapter
    ///
    /// # Arguments
    ///
    /// * `inner` - The storage layer engine to wrap (can be Box or Arc<Box>)
    ///
    /// # Returns
    ///
    /// A new `StorageAdapter` instance
    pub fn new(inner: Arc<Box<dyn StorageEngineTrait + Send + Sync>>) -> Self {
        Self { inner }
    }

    /// Create a new storage adapter from a Box (convenience method)
    ///
    /// # Arguments
    ///
    /// * `inner` - The storage layer engine as a Box (from f4kvs_storage::create_storage)
    ///
    /// # Returns
    ///
    /// A new `StorageAdapter` instance
    pub fn from_box(inner: Box<dyn StorageEngineTrait + Send + Sync>) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

#[async_trait]
impl CoreStorageEngine for StorageAdapter {
    async fn get(&self, key: &str) -> Result<Option<Value>> {
        self.inner
            .get(key)
            .await
            .map_err(|e| F4KvsError::storage(format!("Storage error: {}", e)))
    }

    async fn put(&self, key: &str, value: &Value) -> Result<()> {
        self.inner
            .put(key, value)
            .await
            .map_err(|e| F4KvsError::storage(format!("Storage error: {}", e)))
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner
            .delete(key)
            .await
            .map_err(|e| F4KvsError::storage(format!("Storage error: {}", e)))
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.inner
            .exists(key)
            .await
            .map_err(|e| F4KvsError::storage(format!("Storage error: {}", e)))
    }

    async fn keys(&self) -> Result<Vec<String>> {
        self.inner
            .keys()
            .await
            .map_err(|e| F4KvsError::storage(format!("Storage error: {}", e)))
    }

    async fn count(&self) -> Result<u64> {
        self.inner
            .count()
            .await
            .map_err(|e| F4KvsError::storage(format!("Storage error: {}", e)))
    }

    async fn stats(&self) -> Result<StorageStats> {
        let storage_stats = self
            .inner
            .stats()
            .await
            .map_err(|e| F4KvsError::storage(format!("Storage error: {}", e)))?;
        Ok(StorageStats {
            key_count: storage_stats.total_keys,
            memory_usage: storage_stats.memory_stats.total_memory_usage,
            total_operations: storage_stats.io_stats.read_stats.operation_count
                + storage_stats.io_stats.write_stats.operation_count,
            get_operations: storage_stats.io_stats.read_stats.operation_count,
            put_operations: storage_stats.io_stats.write_stats.operation_count,
            delete_operations: 0,  // Not tracked separately in storage stats
            scan_operations: 0,    // Not tracked separately in storage stats
            average_key_size: 0.0, // Not directly available
            average_value_size: if storage_stats.total_keys > 0 {
                storage_stats.total_size_bytes as f64 / storage_stats.total_keys as f64
            } else {
                0.0
            },
            peak_memory_usage: storage_stats.memory_stats.total_memory_usage,
            cache_hits: storage_stats.cache_stats.block_cache.hit_count,
            cache_misses: storage_stats.cache_stats.block_cache.miss_count,
        })
    }

    async fn flush(&self) -> Result<()> {
        self.inner
            .flush()
            .await
            .map_err(|e| F4KvsError::storage(format!("Storage error: {}", e)))
    }

    async fn clear(&self) -> Result<()> {
        self.inner
            .clear()
            .await
            .map_err(|e| F4KvsError::storage(format!("Storage error: {}", e)))
    }

    async fn batch_put(&self, items: Vec<(String, Value)>) -> Result<()> {
        self.inner
            .batch_put(items)
            .await
            .map_err(|e| F4KvsError::storage(format!("Storage error: {}", e)))
    }

    async fn batch_get(&self, keys: Vec<String>) -> Result<Vec<Option<Value>>> {
        self.inner
            .batch_get(keys)
            .await
            .map_err(|e| F4KvsError::storage(format!("Storage error: {}", e)))
    }

    async fn batch_delete(&self, keys: Vec<String>) -> Result<()> {
        self.inner
            .batch_delete(keys)
            .await
            .map_err(|e| F4KvsError::storage(format!("Storage error: {}", e)))
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner
            .scan_prefix(prefix)
            .await
            .map_err(|e| F4KvsError::storage(format!("Storage error: {}", e)))
    }

    async fn scan_range(&self, start: &str, end: &str) -> Result<Vec<String>> {
        self.inner
            .scan_range(start, end)
            .await
            .map_err(|e| F4KvsError::storage(format!("Storage error: {}", e)))
    }

    async fn scan_prefix_pairs(&self, prefix: &str) -> Result<Vec<(String, Value)>> {
        self.inner
            .scan_prefix_with_values(prefix)
            .await
            .map_err(|e| F4KvsError::storage(format!("Storage error: {}", e)))
    }

    async fn scan_range_pairs(&self, start: &str, end: &str) -> Result<Vec<(String, Value)>> {
        self.inner
            .scan_range_with_values(start, end)
            .await
            .map_err(|e| F4KvsError::storage(format!("Storage error: {}", e)))
    }

    async fn count_prefix(&self, prefix: &str) -> Result<u64> {
        let keys = self
            .inner
            .scan_prefix(prefix)
            .await
            .map_err(|e| F4KvsError::storage(format!("Storage error: {}", e)))?;
        Ok(keys.len() as u64)
    }

    async fn count_range(&self, start: &str, end: &str) -> Result<u64> {
        let keys = self
            .inner
            .scan_range(start, end)
            .await
            .map_err(|e| F4KvsError::storage(format!("Storage error: {}", e)))?;
        Ok(keys.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    // Note: Adapter tests are in f4kvs-server and f4kvs-ql-storage where the adapter is actually used
    // This avoids type path issues between f4kvs_storage::traits and crate::traits
    // The adapter functionality is verified through integration tests in those crates
}
