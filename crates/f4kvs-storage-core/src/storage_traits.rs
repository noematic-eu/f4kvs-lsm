#![allow(missing_docs)]
//! Storage traits and types for F4KVS Core

use crate::{Result, Value};
use async_trait::async_trait;

/// Core storage engine trait
#[async_trait]
pub trait StorageEngine: Send + Sync {
    /// Get a value by key
    async fn get(&self, key: &str) -> Result<Option<Value>>;

    /// Put a key-value pair
    async fn put(&self, key: &str, value: &Value) -> Result<()>;

    /// Delete a key
    async fn delete(&self, key: &str) -> Result<()>;

    /// Check if a key exists
    async fn exists(&self, key: &str) -> Result<bool>;

    /// Get all keys (use with caution)
    async fn keys(&self) -> Result<Vec<String>>;

    /// Get approximate count of keys
    async fn count(&self) -> Result<u64>;

    /// Get storage statistics
    async fn stats(&self) -> Result<StorageStats>;

    /// Clear all data (dangerous!)
    async fn clear(&self) -> Result<()>;

    // Batch operations
    /// Put multiple key-value pairs atomically
    async fn batch_put(&self, items: Vec<(String, Value)>) -> Result<()>;

    /// Get multiple values by keys
    async fn batch_get(&self, keys: Vec<String>) -> Result<Vec<Option<Value>>>;

    /// Delete multiple keys atomically
    async fn batch_delete(&self, keys: Vec<String>) -> Result<()>;

    // Advanced querying operations
    /// Get all keys with a given prefix
    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>>;

    /// Get all keys in a range (inclusive start, exclusive end)
    async fn scan_range(&self, start: &str, end: &str) -> Result<Vec<String>>;

    /// Get key-value pairs with a given prefix
    async fn scan_prefix_pairs(&self, prefix: &str) -> Result<Vec<(String, Value)>>;

    /// Get key-value pairs in a range (inclusive start, exclusive end)
    async fn scan_range_pairs(&self, start: &str, end: &str) -> Result<Vec<(String, Value)>>;

    /// Get approximate count of keys with a given prefix
    async fn count_prefix(&self, prefix: &str) -> Result<u64>;

    /// Get approximate count of keys in a range
    async fn count_range(&self, start: &str, end: &str) -> Result<u64>;

    /// Flush any pending writes to persistent storage
    /// For in-memory storage, this is typically a no-op
    /// For persistent storage, this ensures data is written to disk
    async fn flush(&self) -> Result<()>;
}

/// Storage statistics
#[derive(Debug, Clone)]
pub struct StorageStats {
    /// Total number of keys
    pub key_count: u64,
    /// Approximate total memory usage in bytes
    pub memory_usage: u64,
    /// Number of operations performed
    pub total_operations: u64,
    /// Number of get operations
    pub get_operations: u64,
    /// Number of put operations
    pub put_operations: u64,
    /// Number of delete operations
    pub delete_operations: u64,
    /// Number of scan operations
    pub scan_operations: u64,
    /// Average key size in bytes
    pub average_key_size: f64,
    /// Average value size in bytes
    pub average_value_size: f64,
    /// Peak memory usage in bytes
    pub peak_memory_usage: u64,
    /// Number of cache hits (if caching is enabled)
    pub cache_hits: u64,
    /// Number of cache misses (if caching is enabled)
    pub cache_misses: u64,
}
