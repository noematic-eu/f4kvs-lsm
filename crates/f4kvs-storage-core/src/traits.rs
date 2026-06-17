//! Core storage traits and interfaces for F4KVS storage backends

use crate::{Result, StorageStats, Value};
use async_trait::async_trait;
use std::time::Duration;

/// Durability mode for write operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteDurability {
    /// The write is acknowledged once accepted by the storage pipeline.
    /// In write-back modes this may not be durable yet.
    Acknowledged,
    /// The write is acknowledged only after a durability barrier has completed.
    Durable,
}

/// Key-value iterator trait for range scanning
pub trait KeyValueIterator {
    /// Get the next key-value pair from the iterator
    fn next(&mut self) -> Option<(String, Value)>;
}

/// Core storage engine trait
#[async_trait]
pub trait StorageEngine: Send + Sync + std::fmt::Debug {
    // Basic key-value operations
    /// Get a value by key
    ///
    /// Retrieves the value associated with the given key from the storage backend.
    /// Returns `None` if the key does not exist.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up. Must be a valid UTF-8 string.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(Value))` - The value associated with the key, if found
    /// * `Ok(None)` - The key does not exist in storage
    /// * `Err(F4KvsError)` - An error occurred during the operation
    ///
    /// # Errors
    ///
    /// This function may return errors in the following cases:
    /// * `F4KvsError::Storage` - Storage backend error (I/O failure, corruption, etc.)
    /// * `F4KvsError::InvalidKey` - Invalid key format or length
    /// * `F4KvsError::Serialization` - Error deserializing stored value
    async fn get(&self, key: &str) -> Result<Option<Value>>;

    /// Put a key-value pair
    ///
    /// Stores a value associated with the given key. If the key already exists,
    /// the value will be overwritten.
    ///
    /// Durability semantics:
    /// - This method is an acknowledged write by default.
    /// - Some backends (e.g. write-back cache) may return before data is
    ///   persisted to durable storage.
    /// - Use [`StorageEngine::put_durable`] when callers require durable
    ///   confirmation before continuing.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to store. Must be a valid UTF-8 string.
    /// * `value` - The value to store. Can be any supported `Value` type.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - The value was successfully stored
    /// * `Err(F4KvsError)` - An error occurred during the operation
    ///
    /// # Errors
    ///
    /// This function may return errors in the following cases:
    /// * `F4KvsError::Storage` - Storage backend error (I/O failure, disk full, etc.)
    /// * `F4KvsError::InvalidKey` - Invalid key format or length
    /// * `F4KvsError::Serialization` - Error serializing the value
    /// * `F4KvsError::ResourceLimit` - Storage limit exceeded (memory or disk)
    async fn put(&self, key: &str, value: &Value) -> Result<()>;

    /// Put a key-value pair and wait for a durability barrier.
    ///
    /// Default implementation performs `put` followed by `flush`.
    /// Backends can override for stronger guarantees or better performance.
    async fn put_durable(&self, key: &str, value: &Value) -> Result<()> {
        self.put(key, value).await?;
        self.flush().await
    }

    /// Delete a key
    ///
    /// Removes the key-value pair from storage. If the key does not exist,
    /// this operation is a no-op and returns successfully.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to delete. Must be a valid UTF-8 string.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - The key was deleted (or did not exist)
    /// * `Err(F4KvsError)` - An error occurred during the operation
    ///
    /// # Errors
    ///
    /// This function may return errors in the following cases:
    /// * `F4KvsError::Storage` - Storage backend error (I/O failure, etc.)
    /// * `F4KvsError::InvalidKey` - Invalid key format or length
    async fn delete(&self, key: &str) -> Result<()>;

    /// Check if a key exists
    ///
    /// Checks whether a key exists in storage without retrieving its value.
    /// This is more efficient than calling `get()` when you only need to check existence.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to check. Must be a valid UTF-8 string.
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - The key exists in storage
    /// * `Ok(false)` - The key does not exist
    /// * `Err(F4KvsError)` - An error occurred during the operation
    ///
    /// # Errors
    ///
    /// This function may return errors in the following cases:
    /// * `F4KvsError::Storage` - Storage backend error (I/O failure, etc.)
    /// * `F4KvsError::InvalidKey` - Invalid key format or length
    async fn exists(&self, key: &str) -> Result<bool>;

    // Column family operations
    /// Get a value by key from a specific column family
    async fn get_cf(&self, key: &str, column_family: &str) -> Result<Option<Value>>;
    /// Put a key-value pair in a specific column family
    async fn put_cf(&self, key: &str, value: &Value, column_family: &str) -> Result<()>;
    /// Delete a key from a specific column family
    async fn delete_cf(&self, key: &str, column_family: &str) -> Result<()>;
    /// Check if a key exists in a specific column family
    async fn exists_cf(&self, key: &str, column_family: &str) -> Result<bool>;

    // TTL operations
    /// Put a key-value pair with TTL
    async fn put_with_ttl(&self, key: &str, value: &Value, ttl: Duration) -> Result<()>;
    /// Put a key-value pair with TTL in a specific column family
    async fn put_cf_with_ttl(
        &self,
        key: &str,
        value: &Value,
        column_family: &str,
        ttl: Duration,
    ) -> Result<()>;
    /// Get the TTL for a key
    async fn get_ttl(&self, key: &str) -> Result<Option<Duration>>;
    /// Get the TTL for a key in a specific column family
    async fn get_ttl_cf(&self, key: &str, column_family: &str) -> Result<Option<Duration>>;

    // Batch operations
    /// Put multiple key-value pairs
    async fn batch_put(&self, items: Vec<(String, Value)>) -> Result<()>;
    /// Get multiple values by keys
    async fn batch_get(&self, keys: Vec<String>) -> Result<Vec<Option<Value>>>;
    /// Delete multiple keys
    async fn batch_delete(&self, keys: Vec<String>) -> Result<()>;
    /// Put multiple key-value pairs in a specific column family
    async fn batch_put_cf(&self, items: Vec<(String, Value)>, column_family: &str) -> Result<()>;
    /// Get multiple values by keys from a specific column family
    async fn batch_get_cf(
        &self,
        keys: Vec<String>,
        column_family: &str,
    ) -> Result<Vec<Option<Value>>>;
    /// Delete multiple keys from a specific column family
    async fn batch_delete_cf(&self, keys: Vec<String>, column_family: &str) -> Result<()>;

    // Scan operations - keys only
    /// Scan keys with a prefix
    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>>;
    /// Scan keys in a range
    async fn scan_range(&self, start: &str, end: &str) -> Result<Vec<String>>;
    /// Scan keys in a range with a limit
    async fn scan_range_limit(&self, start: &str, end: &str, limit: usize) -> Result<Vec<String>>;
    /// Scan keys with a prefix in a specific column family
    async fn scan_prefix_cf(&self, prefix: &str, column_family: &str) -> Result<Vec<String>>;
    /// Scan keys in a range in a specific column family
    async fn scan_range_cf(
        &self,
        start: &str,
        end: &str,
        column_family: &str,
    ) -> Result<Vec<String>>;
    /// Scan keys in a range with a limit in a specific column family
    async fn scan_range_limit_cf(
        &self,
        start: &str,
        end: &str,
        limit: usize,
        column_family: &str,
    ) -> Result<Vec<String>>;

    // Scan operations - with values
    /// Scan keys and values with a prefix
    async fn scan_prefix_with_values(&self, prefix: &str) -> Result<Vec<(String, Value)>>;
    /// Scan keys and values in a range
    async fn scan_range_with_values(&self, start: &str, end: &str) -> Result<Vec<(String, Value)>>;
    /// Scan keys and values in a range with a limit
    async fn scan_range_limit_with_values(
        &self,
        start: &str,
        end: &str,
        limit: usize,
    ) -> Result<Vec<(String, Value)>>;
    /// Scan keys and values with a prefix in a specific column family
    async fn scan_prefix_with_values_cf(
        &self,
        prefix: &str,
        column_family: &str,
    ) -> Result<Vec<(String, Value)>>;
    /// Scan keys and values in a range in a specific column family
    async fn scan_range_with_values_cf(
        &self,
        start: &str,
        end: &str,
        column_family: &str,
    ) -> Result<Vec<(String, Value)>>;
    /// Scan all keys and values
    async fn scan_all(&self) -> Result<Vec<(String, Value)>>;
    /// Scan all keys and values in a specific column family
    async fn scan_all_cf(&self, column_family: &str) -> Result<Vec<(String, Value)>>;

    // Iterator support
    /// Create an iterator for a range of keys
    async fn iter_range(&self, start: &str, end: &str) -> Result<Box<dyn KeyValueIterator + Send>>;
    /// Create an iterator for a range of keys in a specific column family
    async fn iter_range_cf(
        &self,
        start: &str,
        end: &str,
        column_family: &str,
    ) -> Result<Box<dyn KeyValueIterator + Send>>;

    // Maintenance operations
    /// Flush all pending writes to disk
    async fn flush(&self) -> Result<()>;
    /// Compact the storage to reclaim space
    async fn compact(&self) -> Result<()>;
    /// Get storage statistics
    async fn stats(&self) -> Result<StorageStats>;

    // Key enumeration operations
    /// Get all keys in the storage engine
    /// Default implementation uses scan_prefix with empty string
    async fn keys(&self) -> Result<Vec<String>> {
        self.scan_prefix("").await
    }

    /// Get the count of all keys in the storage engine
    /// Default implementation counts keys from keys() method
    async fn count(&self) -> Result<u64> {
        let keys = self.keys().await?;
        Ok(keys.len() as u64)
    }

    /// Clear all data from the storage engine
    /// This is a dangerous operation and should be used with caution
    /// Default implementation is not provided - each engine must implement this
    async fn clear(&self) -> Result<()> {
        Err(crate::F4KvsError::storage(
            "Clear operation not supported by this storage backend",
        ))
    }

    // Column family management
    /// Create a new column family
    async fn create_column_family(&mut self, name: &str) -> Result<()>;
    /// Drop an existing column family
    async fn drop_column_family(&mut self, name: &str) -> Result<()>;
    /// List all column families
    fn list_column_families(&self) -> Vec<String>;

    // Transaction support (optional - can return error if not supported)
    /// Begin a new transaction
    async fn begin_transaction(&self) -> Result<Box<dyn Transaction + Send + Sync>> {
        Err(crate::F4KvsError::storage(
            "Transactions not supported by this storage backend",
        ))
    }

    /// Shutdown the storage engine and clean up resources
    ///
    /// Properly shuts down the storage engine, stopping any background tasks,
    /// flushing pending operations, and releasing resources. This should be
    /// called before dropping the engine to ensure clean shutdown.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Engine shut down successfully
    /// * `Err(F4KvsError)` - Error occurred during shutdown
    async fn shutdown(&self) -> Result<()> {
        // Default implementation does nothing - engines that need shutdown
        // should override this method
        Ok(())
    }

    /// Get detailed operation metrics summary (optional)
    ///
    /// Returns detailed per-operation timing metrics if supported by this storage backend.
    /// Returns `None` if detailed metrics are not available.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(DetailedMetricsSummary))` - Detailed metrics available
    /// * `Ok(None)` - Detailed metrics not supported by this backend
    /// * `Err(F4KvsError)` - Error retrieving metrics
    async fn get_detailed_metrics_summary(&self) -> Result<Option<String>> {
        // Default implementation returns None - engines that support detailed metrics
        // should override this method to return JSON-serialized summary
        Ok(None)
    }

    /// Export detailed metrics in Prometheus format (optional)
    ///
    /// Returns Prometheus-formatted detailed metrics if supported by this storage backend.
    /// Returns `None` if detailed metrics are not available.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(String))` - Prometheus metrics string
    /// * `Ok(None)` - Detailed metrics not supported by this backend
    /// * `Err(F4KvsError)` - Error exporting metrics
    async fn export_detailed_metrics_prometheus(&self) -> Result<Option<String>> {
        // Default implementation returns None - engines that support detailed metrics
        // should override this method
        Ok(None)
    }
}

/// Alias for backward compatibility
pub use StorageEngine as Storage;

/// Transaction trait for ACID operations
#[async_trait]
pub trait Transaction: Send + Sync {
    /// Get a value by key within the transaction
    async fn get(&self, key: &str) -> Result<Option<Value>>;
    /// Put a key-value pair within the transaction
    async fn put(&mut self, key: &str, value: &Value) -> Result<()>;
    /// Delete a key within the transaction
    async fn delete(&mut self, key: &str) -> Result<()>;
    /// Commit the transaction
    async fn commit(self: Box<Self>) -> Result<()>;
    /// Rollback the transaction
    async fn rollback(self: Box<Self>) -> Result<()>;
}

/// Snapshot trait for consistent point-in-time reads
pub trait Snapshot: Send + Sync {
    /// Get a value by key from the snapshot
    fn get(&self, key: &str) -> Result<Option<Value>>;
    /// Check if a key exists in the snapshot
    fn exists(&self, key: &str) -> Result<bool>;
    /// Scan keys with a prefix from the snapshot
    fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>>;
    /// Scan keys in a range from the snapshot
    fn scan_range(&self, start: &str, end: &str) -> Result<Vec<String>>;
}

/// Compaction trait for storage maintenance
#[async_trait]
pub trait Compactable {
    /// Trigger manual compaction
    async fn compact_range(&self, start: &str, end: &str) -> Result<()>;

    /// Get compaction statistics
    async fn compaction_stats(&self) -> Result<CompactionStats>;

    /// Configure automatic compaction
    async fn configure_compaction(&mut self, config: CompactionConfig) -> Result<()>;
}

/// Compaction statistics
#[derive(Debug, Clone)]
pub struct CompactionStats {
    /// Statistics for each level
    pub levels: Vec<LevelStats>,
    /// Number of pending compactions
    pub pending_compactions: u32,
    /// Number of running compactions
    pub running_compactions: u32,
    /// Total bytes compacted
    pub total_bytes_compacted: u64,
    /// Total compaction time in milliseconds
    pub total_compaction_time_ms: u64,
}

/// Statistics for a single LSM level
#[derive(Debug, Clone)]
pub struct LevelStats {
    /// Level number (0-based)
    pub level: u32,
    /// Number of files in this level
    pub files: u32,
    /// Total size of files in bytes
    pub size_bytes: u64,
    /// Compaction score for this level
    pub score: f64,
}

/// Compaction configuration
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Maximum number of background compaction threads
    pub max_background_compactions: u32,
    /// Maximum bytes for level base
    pub max_bytes_for_level_base: u64,
    /// Number of L0 files to trigger compaction
    pub level0_file_num_compaction_trigger: u32,
    /// Number of L0 files to slow down writes
    pub level0_slowdown_writes_trigger: u32,
    /// Number of L0 files to stop writes
    pub level0_stop_writes_trigger: u32,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            max_background_compactions: 1,
            max_bytes_for_level_base: 256 * 1024 * 1024, // 256MB
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
        }
    }
}

/// Health check trait for monitoring storage health
#[async_trait]
pub trait HealthCheck {
    /// Perform a health check and return the current status
    async fn health_check(&self) -> Result<HealthStatus>;
}

/// Health status levels
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthStatus {
    /// System is operating normally
    Healthy,
    /// System is experiencing some issues but still functional
    Degraded(String),
    /// System is experiencing critical issues
    Unhealthy(String),
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthStatus::Healthy => write!(f, "healthy"),
            HealthStatus::Degraded(reason) => write!(f, "degraded: {reason}"),
            HealthStatus::Unhealthy(reason) => write!(f, "unhealthy: {reason}"),
        }
    }
}
