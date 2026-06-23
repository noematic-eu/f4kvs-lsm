//! Memtable implementation for LSM Tree Engine

use crate::core::config::MemtableConfig;
use crate::error::Result;
use f4kvs_value::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Effect of a put on memtable key visibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutEffect {
    Inserted,
    UpdatedLive,
    Resurrected,
}

/// Result of a single-key memtable lookup.
#[derive(Debug, Clone, PartialEq)]
pub enum MemtableLookupResult {
    Found(Value),
    Tombstone,
    Missing,
}

/// Memtable for LSM Tree Engine
///
/// Provides fast in-memory storage with sorted key-value pairs.
/// When full, memtables are flushed to disk as SSTables.
pub struct Memtable {
    /// Configuration
    config: MemtableConfig,

    /// Sorted key-value storage
    data: Arc<RwLock<BTreeMap<String, Value>>>,

    /// Current size in bytes
    size: Arc<RwLock<usize>>,

    /// Number of entries
    entry_count: Arc<RwLock<usize>>,

    /// Timestamp when memtable was created
    #[allow(dead_code)] // Will be used for TTL and aging policies
    timestamp: std::time::SystemTime,
}

impl Memtable {
    /// Create a new memtable
    pub fn new(config: &MemtableConfig) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
            data: Arc::new(RwLock::new(BTreeMap::new())),
            size: Arc::new(RwLock::new(0)),
            entry_count: Arc::new(RwLock::new(0)),
            timestamp: std::time::SystemTime::now(),
        })
    }

    /// Put a key-value pair
    pub async fn put(&mut self, key: &str, value: &Value) -> Result<PutEffect> {
        let key_size = key.len();
        let value_size = Self::estimate_value_size(value);

        // Check if this is an update or new entry
        let old_value = {
            let data = self.data.read().await;
            data.get(key).cloned()
        };

        let effect = match &old_value {
            None => PutEffect::Inserted,
            Some(Value::Null) => PutEffect::Resurrected,
            Some(_) => PutEffect::UpdatedLive,
        };

        // Update size tracking
        {
            let mut size = self.size.write().await;
            if let Some(old_value) = old_value {
                // Update: subtract old size, add new size
                *size = size.saturating_sub(key_size + Self::estimate_value_size(&old_value));
                *size += key_size + value_size;
            } else {
                // New entry: add size and increment count
                *size += key_size + value_size;
                let mut count = self.entry_count.write().await;
                *count += 1;
            }
        }

        // Store the value
        {
            let mut data = self.data.write().await;
            data.insert(key.to_string(), value.clone());
        }

        Ok(effect)
    }

    /// Estimate the size of a value in bytes
    fn estimate_value_size(value: &Value) -> usize {
        match value {
            Value::String(s) => s.len(),
            Value::Int64(_) => 8,
            Value::UInt64(_) => 8,
            Value::Float64(_) => 8,
            Value::Bool(_) => 1,
            Value::Bytes(b) => b.len(),
            Value::Json(v) => v.to_string().len(),
            Value::Null => 0,
        }
    }

    /// Single-pass key lookup (found value, tombstone, or missing).
    pub async fn lookup(&self, key: &str) -> Result<MemtableLookupResult> {
        let data = self.data.read().await;
        match data.get(key) {
            Some(Value::Null) => Ok(MemtableLookupResult::Tombstone),
            Some(value) => Ok(MemtableLookupResult::Found(value.clone())),
            None => Ok(MemtableLookupResult::Missing),
        }
    }

    /// Get a value by key
    pub async fn get(&self, key: &str) -> Result<Option<Value>> {
        let data = self.data.read().await;
        if let Some(value) = data.get(key) {
            // Check if this is a tombstone (deleted marker)
            if matches!(value, Value::Null) {
                Ok(None)
            } else {
                Ok(Some(value.clone()))
            }
        } else {
            Ok(None)
        }
    }

    /// Delete a key (mark as deleted)
    pub async fn delete(&mut self, key: &str) -> Result<()> {
        let key_size = key.len();

        // Check if this is an update or new entry
        let old_value = {
            let data = self.data.read().await;
            data.get(key).cloned()
        };

        // Update size tracking
        {
            let mut size = self.size.write().await;
            if let Some(old_value) = old_value {
                *size = size.saturating_sub(key_size + Self::estimate_value_size(&old_value));
            }
            // Add size for tombstone marker
            *size += key_size + 1; // Value::Null is roughly 1 byte
        }

        // Store tombstone marker
        {
            let mut data = self.data.write().await;
            data.insert(key.to_string(), Value::Null);
        }

        Ok(())
    }

    /// Check if a key exists
    pub async fn exists(&self, key: &str) -> Result<bool> {
        let data = self.data.read().await;
        if let Some(value) = data.get(key) {
            Ok(!matches!(value, Value::Null))
        } else {
            Ok(false)
        }
    }

    /// Check if a key exists as a tombstone (deleted)
    pub async fn is_tombstone(&self, key: &str) -> Result<bool> {
        let data = self.data.read().await;
        if let Some(value) = data.get(key) {
            Ok(matches!(value, Value::Null))
        } else {
            Ok(false)
        }
    }

    /// Get current size in bytes
    pub async fn size(&self) -> usize {
        *self.size.read().await
    }

    /// Get number of entries
    pub async fn entry_count(&self) -> usize {
        *self.entry_count.read().await
    }

    /// Check if memtable is full
    pub async fn is_full(&self) -> bool {
        self.size().await >= self.config.max_size
    }

    /// Get configuration
    pub fn config(&self) -> &MemtableConfig {
        &self.config
    }

    /// Scan keys with a prefix
    pub async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let data = self.data.read().await;
        let mut keys = Vec::new();

        for (key, value) in data.range(prefix.to_string()..) {
            if !key.starts_with(prefix) {
                break;
            }
            if !matches!(value, Value::Null) {
                keys.push(key.clone());
            }
        }

        Ok(keys)
    }

    /// Scan keys in a range
    pub async fn scan_range(&self, start: &str, end: &str) -> Result<Vec<String>> {
        let entries = self.scan_range_layer(start, end).await?;
        Ok(entries
            .into_iter()
            .filter(|(_, _, deleted)| !deleted)
            .map(|(key, _, _)| key)
            .collect())
    }

    /// Scan prefix entries including tombstones (for layer merge).
    pub async fn scan_prefix_layer(&self, prefix: &str) -> Result<Vec<(String, Value, bool)>> {
        let data = self.data.read().await;
        let mut entries = Vec::new();
        for (key, value) in data.range(prefix.to_string()..) {
            if !key.starts_with(prefix) {
                break;
            }
            let deleted = matches!(value, Value::Null);
            entries.push((key.clone(), value.clone(), deleted));
        }
        Ok(entries)
    }

    /// Scan range entries including tombstones (for layer merge).
    pub async fn scan_range_layer(
        &self,
        start: &str,
        end: &str,
    ) -> Result<Vec<(String, Value, bool)>> {
        let data = self.data.read().await;
        let mut entries = Vec::new();
        let end_bound = crate::utils::exclusive_range_end(end);

        if let Some(end_bound) = end_bound {
            for (key, value) in data.range(start.to_string()..end_bound) {
                let deleted = matches!(value, Value::Null);
                entries.push((key.clone(), value.clone(), deleted));
            }
        } else {
            for (key, value) in data.range(start.to_string()..) {
                let deleted = matches!(value, Value::Null);
                entries.push((key.clone(), value.clone(), deleted));
            }
        }
        Ok(entries)
    }

    /// Get all entries for flushing
    pub async fn get_all_entries(&self) -> Vec<(String, Value, bool)> {
        let data = self.data.read().await;
        data.iter()
            .map(|(key, value)| (key.clone(), value.clone(), matches!(value, Value::Null)))
            .collect()
    }

    /// Clear all data (for testing)
    pub async fn clear(&mut self) {
        {
            let mut data = self.data.write().await;
            data.clear();
        }
        {
            let mut size = self.size.write().await;
            *size = 0;
        }
        {
            let mut count = self.entry_count.write().await;
            *count = 0;
        }
    }
}

impl Clone for Memtable {
    fn clone(&self) -> Self {
        // Use blocking reads for Clone since it can't be async
        let data = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async { self.data.read().await })
        });
        let size = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async { *self.size.read().await })
        });
        let entry_count = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async { *self.entry_count.read().await })
        });

        Self {
            config: self.config.clone(),
            data: Arc::new(RwLock::new(data.clone())),
            size: Arc::new(RwLock::new(size)),
            entry_count: Arc::new(RwLock::new(entry_count)),
            timestamp: self.timestamp,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;

    #[tokio::test]
    async fn test_memtable_basic_operations() {
        let config = MemtableConfig::default();
        let mut memtable = Memtable::new(&config).expect("test operation failed");

        // Test put and get
        memtable
            .put("key1", &Value::String("value1".to_string()))
            .await
            .expect("test operation failed");
        assert_eq!(
            memtable.get("key1").await.expect("test operation failed"),
            Some(Value::String("value1".to_string()))
        );

        // Test update
        memtable
            .put("key1", &Value::String("value2".to_string()))
            .await
            .expect("test operation failed");
        assert_eq!(
            memtable.get("key1").await.expect("test operation failed"),
            Some(Value::String("value2".to_string()))
        );

        // Test delete
        memtable
            .delete("key1")
            .await
            .expect("test operation failed");
        assert_eq!(
            memtable.get("key1").await.expect("test operation failed"),
            None
        );

        // Test exists
        assert!(!memtable
            .exists("key1")
            .await
            .expect("test operation failed"));
    }

    #[tokio::test]
    async fn test_memtable_scanning() {
        let config = MemtableConfig::default();
        let mut memtable = Memtable::new(&config).expect("test operation failed");

        // Add some test data
        memtable
            .put("user:1", &Value::String("alice".to_string()))
            .await
            .expect("test operation failed");
        memtable
            .put("user:2", &Value::String("bob".to_string()))
            .await
            .expect("test operation failed");
        memtable
            .put("user:3", &Value::String("charlie".to_string()))
            .await
            .expect("test operation failed");
        memtable
            .put("config:debug", &Value::Bool(true))
            .await
            .expect("test operation failed");

        // Test prefix scan
        let user_keys = memtable
            .scan_prefix("user:")
            .await
            .expect("test operation failed");
        assert_eq!(user_keys.len(), 3);
        assert!(user_keys.contains(&"user:1".to_string()));

        // Test range scan
        let range_keys = memtable
            .scan_range("user:1", "user:3")
            .await
            .expect("test operation failed");
        assert_eq!(range_keys.len(), 3);
        assert!(range_keys.contains(&"user:1".to_string()));
        assert!(range_keys.contains(&"user:2".to_string()));
        assert!(range_keys.contains(&"user:3".to_string()));
    }

    #[tokio::test]
    async fn test_memtable_size_tracking() {
        let config = MemtableConfig::default();
        let mut memtable = Memtable::new(&config).expect("test operation failed");

        let initial_size = memtable.size().await;
        let initial_count = memtable.entry_count().await;

        // Add a key
        memtable
            .put("key1", &Value::String("value1".to_string()))
            .await
            .expect("test operation failed");
        assert!(memtable.size().await > initial_size);
        assert_eq!(memtable.entry_count().await, initial_count + 1);

        // Update the key
        let size_before_update = memtable.size().await;
        memtable
            .put("key1", &Value::String("longer_value".to_string()))
            .await
            .expect("test operation failed");
        assert!(memtable.size().await > size_before_update);
        assert_eq!(memtable.entry_count().await, initial_count + 1); // Count shouldn't change

        // Delete the key
        let size_before_delete = memtable.size().await;
        memtable
            .delete("key1")
            .await
            .expect("test operation failed");
        assert!(memtable.size().await < size_before_delete);
        assert_eq!(memtable.entry_count().await, initial_count + 1); // Tombstone still counts
    }
}
