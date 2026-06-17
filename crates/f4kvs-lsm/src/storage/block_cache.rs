//! Block cache implementation for LSM Tree Engine
//!
//! This module provides an LRU-based block cache for efficient read operations.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// Cached block data
#[derive(Debug, Clone)]
struct CachedBlock {
    data: Vec<u8>,
    last_accessed: Instant,
    access_count: u64,
}

/// LRU Block Cache for SSTables
#[derive(Debug)]
pub struct BlockCache {
    /// Maximum cache size in bytes
    max_size: usize,
    /// Current cache size in bytes
    current_size: usize,
    /// Cache entries
    blocks: HashMap<String, CachedBlock>,
    /// Access order for LRU eviction
    access_order: Vec<String>,
}

impl BlockCache {
    /// Create a new block cache
    pub fn new(max_size: usize) -> Self {
        Self {
            max_size,
            current_size: 0,
            blocks: HashMap::new(),
            access_order: Vec::new(),
        }
    }

    /// Get a block from cache
    pub fn get(&mut self, key: &str) -> Option<&[u8]> {
        if let Some(block) = self.blocks.get_mut(key) {
            block.last_accessed = Instant::now();
            block.access_count += 1;

            // Update access order - we need to do this after releasing the borrow
            let key_owned = key.to_string();
            let _ = block; // Explicitly note we're done with the borrow
            self.update_access_order(&key_owned);

            // Re-borrow to return the data
            if let Some(block) = self.blocks.get(key) {
                Some(&block.data)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Put a block into cache
    pub fn put(&mut self, key: String, data: Vec<u8>) {
        let data_size = data.len();

        // Evict blocks if necessary
        while self.current_size + data_size > self.max_size && !self.blocks.is_empty() {
            self.evict_lru();
        }

        // Add new block
        let block = CachedBlock {
            data,
            last_accessed: Instant::now(),
            access_count: 1,
        };

        self.blocks.insert(key.clone(), block);
        self.current_size += data_size;
        self.access_order.push(key);
    }

    /// Update access order for LRU
    fn update_access_order(&mut self, key: &str) {
        // Remove from current position
        if let Some(pos) = self.access_order.iter().position(|k| k == key) {
            self.access_order.remove(pos);
        }
        // Add to end (most recently used)
        self.access_order.push(key.to_string());
    }

    /// Evict least recently used block
    fn evict_lru(&mut self) {
        if let Some(key) = self.access_order.first().cloned() {
            if let Some(block) = self.blocks.remove(&key) {
                self.current_size -= block.data.len();
            }
            self.access_order.remove(0);
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            current_size: self.current_size,
            max_size: self.max_size,
            block_count: self.blocks.len(),
            hit_rate: self.calculate_hit_rate(),
        }
    }

    /// Calculate hit rate (simplified)
    fn calculate_hit_rate(&self) -> f64 {
        let total_accesses: u64 = self.blocks.values().map(|b| b.access_count).sum();
        if total_accesses == 0 {
            0.0
        } else {
            // This is a simplified calculation
            // In a real implementation, you'd track hits vs misses
            total_accesses as f64 / (total_accesses + self.blocks.len() as u64) as f64
        }
    }

    /// Clear the cache
    pub fn clear(&mut self) {
        self.blocks.clear();
        self.access_order.clear();
        self.current_size = 0;
    }

    /// Remove a specific block
    pub fn remove(&mut self, key: &str) -> Option<Vec<u8>> {
        if let Some(block) = self.blocks.remove(key) {
            self.current_size -= block.data.len();
            if let Some(pos) = self.access_order.iter().position(|k| k == key) {
                self.access_order.remove(pos);
            }
            Some(block.data)
        } else {
            None
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Current cache size in bytes
    pub current_size: usize,
    /// Maximum cache size in bytes
    pub max_size: usize,
    /// Number of cached blocks
    pub block_count: usize,
    /// Cache hit rate (0.0 to 1.0)
    pub hit_rate: f64,
}

/// Thread-safe block cache wrapper
#[derive(Debug)]
pub struct SharedBlockCache {
    cache: Arc<RwLock<BlockCache>>,
}

impl SharedBlockCache {
    /// Create a new shared block cache
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: Arc::new(RwLock::new(BlockCache::new(max_size))),
        }
    }

    /// Get a block from cache
    pub async fn get(&self, key: &str) -> Option<Vec<u8>> {
        let mut cache = self.cache.write().await;
        cache.get(key).map(|data| data.to_vec())
    }

    /// Put a block into cache
    pub async fn put(&self, key: String, data: Vec<u8>) {
        let mut cache = self.cache.write().await;
        cache.put(key, data);
    }

    /// Get cache statistics
    pub async fn stats(&self) -> CacheStats {
        let cache = self.cache.read().await;
        cache.stats()
    }

    /// Clear the cache
    pub async fn clear(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
    }

    /// Remove a specific block
    pub async fn remove(&self, key: &str) -> Option<Vec<u8>> {
        let mut cache = self.cache.write().await;
        cache.remove(key)
    }
}

impl Clone for SharedBlockCache {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_cache_basic() {
        let mut cache = BlockCache::new(1000);

        // Test put and get
        cache.put("key1".to_string(), vec![1, 2, 3]);
        assert_eq!(cache.get("key1"), Some(&[1, 2, 3][..]));
        assert_eq!(cache.get("key2"), None);
    }

    #[test]
    fn test_block_cache_eviction() {
        let mut cache = BlockCache::new(10); // Very small cache

        // Add blocks that exceed cache size
        cache.put("key1".to_string(), vec![1, 2, 3, 4, 5]); // 5 bytes
        cache.put("key2".to_string(), vec![6, 7, 8, 9, 10]); // 5 bytes
        cache.put("key3".to_string(), vec![11, 12, 13, 14, 15]); // 5 bytes

        // First block should be evicted
        assert_eq!(cache.get("key1"), None);
        assert_eq!(cache.get("key2"), Some(&[6, 7, 8, 9, 10][..]));
        assert_eq!(cache.get("key3"), Some(&[11, 12, 13, 14, 15][..]));
    }

    #[test]
    fn test_shared_block_cache() {
        let cache = SharedBlockCache::new(1000);

        // Test async operations
        tokio::runtime::Runtime::new()
            .expect("test operation failed")
            .block_on(async {
                cache.put("key1".to_string(), vec![1, 2, 3]).await;
                assert_eq!(cache.get("key1").await, Some(vec![1, 2, 3]));

                let stats = cache.stats().await;
                assert_eq!(stats.block_count, 1);
            });
    }
}
