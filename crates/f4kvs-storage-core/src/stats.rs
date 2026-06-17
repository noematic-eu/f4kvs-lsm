//! Storage statistics and metrics

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

/// Comprehensive storage statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStats {
    /// Basic key-value counts
    pub total_keys: u64,
    /// Total size of all data in bytes
    pub total_size_bytes: u64,

    /// Performance metrics
    pub cache_stats: CacheStats,
    /// I/O operation statistics
    pub io_stats: IoStats,
    /// Compaction operation statistics
    pub compaction_stats: CompactionStats,

    /// Column family statistics
    pub cf_stats: HashMap<String, ColumnFamilyStats>,

    /// Memory usage
    pub memory_stats: MemoryStats,

    /// WAL statistics
    pub wal_stats: Option<WALStats>,

    /// Health indicators
    pub health: HealthStats,

    /// Collection timestamp
    pub timestamp: SystemTime,
}

impl Default for StorageStats {
    fn default() -> Self {
        Self {
            total_keys: 0,
            total_size_bytes: 0,
            cache_stats: CacheStats::default(),
            io_stats: IoStats::default(),
            compaction_stats: CompactionStats::default(),
            cf_stats: HashMap::new(),
            memory_stats: MemoryStats::default(),
            wal_stats: None,
            health: HealthStats::default(),
            timestamp: SystemTime::now(),
        }
    }
}

/// Cache performance statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CacheStats {
    /// Block cache metrics
    pub block_cache: CacheMetrics,
    /// Bloom filter cache metrics
    pub bloom_cache: CacheMetrics,
    /// Write buffer cache metrics
    pub write_buffer: CacheMetrics,
}

/// Individual cache metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheMetrics {
    /// Cache capacity in bytes
    pub capacity: u64,
    /// Current cache usage in bytes
    pub usage: u64,
    /// Number of cache hits
    pub hit_count: u64,
    /// Number of cache misses
    pub miss_count: u64,
    /// Cache hit rate (0.0-1.0)
    pub hit_rate: f64,
    /// Number of evictions
    pub eviction_count: u64,
}

impl Default for CacheMetrics {
    fn default() -> Self {
        Self {
            capacity: 0,
            usage: 0,
            hit_count: 0,
            miss_count: 0,
            hit_rate: 0.0,
            eviction_count: 0,
        }
    }
}

impl CacheMetrics {
    /// Calculate hit rate from counts
    pub fn calculate_hit_rate(&mut self) {
        let total = self.hit_count + self.miss_count;
        self.hit_rate = if total > 0 {
            self.hit_count as f64 / total as f64
        } else {
            0.0
        };
    }

    /// Get utilization percentage
    pub fn utilization(&self) -> f64 {
        if self.capacity > 0 {
            (self.usage as f64 / self.capacity as f64) * 100.0
        } else {
            0.0
        }
    }
}

/// Bloom filter statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilterStats {
    /// Whether bloom filters are enabled
    pub enabled: bool,
    /// Bits per key for bloom filters
    pub bits_per_key: u32,
    /// False positive rate (0.0-1.0)
    pub false_positive_rate: f64,
    /// Number of bloom filters
    pub filter_count: u64,
    /// Total number of bits used
    pub total_bits: u64,
}

impl Default for BloomFilterStats {
    fn default() -> Self {
        Self {
            enabled: false,
            bits_per_key: 0,
            false_positive_rate: 0.0,
            filter_count: 0,
            total_bits: 0,
        }
    }
}

/// I/O operation statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IoStats {
    /// Read operation metrics
    pub read_stats: IoMetrics,
    /// Write operation metrics
    pub write_stats: IoMetrics,
    /// Compaction operation metrics
    pub compaction_stats: IoMetrics,
}

/// I/O metrics for reads, writes, or syncs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoMetrics {
    /// Number of operations performed
    pub operation_count: u64,
    /// Total bytes processed
    pub total_bytes: u64,
    /// Total time spent in milliseconds
    pub total_time_ms: u64,
    /// Average latency per operation in milliseconds
    pub avg_latency_ms: f64,
    /// Maximum latency in milliseconds
    pub max_latency_ms: u64,
    /// Minimum latency in milliseconds
    pub min_latency_ms: u64,
    /// Operations per second
    pub ops_per_second: f64,
    /// Bytes per second
    pub bytes_per_second: f64,
}

impl Default for IoMetrics {
    fn default() -> Self {
        Self {
            operation_count: 0,
            total_bytes: 0,
            total_time_ms: 0,
            avg_latency_ms: 0.0,
            max_latency_ms: 0,
            min_latency_ms: u64::MAX,
            ops_per_second: 0.0,
            bytes_per_second: 0.0,
        }
    }
}

impl IoMetrics {
    /// Update metrics with a new operation
    pub fn record_operation(&mut self, bytes: u64, latency_ms: u64) {
        self.operation_count += 1;
        self.total_bytes += bytes;
        self.total_time_ms += latency_ms;

        self.max_latency_ms = self.max_latency_ms.max(latency_ms);
        if self.min_latency_ms == u64::MAX {
            self.min_latency_ms = latency_ms;
        } else {
            self.min_latency_ms = self.min_latency_ms.min(latency_ms);
        }

        self.avg_latency_ms = if self.operation_count > 0 {
            self.total_time_ms as f64 / self.operation_count as f64
        } else {
            0.0
        };
    }

    /// Calculate throughput metrics over a time period
    pub fn calculate_throughput(&mut self, duration: Duration) {
        let seconds = duration.as_secs_f64();
        if seconds > 0.0 {
            self.ops_per_second = self.operation_count as f64 / seconds;
            self.bytes_per_second = self.total_bytes as f64 / seconds;
        }
    }
}

/// Compaction statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionStats {
    /// Statistics for each level
    pub levels: Vec<LevelStats>,
    /// Number of pending compactions
    pub pending_compactions: u32,
    /// Number of running compactions
    pub running_compactions: u32,
    /// Total number of compactions performed
    pub total_compactions: u64,
    /// Total bytes compacted
    pub total_bytes_compacted: u64,
    /// Total compaction time in milliseconds
    pub total_compaction_time_ms: u64,
    /// Average compaction time in milliseconds
    pub avg_compaction_time_ms: f64,
    /// Time of last compaction
    pub last_compaction: Option<SystemTime>,
}

impl Default for CompactionStats {
    fn default() -> Self {
        Self {
            levels: Vec::new(),
            pending_compactions: 0,
            running_compactions: 0,
            total_compactions: 0,
            total_bytes_compacted: 0,
            total_compaction_time_ms: 0,
            avg_compaction_time_ms: 0.0,
            last_compaction: None,
        }
    }
}

/// Statistics for a single LSM level
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LevelStats {
    /// Level number (0-based)
    pub level: u32,
    /// Number of files in this level
    pub files: u32,
    /// Total size of files in bytes
    pub size_bytes: u64,
    /// Compaction score for this level
    pub score: f64,
    /// Average read latency in milliseconds
    pub read_latency_ms: f64,
    /// Write amplification factor
    pub write_amplification: f64,
}

/// Column family specific statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnFamilyStats {
    /// Name of the column family
    pub name: String,
    /// Number of keys in this column family
    pub key_count: u64,
    /// Total size in bytes
    pub size_bytes: u64,
    /// Number of read operations
    pub read_ops: u64,
    /// Number of write operations
    pub write_ops: u64,
    /// Number of delete operations
    pub delete_ops: u64,
    /// Average key size in bytes
    pub avg_key_size: f64,
    /// Average value size in bytes
    pub avg_value_size: f64,
    /// Last update timestamp
    pub last_updated: SystemTime,
}

impl Default for ColumnFamilyStats {
    fn default() -> Self {
        Self {
            name: String::new(),
            key_count: 0,
            size_bytes: 0,
            read_ops: 0,
            write_ops: 0,
            delete_ops: 0,
            avg_key_size: 0.0,
            avg_value_size: 0.0,
            last_updated: SystemTime::now(),
        }
    }
}

/// Memory usage statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryStats {
    /// Total memory usage in bytes
    pub total_memory_usage: u64,
    /// Memtable memory usage in bytes
    pub memtable_usage: u64,
    /// Immutable memtable memory usage in bytes
    pub immutable_memtable_usage: u64,
    /// Block cache memory usage in bytes
    pub block_cache_usage: u64,
    /// Index cache memory usage in bytes
    pub index_cache_usage: u64,
    /// Filter cache memory usage in bytes
    pub filter_cache_usage: u64,
    /// Other memory usage in bytes
    pub other_usage: u64,
    /// Memory limit in bytes (if set)
    pub memory_limit: Option<u64>,
    /// Memory utilization percentage (0.0-100.0)
    pub utilization_percent: f64,
}

impl Default for MemoryStats {
    fn default() -> Self {
        Self {
            total_memory_usage: 0,
            memtable_usage: 0,
            immutable_memtable_usage: 0,
            block_cache_usage: 0,
            index_cache_usage: 0,
            filter_cache_usage: 0,
            other_usage: 0,
            memory_limit: None,
            utilization_percent: 0.0,
        }
    }
}

impl MemoryStats {
    /// Calculate total memory usage from components
    pub fn calculate_total(&mut self) {
        self.total_memory_usage = self.memtable_usage
            + self.immutable_memtable_usage
            + self.block_cache_usage
            + self.index_cache_usage
            + self.filter_cache_usage
            + self.other_usage;

        if let Some(limit) = self.memory_limit {
            self.utilization_percent = if limit > 0 {
                (self.total_memory_usage as f64 / limit as f64) * 100.0
            } else {
                0.0
            };
        }
    }
}

/// Write-Ahead Log statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WALStats {
    /// Whether WAL is enabled
    pub enabled: bool,
    /// Current segment number
    pub current_segment: u64,
    /// Total number of segments
    pub total_segments: u32,
    /// Total WAL size in bytes
    pub total_size_bytes: u64,
    /// Number of entries written
    pub entries_written: u64,
    /// Number of entries flushed
    pub entries_flushed: u64,
    /// Number of flush operations
    pub flush_count: u64,
    /// Number of sync operations
    pub sync_count: u64,
    /// Time of last flush
    pub last_flush: Option<SystemTime>,
    /// Time of last sync
    pub last_sync: Option<SystemTime>,
    /// Average flush time in milliseconds
    pub avg_flush_time_ms: f64,
    /// Average sync time in milliseconds
    pub avg_sync_time_ms: f64,
}

impl Default for WALStats {
    fn default() -> Self {
        Self {
            enabled: false,
            current_segment: 0,
            total_segments: 0,
            total_size_bytes: 0,
            entries_written: 0,
            entries_flushed: 0,
            flush_count: 0,
            sync_count: 0,
            last_flush: None,
            last_sync: None,
            avg_flush_time_ms: 0.0,
            avg_sync_time_ms: 0.0,
        }
    }
}

/// Health and status indicators
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStats {
    /// Overall health status
    pub overall_health: HealthStatus,
    /// Number of errors encountered
    pub error_count: u64,
    /// Number of warnings encountered
    pub warning_count: u64,
    /// Last error message (if any)
    pub last_error: Option<String>,
    /// Uptime in seconds
    pub uptime_seconds: u64,
    /// Number of background errors
    pub background_errors: u64,
    /// Number of corruption events
    pub corruption_count: u64,
}

impl Default for HealthStats {
    fn default() -> Self {
        Self {
            overall_health: HealthStatus::Healthy,
            error_count: 0,
            warning_count: 0,
            last_error: None,
            uptime_seconds: 0,
            background_errors: 0,
            corruption_count: 0,
        }
    }
}

/// Health status levels
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// System is operating normally
    Healthy,
    /// System is experiencing some issues but still functional
    Degraded,
    /// System is experiencing critical issues
    Unhealthy,
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthStatus::Healthy => write!(f, "healthy"),
            HealthStatus::Degraded => write!(f, "degraded"),
            HealthStatus::Unhealthy => write!(f, "unhealthy"),
        }
    }
}

/// Utility functions for statistics
impl StorageStats {
    /// Create a new stats instance with current timestamp
    pub fn new() -> Self {
        Self {
            timestamp: SystemTime::now(),
            ..Default::default()
        }
    }

    /// Update overall health based on various metrics
    pub fn update_health(&mut self) {
        let mut issues = Vec::new();

        // Check cache hit rates
        if self.cache_stats.block_cache.hit_rate < 0.5 {
            issues.push("Low block cache hit rate");
        }

        // Check memory usage
        if self.memory_stats.utilization_percent > 90.0 {
            issues.push("High memory usage");
        }

        // Check for background errors
        if self.health.background_errors > 0 {
            issues.push("Background errors detected");
        }

        // Check for corruption
        if self.health.corruption_count > 0 {
            issues.push("Data corruption detected");
        }

        // Update health status
        self.health.overall_health =
            if self.health.corruption_count > 0 || self.health.background_errors > 10 {
                HealthStatus::Unhealthy
            } else if !issues.is_empty() {
                HealthStatus::Degraded
            } else {
                HealthStatus::Healthy
            };
    }

    /// Get a summary string of key metrics
    pub fn summary(&self) -> String {
        format!(
            "Keys: {}, Size: {:.2} MB, Cache Hit Rate: {:.1}%, Memory: {:.1}%, Health: {}",
            self.total_keys,
            self.total_size_bytes as f64 / (1024.0 * 1024.0),
            self.cache_stats.block_cache.hit_rate * 100.0,
            self.memory_stats.utilization_percent,
            self.health.overall_health
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_metrics() {
        let mut metrics = CacheMetrics {
            hit_count: 80,
            miss_count: 20,
            capacity: 1000,
            usage: 750,
            ..Default::default()
        };

        metrics.calculate_hit_rate();
        assert_eq!(metrics.hit_rate, 0.8);
        assert_eq!(metrics.utilization(), 75.0);
    }

    #[test]
    fn test_io_metrics() {
        let mut metrics = IoMetrics::default();

        metrics.record_operation(100, 10);
        assert_eq!(metrics.operation_count, 1);
        assert_eq!(metrics.total_bytes, 100);
        assert_eq!(metrics.avg_latency_ms, 10.0);
        assert_eq!(metrics.min_latency_ms, 10);
        assert_eq!(metrics.max_latency_ms, 10);

        metrics.record_operation(200, 20);
        assert_eq!(metrics.operation_count, 2);
        assert_eq!(metrics.total_bytes, 300);
        assert_eq!(metrics.avg_latency_ms, 15.0);
        assert_eq!(metrics.min_latency_ms, 10);
        assert_eq!(metrics.max_latency_ms, 20);
    }

    #[test]
    fn test_memory_stats() {
        let mut stats = MemoryStats {
            memtable_usage: 100,
            block_cache_usage: 200,
            index_cache_usage: 50,
            memory_limit: Some(1000),
            ..Default::default()
        };

        stats.calculate_total();
        assert_eq!(stats.total_memory_usage, 350);
        assert_eq!(stats.utilization_percent, 35.0);
    }

    #[test]
    fn test_storage_stats_summary() {
        let stats = StorageStats {
            total_keys: 1000,
            total_size_bytes: 5 * 1024 * 1024, // 5MB
            cache_stats: CacheStats {
                block_cache: CacheMetrics {
                    hit_rate: 0.85,
                    ..Default::default()
                },
                ..Default::default()
            },
            memory_stats: MemoryStats {
                utilization_percent: 67.5,
                ..Default::default()
            },
            health: HealthStats {
                overall_health: HealthStatus::Healthy,
                ..Default::default()
            },
            ..Default::default()
        };

        let summary = stats.summary();
        assert!(summary.contains("Keys: 1000"));
        assert!(summary.contains("Size: 5.00 MB"));
        assert!(summary.contains("Cache Hit Rate: 85.0%"));
        assert!(summary.contains("Memory: 67.5%"));
        assert!(summary.contains("Health: healthy"));
    }
}
