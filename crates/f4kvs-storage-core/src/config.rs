//! Configuration for F4KVS storage backends

use crate::common::validation::ValidationConfig;
use crate::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Storage backend types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageBackend {
    /// Pure in-memory storage (Redis-like performance)
    Memory,
    /// Pure LSM tree storage (Cassandra-like durability)
    LsmTree,
    /// Partitioned storage with WAL
    Partitioned,
    /// File system storage
    FileSystem,
    /// Analytics engine (columnar + time-series)
    Analytics,
}

impl std::fmt::Display for StorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageBackend::Memory => write!(f, "memory"),
            StorageBackend::LsmTree => write!(f, "lsm-tree"),
            StorageBackend::Partitioned => write!(f, "partitioned"),
            StorageBackend::FileSystem => write!(f, "filesystem"),
            StorageBackend::Analytics => write!(f, "analytics"),
        }
    }
}

impl std::str::FromStr for StorageBackend {
    type Err = crate::F4KvsError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "memory" => Ok(StorageBackend::Memory),
            "lsm-tree" | "lsm_tree" | "lsmtree" => Ok(StorageBackend::LsmTree),
            "partitioned" => Ok(StorageBackend::Partitioned),
            "filesystem" | "fs" => Ok(StorageBackend::FileSystem),
            "analytics" => Ok(StorageBackend::Analytics),
            _ => Err(crate::F4KvsError::storage(format!(
                "Unknown storage backend: {s}"
            ))),
        }
    }
}

/// LSM-tree specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LsmTreeConfig {
    /// Maximum size of memtable in bytes
    pub memtable_size: u64,
    /// Maximum number of immutable memtables before compaction
    pub immutable_memtable_limit: u32,
    /// Target size of SSTable files in bytes
    pub sstable_size: u64,
    /// Number of levels in the LSM-tree
    pub levels: u32,
    /// Size multiplier between levels
    pub level_size_multiplier: f64,
    /// Optional merge operator for handling duplicate keys
    pub merge_operator: Option<String>,
    /// Compaction strategy
    pub compaction_strategy: CompactionStrategy,
}

impl Default for LsmTreeConfig {
    fn default() -> Self {
        Self {
            memtable_size: 64 * 1024 * 1024, // 64MB
            immutable_memtable_limit: 2,
            sstable_size: 64 * 1024 * 1024, // 64MB
            levels: 7,
            level_size_multiplier: 10.0,
            merge_operator: None,
            compaction_strategy: CompactionStrategy::LevelTiered,
        }
    }
}

/// Memory storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryConfig {
    /// Maximum memory usage in bytes
    pub max_memory_usage: Option<u64>,

    /// Eviction policy
    pub eviction_policy: EvictionPolicy,

    /// TTL enabled
    pub ttl_enabled: bool,

    /// Enable persistence hooks
    pub enable_persistence: bool,
}

/// Buffer pool configuration for caching persistent storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferPoolConfig {
    /// Maximum cache size in bytes (default: 256MB)
    pub cache_size: u64,
    /// Maximum number of cache entries (default: 10000)
    pub max_entries: usize,
    /// Batch size for async writes (default: 100)
    pub write_batch_size: usize,
    /// Interval for background flush (default: 100ms)
    pub write_interval: Duration,
    /// Use write-back vs write-through (default: true)
    ///
    /// When enabled, `put` acknowledgments may happen before durability unless
    /// callers use explicit barriers (`flush`/`put_durable`) or force durable puts.
    pub enable_write_back: bool,
    /// Force `put`/`batch_put` to wait for durability barriers (default: false)
    ///
    /// This preserves write-back internals but exposes write-through-like
    /// acknowledgment semantics to callers that require crash-safe confirmation.
    pub force_durable_puts: bool,
    /// Enable hot key detection and pinning (default: true)
    pub enable_hot_key_detection: bool,
    /// Access count threshold to be considered hot (default: 10)
    pub hot_key_threshold: u64,
    /// Sliding window size for hot key detection (default: 1000)
    pub hot_key_window_size: usize,
    /// Enable partition-aware caching (default: false)
    pub enable_partition_aware: bool,
    /// Enable prefetching for sequential access (default: false)
    pub enable_prefetch: bool,
    /// Prefetch window size (default: 3)
    pub prefetch_window: usize,
    /// Prefetch threshold for sequential pattern confidence (default: 0.7)
    pub prefetch_threshold: f64,
    /// Enable admission filter to prevent cache pollution (default: false)
    pub enable_admission_filter: bool,
    /// Enable cache warming with recent keys on startup (default: false)
    pub enable_cache_warming: bool,
    /// Number of recent keys to warm cache with (default: 1000)
    pub cache_warming_keys: usize,
    /// Enable dynamic cache size autotuning (default: false)
    pub enable_autotuning: bool,
    /// Autotuning interval in seconds (default: 60)
    pub autotuning_interval_secs: u64,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            cache_size: 256 * 1024 * 1024, // 256MB
            max_entries: 10000,
            write_batch_size: 100,
            write_interval: Duration::from_millis(100),
            enable_write_back: true,
            force_durable_puts: false,
            enable_hot_key_detection: true,
            hot_key_threshold: 10,
            hot_key_window_size: 1000,
            enable_partition_aware: false,
            enable_prefetch: false,
            prefetch_window: 3,
            prefetch_threshold: 0.7,
            enable_admission_filter: false,
            enable_cache_warming: true,
            cache_warming_keys: 1000,
            enable_autotuning: false,
            autotuning_interval_secs: 60,
        }
    }
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            max_memory_usage: Some(1024 * 1024 * 1024), // 1GB
            eviction_policy: EvictionPolicy::Lru,
            ttl_enabled: true,
            enable_persistence: true,
        }
    }
}

/// Tiering policy for hybrid storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringPolicy {
    /// Hot data threshold (accesses per minute)
    pub hot_threshold: u64,

    /// Warm data threshold (accesses per hour)
    pub warm_threshold: u64,

    /// Cold data threshold (accesses per day)
    pub cold_threshold: u64,

    /// Tiering interval
    pub tiering_interval: Duration,

    /// Enable background tiering
    pub background_tiering: bool,
}

impl Default for TieringPolicy {
    fn default() -> Self {
        Self {
            hot_threshold: 100,                         // 100+ accesses per minute = hot
            warm_threshold: 10,                         // 10+ accesses per hour = warm
            cold_threshold: 1,                          // 1+ access per day = cold
            tiering_interval: Duration::from_secs(300), // 5 minutes
            background_tiering: true,
        }
    }
}

/// Main storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Storage backend type
    pub backend: StorageBackend,

    /// Data directory
    pub data_dir: PathBuf,

    /// Write-ahead logging configuration
    pub wal: WALConfig,

    /// Cache configuration
    pub cache: CacheConfig,

    /// Compaction configuration
    pub compaction: CompactionConfig,

    /// Column families to create on startup
    pub column_families: Vec<String>,

    /// Maximum memory usage (bytes)
    pub max_memory_usage: Option<u64>,

    /// Background thread count
    pub background_threads: u32,

    /// Metrics collection interval
    pub metrics_interval: Duration,

    /// LSM tree configuration (when backend is LsmTree)
    pub lsm_tree: Option<LsmTreeConfig>,

    /// Partitioned storage configuration (when backend is Partitioned)
    pub partitioned: Option<PartitionedStorageConfig>,

    /// Buffer pool configuration (for persistent backends)
    pub buffer_pool: Option<BufferPoolConfig>,

    /// Input validation configuration
    pub validation: Option<ValidationConfig>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: StorageBackend::LsmTree,
            data_dir: PathBuf::from("./f4kvs_data"),
            wal: WALConfig::default(),
            cache: CacheConfig::default(),
            compaction: CompactionConfig::default(),
            column_families: vec!["default".to_string()],
            max_memory_usage: None,
            background_threads: 4, // Use 4 threads for clean implementation
            metrics_interval: Duration::from_secs(30),
            lsm_tree: Some(LsmTreeConfig::default()),
            partitioned: Some(PartitionedStorageConfig::default()),
            buffer_pool: Some(BufferPoolConfig::default()),
            validation: Some(ValidationConfig::default()),
        }
    }
}

/// Partitioned storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionedStorageConfig {
    /// Maximum size of each partition in bytes
    pub max_partition_size: u64,
    /// Maximum number of partitions allowed
    pub max_partitions: u32,
    /// Whether to use hash-based partitioning
    pub partition_key_hash: bool,
    /// Split threshold as percentage (0.0-1.0)
    pub auto_split_threshold: f64,
    /// Number of replicas for each partition
    pub replication_factor: u32,
}

impl Default for PartitionedStorageConfig {
    fn default() -> Self {
        Self {
            max_partition_size: 100 * 1024 * 1024, // 100MB
            max_partitions: 256,
            partition_key_hash: true,
            auto_split_threshold: 0.8, // 80%
            replication_factor: 1,
        }
    }
}

/// Write-Ahead Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WALConfig {
    /// Whether WAL is enabled
    pub enabled: bool,
    /// Directory for WAL files (None for default)
    pub dir: Option<PathBuf>,
    /// Size of each WAL segment in bytes
    pub segment_size: u64,
    /// Buffer size for WAL writes
    pub buffer_size: usize,
    /// Interval for flushing WAL buffer
    pub flush_interval: Duration,
    /// Interval for syncing WAL to disk
    pub sync_interval: Duration,
    /// Maximum number of WAL segments to keep
    pub max_segments: u32,
    /// Whether to enable checksums for WAL entries
    pub enable_checksums: bool,
}

impl Default for WALConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            dir: None,                       // Will use data_dir/wal
            segment_size: 100 * 1024 * 1024, // 100MB
            buffer_size: 1000,
            flush_interval: Duration::from_secs(1),
            sync_interval: Duration::from_secs(5),
            max_segments: 100,
            enable_checksums: true,
        }
    }
}

/// Cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Size of block cache in bytes
    pub block_cache_size: u64,
    /// Size of row cache in bytes
    pub row_cache_size: u64,
    /// Size of index cache in bytes
    pub index_cache_size: u64,
    /// Size of filter cache in bytes
    pub filter_cache_size: u64,
    /// Whether to enable bloom filters
    pub enable_bloom_filters: bool,
    /// Bits per key for bloom filters
    pub bloom_filter_bits_per_key: u32,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            block_cache_size: 256 * 1024 * 1024, // 256MB
            row_cache_size: 128 * 1024 * 1024,   // 128MB
            index_cache_size: 64 * 1024 * 1024,  // 64MB
            filter_cache_size: 32 * 1024 * 1024, // 32MB
            enable_bloom_filters: true,
            bloom_filter_bits_per_key: 10,
        }
    }
}

/// Compaction configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionConfig {
    /// Maximum number of background compaction threads
    pub max_background_compactions: u32,
    /// Maximum number of background flush threads
    pub max_background_flushes: u32,
    /// Number of L0 files to trigger compaction
    pub level0_file_num_compaction_trigger: u32,
    /// Number of L0 files to slow down writes
    pub level0_slowdown_writes_trigger: u32,
    /// Number of L0 files to stop writes
    pub level0_stop_writes_trigger: u32,
    /// Maximum bytes for level base
    pub max_bytes_for_level_base: u64,
    /// Multiplier for level size calculation
    pub max_bytes_for_level_multiplier: f64,
    /// Target file size base
    pub target_file_size_base: u64,
    /// Target file size multiplier
    pub target_file_size_multiplier: i32,
    /// Compaction style strategy
    pub compaction_style: CompactionStyle,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            max_background_compactions: 1,
            max_background_flushes: 1,
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_bytes_for_level_base: 256 * 1024 * 1024, // 256MB
            max_bytes_for_level_multiplier: 10.0,
            target_file_size_base: 64 * 1024 * 1024, // 64MB
            target_file_size_multiplier: 1,
            compaction_style: CompactionStyle::Level,
        }
    }
}

/// Compaction strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactionStyle {
    /// Level-based compaction (LSM-tree style)
    Level,
    /// Universal compaction (RocksDB style)
    Universal,
    /// FIFO compaction (time-based)
    FIFO,
}

impl std::fmt::Display for CompactionStyle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompactionStyle::Level => write!(f, "level"),
            CompactionStyle::Universal => write!(f, "universal"),
            CompactionStyle::FIFO => write!(f, "fifo"),
        }
    }
}

/// Eviction policies for memory storage
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvictionPolicy {
    /// Least Recently Used
    Lru,
    /// Least Frequently Used
    Lfu,
    /// Time To Live based
    Ttl,
    /// Random eviction
    Random,
    /// No eviction (OOM on full)
    NoEviction,
}

impl std::fmt::Display for EvictionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvictionPolicy::Lru => write!(f, "lru"),
            EvictionPolicy::Lfu => write!(f, "lfu"),
            EvictionPolicy::Ttl => write!(f, "ttl"),
            EvictionPolicy::Random => write!(f, "random"),
            EvictionPolicy::NoEviction => write!(f, "no-eviction"),
        }
    }
}

/// Compaction strategies for LSM tree
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactionStrategy {
    /// Size-tiered compaction (Cassandra style)
    SizeTiered,
    /// Level-tiered compaction (RocksDB style)
    LevelTiered,
    /// Time-windowed compaction
    TimeWindowed,
}

impl std::fmt::Display for CompactionStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompactionStrategy::SizeTiered => write!(f, "size-tiered"),
            CompactionStrategy::LevelTiered => write!(f, "level-tiered"),
            CompactionStrategy::TimeWindowed => write!(f, "time-windowed"),
        }
    }
}

/// Storage validation
impl StorageConfig {
    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        if self.data_dir.as_os_str().is_empty() {
            return Err(crate::F4KvsError::storage("data_dir cannot be empty"));
        }

        if self.background_threads == 0 {
            return Err(crate::F4KvsError::storage(
                "background_threads must be at least 1",
            ));
        }

        if let Some(max_memory) = self.max_memory_usage {
            let total_cache_size = self.cache.block_cache_size
                + self.cache.row_cache_size
                + self.cache.index_cache_size
                + self.cache.filter_cache_size;

            if total_cache_size > max_memory {
                return Err(crate::F4KvsError::storage(
                    "Total cache size exceeds max_memory_usage",
                ));
            }
        }

        Ok(())
    }

    /// Create configuration for testing
    ///
    /// # Panics
    ///
    /// Panics if a temporary directory cannot be created. This is acceptable
    /// for test helper functions as test environments should always be able to
    /// create temporary directories.
    pub fn for_testing() -> Self {
        Self {
            data_dir: tempfile::tempdir()
                .expect("Failed to create temp directory for testing")
                .keep(),
            cache: CacheConfig {
                block_cache_size: 1024 * 1024,  // 1MB
                row_cache_size: 1024 * 1024,    // 1MB
                index_cache_size: 1024 * 1024,  // 1MB
                filter_cache_size: 1024 * 1024, // 1MB
                ..Default::default()
            },
            wal: WALConfig {
                segment_size: 1024 * 1024, // 1MB
                ..Default::default()
            },
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validation() {
        let config = StorageConfig::default();
        assert!(config.validate().is_ok());

        let mut invalid_config = config.clone();
        invalid_config.background_threads = 0;
        assert!(invalid_config.validate().is_err());
    }

    #[test]
    fn test_testing_config() {
        let config = StorageConfig::for_testing();
        assert!(config.validate().is_ok());
        assert!(config.data_dir.exists() || config.data_dir.parent().is_none_or(|p| p.exists()));
    }

    #[test]
    fn test_storage_backend_display() {
        assert_eq!(StorageBackend::Memory.to_string(), "memory");
        assert_eq!(StorageBackend::LsmTree.to_string(), "lsm-tree");
        assert_eq!(StorageBackend::Partitioned.to_string(), "partitioned");
    }

    #[test]
    fn test_storage_backend_from_str() {
        assert_eq!(
            "memory".parse::<StorageBackend>().expect("parse failed"),
            StorageBackend::Memory
        );
        assert_eq!(
            "lsm-tree".parse::<StorageBackend>().expect("parse failed"),
            StorageBackend::LsmTree
        );
        assert!("invalid".parse::<StorageBackend>().is_err());
    }
}
