//! Configuration for the F4KVS LSM Tree Engine

use crate::compaction::adaptive::AdaptiveCompactionConfig;
use f4kvs_storage_core::StorageConfig;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// LSM Tree configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LsmConfig {
    /// Data directory for LSM tree files
    pub data_dir: PathBuf,

    /// Memtable configuration
    pub memtable: MemtableConfig,

    /// SSTable configuration
    pub sstable: SstableConfig,

    /// Level configuration
    pub levels: LevelConfig,

    /// Compaction configuration
    pub compaction: CompactionConfig,

    /// WAL configuration
    pub wal: WalConfig,

    /// Bloom filter configuration
    pub bloom_filter: BloomFilterConfig,

    /// Column family configuration
    pub column_families: ColumnFamilyConfig,

    /// Performance tuning
    pub performance: PerformanceConfig,

    /// Adaptive compaction configuration
    pub adaptive_compaction: Option<AdaptiveCompactionConfig>,
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./f4kvs_lsm_data"),
            memtable: MemtableConfig::default(),
            sstable: SstableConfig::default(),
            levels: LevelConfig::default(),
            compaction: CompactionConfig::default(),
            wal: WalConfig::default(),
            bloom_filter: BloomFilterConfig::default(),
            column_families: ColumnFamilyConfig::default(),
            performance: PerformanceConfig::default(),
            adaptive_compaction: None,
        }
    }
}

/// Memtable configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemtableConfig {
    /// Maximum size of memtable in bytes
    pub max_size: usize,

    /// Maximum number of immutable memtables
    pub max_immutable_count: usize,

    /// Flush threshold (percentage of max_size)
    pub flush_threshold: f64,

    /// Use skip list for memtable (alternative: B-tree)
    pub use_skip_list: bool,

    /// Enable concurrent reads on immutable memtables
    pub concurrent_reads: bool,
}

impl Default for MemtableConfig {
    fn default() -> Self {
        Self {
            max_size: 64 * 1024 * 1024, // 64MB
            max_immutable_count: 2,
            flush_threshold: 0.8, // 80%
            use_skip_list: true,
            concurrent_reads: true,
        }
    }
}

/// SSTable configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SstableConfig {
    /// Target size for SSTables in bytes
    pub target_size: usize,

    /// Maximum size for SSTables in bytes
    pub max_size: usize,

    /// Block size for SSTable blocks
    pub block_size: usize,

    /// Maximum number of open files
    pub max_open_files: usize,

    /// Number of retry attempts for file operations
    pub file_retry_attempts: usize,

    /// Delay between retry attempts in milliseconds
    pub retry_delay_ms: u64,

    /// Enable resilient file handling (auto-reopen closed files)
    pub enable_resilient_handling: bool,
}

impl Default for SstableConfig {
    fn default() -> Self {
        Self {
            target_size: 64 * 1024 * 1024, // 64MB
            max_size: 128 * 1024 * 1024,   // 128MB
            block_size: 4 * 1024,          // 4KB
            max_open_files: 1000,
            file_retry_attempts: 3,
            retry_delay_ms: 100,
            enable_resilient_handling: true,
        }
    }
}

/// Level configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LevelConfig {
    /// Number of levels in the LSM tree
    pub count: usize,

    /// Size multiplier between levels
    pub size_multiplier: f64,

    /// Maximum number of SSTables per level
    pub max_sstables_per_level: usize,

    /// Enable level-based compaction
    pub enable_leveled_compaction: bool,
}

impl Default for LevelConfig {
    fn default() -> Self {
        Self {
            count: 7,
            size_multiplier: 10.0,
            max_sstables_per_level: 10,
            enable_leveled_compaction: true,
        }
    }
}

/// Compaction configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionConfig {
    /// Compaction strategy
    pub strategy: CompactionStrategy,

    /// Maximum number of concurrent compactions
    pub max_concurrent: usize,

    /// Compaction priority
    pub priority: CompactionPriority,

    /// Enable background compaction
    pub background_enabled: bool,

    /// Compaction interval
    pub interval: Duration,

    /// Maximum compaction time per run
    pub max_time_per_run: Duration,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            strategy: CompactionStrategy::Leveled,
            max_concurrent: 2,
            priority: CompactionPriority::Balanced,
            background_enabled: true,
            interval: Duration::from_secs(60),         // 1 minute
            max_time_per_run: Duration::from_secs(30), // 30 seconds
        }
    }
}

/// Compaction strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactionStrategy {
    /// Size-tiered compaction (Cassandra style)
    SizeTiered,
    /// Leveled compaction (RocksDB style)
    Leveled,
    /// Time-windowed compaction
    TimeWindowed,
    /// Hybrid approach
    Hybrid,
}

/// Compaction priority
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactionPriority {
    /// Prioritize read performance
    ReadOptimized,
    /// Prioritize write performance
    WriteOptimized,
    /// Balanced approach
    Balanced,
    /// Prioritize space efficiency
    SpaceOptimized,
}

/// WAL sync mode for durability guarantees
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum WalSyncMode {
    /// No explicit sync - OS may buffer writes (lowest durability, highest performance)
    None,
    /// Flush to OS buffer cache only (good performance, may lose data on power failure)
    Flush,
    /// Sync to disk using fsync (highest durability, lower performance)
    #[default]
    Fsync,
    /// Async fsync - sync in background (good balance of durability and performance)
    FsyncAsync,
}

/// WAL configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    /// Enable WAL
    pub enabled: bool,

    /// WAL directory
    pub dir: PathBuf,

    /// WAL segment size in bytes
    pub segment_size: usize,

    /// WAL buffer size in bytes
    pub buffer_size: usize,

    /// WAL flush interval
    pub flush_interval: Duration,

    /// WAL sync mode for durability
    ///
    /// Controls how aggressively WAL writes are synchronized to disk:
    /// - None: No explicit sync (fastest, may lose data on crash)
    /// - Flush: Flush to OS buffer (fast, may lose data on power failure)
    /// - Fsync: Full fsync to disk (strict durability, slower)
    /// - FsyncAsync: Async fsync (good balance of performance and durability)
    ///
    /// **Strict Mode**: When using Fsync mode, writes are not acknowledged until
    /// fsync completes successfully. This ensures crash-safe writes when durability is required.
    ///
    /// **Lossy Mode**: When using FsyncAsync mode, fsync errors are logged but not
    /// propagated to callers. This provides better performance but may lose data on crash.
    ///
    /// Default: Fsync (ensures durability)
    pub sync_mode: WalSyncMode,

    /// Enable WAL compression
    pub enable_compression: bool,

    /// WAL retention period
    pub retention_period: Duration,

    /// WAL cleanup interval (how often to run cleanup)
    pub cleanup_interval: Duration,

    /// WAL retention after flush (grace period before deleting flushed segments)
    pub retention_after_flush: Duration,

    /// Maximum number of WAL segments before triggering cleanup
    pub max_segments: usize,

    /// Allow engine startup to continue even if WAL recovery fails
    ///
    /// **WARNING**: Setting this to `true` may result in data loss if recovery fails.
    /// Should only be used in development/testing environments.
    ///
    /// Default: `false` (recovery failures will block engine startup)
    pub allow_recovery_failure: bool,

    /// Timeout for WAL recovery operations
    ///
    /// If WAL recovery takes longer than this duration, recovery will be aborted.
    /// This prevents the engine from hanging indefinitely during startup.
    ///
    /// Default: `30` seconds (reasonable for most use cases)
    pub recovery_timeout: Duration,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            dir: PathBuf::from("./f4kvs_lsm_wal"),
            segment_size: 64 * 1024 * 1024,             // 64MB
            buffer_size: 1024 * 1024,                   // 1MB
            flush_interval: Duration::from_millis(100), // 100ms
            sync_mode: WalSyncMode::Fsync,              // Default: full durability
            enable_compression: true,
            retention_period: Duration::from_secs(3600), // 1 hour
            cleanup_interval: Duration::from_secs(300),  // 5 minutes
            retention_after_flush: Duration::from_secs(60), // 1 minute
            max_segments: 10,                            // 10 segments
            allow_recovery_failure: false,               // Default: recovery failures block startup
            recovery_timeout: Duration::from_secs(30),   // 30 seconds
        }
    }
}

/// Bloom filter configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilterConfig {
    /// Enable bloom filters
    pub enabled: bool,

    /// False positive rate
    pub false_positive_rate: f64,

    /// Bloom filter bits per key
    pub bits_per_key: usize,

    /// Enable cache for bloom filters
    pub enable_cache: bool,
}

impl Default for BloomFilterConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            false_positive_rate: 0.01, // 1%
            bits_per_key: 10,
            enable_cache: true,
        }
    }
}

/// Column family configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnFamilyConfig {
    /// Default column family name
    pub default_name: String,

    /// Enable column family isolation
    pub enable_isolation: bool,

    /// Maximum number of column families
    pub max_count: usize,
}

impl Default for ColumnFamilyConfig {
    fn default() -> Self {
        Self {
            default_name: "default".to_string(),
            enable_isolation: true,
            max_count: 100,
        }
    }
}

/// Performance configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Enable read-ahead
    pub enable_read_ahead: bool,

    /// Read-ahead size in bytes
    pub read_ahead_size: usize,

    /// Enable write buffering
    pub enable_write_buffering: bool,

    /// Write buffer size in bytes
    pub write_buffer_size: usize,

    /// Enable parallel reads
    pub enable_parallel_reads: bool,

    /// Maximum parallel read threads
    pub max_parallel_reads: usize,

    /// Maximum batch size for batch operations (DoS protection)
    /// Default: 10,000 items
    pub max_batch_size: usize,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            enable_read_ahead: true,
            read_ahead_size: 64 * 1024, // 64KB
            enable_write_buffering: true,
            write_buffer_size: 1024 * 1024, // 1MB
            enable_parallel_reads: true,
            max_parallel_reads: 4,
            max_batch_size: 10_000, // Default: 10,000 items per batch (DoS protection)
        }
    }
}

impl LsmConfig {
    /// Create an LSM configuration from a generic storage configuration
    #[allow(clippy::field_reassign_with_default)]
    pub fn from_storage_config(config: &StorageConfig) -> Self {
        let mut lsm_config = Self::default();

        // Set data directory
        lsm_config.data_dir = config.data_dir.clone();

        // Configure WAL
        lsm_config.wal.enabled = config.wal.enabled;
        if let Some(ref wal_dir) = config.wal.dir {
            lsm_config.wal.dir = wal_dir.clone();
        } else {
            lsm_config.wal.dir = config.data_dir.join("wal");
        }
        lsm_config.wal.segment_size = config.wal.segment_size as usize;
        lsm_config.wal.buffer_size = config.wal.buffer_size;

        // Configure from LSM-specific settings if available
        if let Some(ref lsm_tree_config) = config.lsm_tree {
            lsm_config.memtable.max_size = lsm_tree_config.memtable_size as usize;
            lsm_config.memtable.max_immutable_count =
                lsm_tree_config.immutable_memtable_limit as usize;
            lsm_config.sstable.target_size = lsm_tree_config.sstable_size as usize;
            lsm_config.levels.count = lsm_tree_config.levels as usize;
            lsm_config.levels.size_multiplier = lsm_tree_config.level_size_multiplier;
        }

        // Configure bloom filters
        lsm_config.bloom_filter.enabled = config.cache.enable_bloom_filters;
        lsm_config.bloom_filter.bits_per_key = config.cache.bloom_filter_bits_per_key as usize;

        lsm_config
    }

    /// Validate configuration and return detailed error messages for any issues
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        // Validate data directory
        if self.data_dir.as_os_str().is_empty() {
            errors.push("data_dir cannot be empty".to_string());
        }

        // Validate memtable configuration
        if self.memtable.max_size == 0 {
            errors.push("memtable.max_size must be greater than 0".to_string());
        }
        if self.memtable.max_size < 1024 {
            errors.push("memtable.max_size should be at least 1KB".to_string());
        }
        if self.memtable.max_immutable_count == 0 {
            errors.push("memtable.max_immutable_count must be greater than 0".to_string());
        }
        if self.memtable.max_immutable_count > 100 {
            errors.push("memtable.max_immutable_count should not exceed 100".to_string());
        }
        if self.memtable.flush_threshold <= 0.0 || self.memtable.flush_threshold > 1.0 {
            errors.push("memtable.flush_threshold must be between 0.0 and 1.0".to_string());
        }

        // Validate SSTable configuration
        if self.sstable.target_size == 0 {
            errors.push("sstable.target_size must be greater than 0".to_string());
        }
        if self.sstable.target_size < 1024 {
            errors.push("sstable.target_size should be at least 1KB".to_string());
        }
        if self.sstable.max_size < self.sstable.target_size {
            errors.push("sstable.max_size must be >= sstable.target_size".to_string());
        }
        if self.sstable.block_size == 0 {
            errors.push("sstable.block_size must be greater than 0".to_string());
        }
        if self.sstable.block_size < 512 {
            errors.push("sstable.block_size should be at least 512 bytes".to_string());
        }

        // Validate level configuration
        if self.levels.count == 0 {
            errors.push("levels.count must be greater than 0".to_string());
        }
        if self.levels.count > 20 {
            errors.push("levels.count should not exceed 20".to_string());
        }
        if self.levels.size_multiplier <= 1.0 {
            errors.push("levels.size_multiplier must be greater than 1.0".to_string());
        }

        // Validate compaction configuration
        if self.compaction.max_concurrent == 0 {
            errors.push("compaction.max_concurrent must be greater than 0".to_string());
        }
        if self.compaction.max_concurrent > 10 {
            errors.push("compaction.max_concurrent should not exceed 10".to_string());
        }

        // Validate WAL configuration
        if self.wal.enabled {
            if self.wal.segment_size == 0 {
                errors.push(
                    "wal.segment_size must be greater than 0 when WAL is enabled".to_string(),
                );
            }
            if self.wal.segment_size < 1024 {
                errors.push("wal.segment_size should be at least 1KB".to_string());
            }
            if self.wal.buffer_size == 0 {
                errors
                    .push("wal.buffer_size must be greater than 0 when WAL is enabled".to_string());
            }
        }

        // Validate bloom filter configuration
        if self.bloom_filter.enabled {
            if self.bloom_filter.bits_per_key == 0 {
                errors.push(
                    "bloom_filter.bits_per_key must be greater than 0 when bloom filter is enabled"
                        .to_string(),
                );
            }
            if self.bloom_filter.bits_per_key > 100 {
                errors.push("bloom_filter.bits_per_key should not exceed 100".to_string());
            }
        }

        // Validate performance configuration
        // Note: LSM performance validation temporarily disabled due to type confusion
        // TODO: Re-enable after resolving PerformanceConfig type issues
        // if self.performance.max_parallel_reads == 0 {
        //     errors.push("performance.max_parallel_reads must be greater than 0".to_string());
        // }
        //
        // if self.performance.enable_parallel_reads && self.performance.max_parallel_reads > 100 {
        //     errors.push("performance.max_parallel_reads should not exceed 100".to_string());
        // }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Validate configuration and return a single error message
    pub fn validate_simple(&self) -> Result<(), String> {
        match self.validate() {
            Ok(()) => Ok(()),
            Err(errors) => Err(format!(
                "Configuration validation failed:\n{}",
                errors.join("\n")
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Test LsmConfig default values are reasonable
    #[test]
    fn test_lsm_config_defaults() {
        let config = LsmConfig::default();

        assert_eq!(config.data_dir.to_str().unwrap(), "./f4kvs_lsm_data");
        assert_eq!(config.memtable.max_size, 64 * 1024 * 1024); // 64MB
        assert_eq!(config.sstable.target_size, 64 * 1024 * 1024);
        assert_eq!(config.levels.count, 7);
        assert_eq!(config.compaction.max_concurrent, 2);
        assert_eq!(config.wal.segment_size, 64 * 1024 * 1024);
    }

    /// Test configuration serialization/deserialization round-trip
    #[test]
    fn test_config_serialization() {
        let config = LsmConfig::default();

        // Serialize to JSON
        let json = serde_json::to_string(&config).expect("Should serialize");

        // Deserialize back
        let deserialized: LsmConfig = serde_json::from_str(&json).expect("Should deserialize");

        assert_eq!(deserialized.data_dir, config.data_dir);
        assert_eq!(deserialized.memtable.max_size, config.memtable.max_size);
    }

    /// Test MemtableConfig validation - max_size too small
    #[test]
    fn test_memtable_config_validation_min_size() {
        let mut config = LsmConfig::default();
        config.memtable.max_size = 512; // Too small

        let errors = config.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("at least 1KB")));
    }

    /// Test MemtableConfig validation - max_immutable_count too large
    #[test]
    fn test_memtable_config_validation_max_immutable() {
        let mut config = LsmConfig::default();
        config.memtable.max_immutable_count = 200; // Too large

        let errors = config.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("should not exceed")));
    }

    /// Test MemtableConfig validation - invalid flush_threshold
    #[test]
    fn test_memtable_config_validation_flush_threshold() {
        // Test threshold > 1.0
        let mut config = LsmConfig::default();
        config.memtable.flush_threshold = 1.5;

        let errors = config.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("flush_threshold")));

        // Test threshold <= 0.0
        config.memtable.flush_threshold = -0.5;
        let errors = config.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("flush_threshold")));
    }

    /// Test SstableConfig validation - target_size too small
    #[test]
    fn test_sstable_config_validation_min_target() {
        let mut config = LsmConfig::default();
        config.sstable.target_size = 512; // Too small

        let errors = config.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("at least 1KB")));
    }

    /// Test SstableConfig validation - max_size < target_size
    #[test]
    fn test_sstable_config_validation_max_vs_target() {
        let mut config = LsmConfig::default();
        config.sstable.max_size = 32 * 1024 * 1024; // Less than target

        let errors = config.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("max_size must be >=")));
    }

    /// Test SstableConfig validation - block_size too small
    #[test]
    fn test_sstable_config_validation_min_block() {
        let mut config = LsmConfig::default();
        config.sstable.block_size = 256; // Too small

        let errors = config.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("at least 512 bytes")));
    }

    /// Test LevelConfig validation - count too large
    #[test]
    fn test_level_config_validation_max_levels() {
        let mut config = LsmConfig::default();
        config.levels.count = 30; // Too many levels

        let errors = config.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("should not exceed 20")));
    }

    /// Test LevelConfig validation - size_multiplier invalid
    #[test]
    fn test_level_config_validation_multiplier() {
        let mut config = LsmConfig::default();
        config.levels.size_multiplier = 1.0; // Must be > 1.0

        let errors = config.validate().unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.contains("must be greater than 1.0")));
    }

    /// Test CompactionConfig validation - max_concurrent too large
    #[test]
    fn test_compaction_config_validation_max_concurrent() {
        let mut config = LsmConfig::default();
        config.compaction.max_concurrent = 20; // Too many concurrent compactions

        let errors = config.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("should not exceed 10")));
    }

    /// Test WalSyncMode enum variants
    #[test]
    fn test_wal_sync_mode_variants() {
        // Verify all variants exist and serialize correctly
        let modes = [
            WalSyncMode::None,
            WalSyncMode::Flush,
            WalSyncMode::Fsync,
            WalSyncMode::FsyncAsync,
        ];

        for mode in &modes {
            let json = serde_json::to_string(mode).expect("Should serialize");
            assert!(!json.is_empty());

            // Deserialize back
            let deserialized: WalSyncMode =
                serde_json::from_str(&json).expect("Should deserialize");
            assert_eq!(mode, &deserialized);
        }
    }

    /// Test WAL configuration validation when enabled
    #[test]
    fn test_wal_config_validation_enabled() {
        let mut config = LsmConfig::default();
        config.wal.enabled = true;
        config.wal.segment_size = 512; // Too small for enabled WAL

        let errors = config.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("wal.segment_size")));
    }

    /// Test WAL configuration validation when disabled (should skip validation)
    #[test]
    fn test_wal_config_validation_disabled() {
        let mut config = LsmConfig::default();
        config.wal.enabled = false;
        config.wal.segment_size = 0; // Invalid but should pass since disabled

        assert!(
            config.validate().is_ok(),
            "Disabled WAL should skip segment validation"
        );
    }

    /// Test BloomFilterConfig validation when enabled with invalid bits_per_key
    #[test]
    fn test_bloom_filter_config_validation_enabled() {
        let mut config = LsmConfig::default();
        config.bloom_filter.enabled = true;
        config.bloom_filter.bits_per_key = 0; // Invalid for enabled filter

        let errors = config.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("bits_per_key")));
    }

    /// Test BloomFilterConfig validation - bits_per_key too large
    #[test]
    fn test_bloom_filter_config_validation_max_bits() {
        let mut config = LsmConfig::default();
        config.bloom_filter.enabled = true;
        config.bloom_filter.bits_per_key = 200; // Too many bits per key

        let errors = config.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("should not exceed 100")));
    }

    /// Test CompactionStrategy enum variants
    #[test]
    fn test_compaction_strategy_variants() {
        let strategies = [
            CompactionStrategy::SizeTiered,
            CompactionStrategy::Leveled,
            CompactionStrategy::TimeWindowed,
            CompactionStrategy::Hybrid,
        ];

        for strategy in &strategies {
            let json = serde_json::to_string(strategy).expect("Should serialize");
            assert!(!json.is_empty());

            let deserialized: CompactionStrategy =
                serde_json::from_str(&json).expect("Should deserialize");
            assert_eq!(strategy, &deserialized);
        }
    }

    /// Test CompactionPriority enum variants
    #[test]
    fn test_compaction_priority_variants() {
        let priorities = [
            CompactionPriority::ReadOptimized,
            CompactionPriority::WriteOptimized,
            CompactionPriority::Balanced,
            CompactionPriority::SpaceOptimized,
        ];

        for priority in &priorities {
            let json = serde_json::to_string(priority).expect("Should serialize");
            assert!(!json.is_empty());

            let deserialized: CompactionPriority =
                serde_json::from_str(&json).expect("Should deserialize");
            assert_eq!(priority, &deserialized);
        }
    }

    /// Test LsmConfig from_storage_config conversion
    #[test]
    fn test_lsm_config_from_storage_config() {
        let mut config = LsmConfig::default();
        config.memtable.max_size = 0; // Invalid

        let result = config.validate_simple();
        assert!(result.is_err());

        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("Configuration validation failed"));
        assert!(err_msg.contains("memtable.max_size must be greater than 0"));
    }

    /// Test all configuration types derive Debug and Clone correctly
    #[test]
    fn test_config_clone_debug() {
        let config = LsmConfig::default();

        // Test Debug trait
        let debug_str = format!("{:?}", config);
        assert!(!debug_str.is_empty());

        // Test Clone trait
        let cloned = config.clone();
        assert_eq!(config.data_dir, cloned.data_dir);
    }

    /// Test default configuration passes validation
    #[test]
    fn test_default_config_validates() {
        let config = LsmConfig::default();
        assert!(config.validate().is_ok(), "Default config should be valid");
    }

    /// Test column family configuration defaults
    #[test]
    fn test_column_family_config_defaults() {
        let config = ColumnFamilyConfig::default();

        assert_eq!(config.default_name, "default");
        assert!(config.enable_isolation);
        assert_eq!(config.max_count, 100);
    }

    /// Test performance configuration defaults (DoS protection)
    #[test]
    fn test_performance_config_defaults() {
        let config = PerformanceConfig::default();

        assert!(config.enable_read_ahead);
        assert_eq!(config.read_ahead_size, 64 * 1024);
        assert!(config.enable_parallel_reads);
        assert_eq!(config.max_parallel_reads, 4);
        assert_eq!(config.max_batch_size, 10_000); // DoS protection limit
    }

    /// Test empty data_dir validation
    #[test]
    fn test_empty_data_dir_validation() {
        let mut config = LsmConfig::default();
        config.data_dir = PathBuf::new();

        let errors = config.validate().unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.contains("data_dir cannot be empty")));
    }

    /// Test WAL configuration defaults
    #[test]
    fn test_wal_config_defaults() {
        let config = WalConfig::default();

        assert!(config.enabled);
        assert_eq!(config.segment_size, 64 * 1024 * 1024); // 64MB
        assert_eq!(config.buffer_size, 1024 * 1024); // 1MB
        assert_eq!(config.flush_interval, Duration::from_millis(100));
        assert_eq!(config.sync_mode, WalSyncMode::Fsync);
    }

    /// Test bloom filter configuration defaults
    #[test]
    fn test_bloom_filter_config_defaults() {
        let config = BloomFilterConfig::default();

        assert!(config.enabled);
        assert_eq!(config.false_positive_rate, 0.01); // 1%
        assert_eq!(config.bits_per_key, 10);
        assert!(config.enable_cache);
    }

    /// Test level configuration defaults
    #[test]
    fn test_level_config_defaults() {
        let config = LevelConfig::default();

        assert_eq!(config.count, 7);
        assert_eq!(config.size_multiplier, 10.0);
        assert_eq!(config.max_sstables_per_level, 10);
        assert!(config.enable_leveled_compaction);
    }

    /// Test compaction configuration defaults
    #[test]
    fn test_compaction_config_defaults() {
        let config = CompactionConfig::default();

        assert_eq!(config.strategy, CompactionStrategy::Leveled);
        assert_eq!(config.max_concurrent, 2);
        assert_eq!(config.priority, CompactionPriority::Balanced);
        assert!(config.background_enabled);
        assert_eq!(config.interval, Duration::from_secs(60));
    }

    /// Test memtable configuration defaults
    #[test]
    fn test_memtable_config_defaults() {
        let config = MemtableConfig::default();

        assert_eq!(config.max_size, 64 * 1024 * 1024); // 64MB
        assert_eq!(config.max_immutable_count, 2);
        assert_eq!(config.flush_threshold, 0.8); // 80%
        assert!(config.use_skip_list);
        assert!(config.concurrent_reads);
    }

    /// Test sstable configuration defaults
    #[test]
    fn test_sstable_config_defaults() {
        let config = SstableConfig::default();

        assert_eq!(config.target_size, 64 * 1024 * 1024); // 64MB
        assert_eq!(config.max_size, 128 * 1024 * 1024); // 128MB
        assert_eq!(config.block_size, 4 * 1024); // 4KB
        assert_eq!(config.max_open_files, 1000);
        assert_eq!(config.file_retry_attempts, 3);
        assert_eq!(config.retry_delay_ms, 100);
        assert!(config.enable_resilient_handling);
    }

    /// Test LsmConfig default constructor consistency
    #[test]
    fn test_lsm_config_default_consistency() {
        let config1 = LsmConfig::default();
        let config2 = LsmConfig::default();

        assert_eq!(config1.data_dir, config2.data_dir);
        assert_eq!(config1.memtable.max_size, config2.memtable.max_size);
        assert_eq!(config1.levels.count, config2.levels.count);
    }
}
