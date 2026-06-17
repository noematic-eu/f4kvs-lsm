#![deny(missing_docs)]
//! # F4KVS Storage Core
//!
//! This crate provides the core traits, configuration, and utilities shared across
//! all F4KVS storage engine implementations.
//!
//! ## Overview
//!
//! The storage core crate defines:
//! - `StorageEngine` trait - The main interface all storage engines must implement
//! - Configuration structures - `StorageConfig`, `StorageBackend`, etc.
//! - Statistics and monitoring - `StorageStats`, `StorageMonitor`
//! - Common utilities - Validation, I/O helpers
//!
//! ## Usage
//!
//! Storage engine implementations should depend on this crate and implement
//! the `StorageEngine` trait:
//!
//! ```rust,ignore
//! use f4kvs_storage_core::traits::StorageEngine;
//! use f4kvs_storage_core::{Result, Value};
//!
//! struct MyEngine { /* ... */ }
//!
//! #[async_trait::async_trait]
//! impl StorageEngine for MyEngine {
//!     // Implement required methods...
//! }
//! ```

// Re-export value types for convenience
pub use f4kvs_value::{F4KvsError, Result, Value};

// Core storage traits and interfaces
pub mod adapter;
pub mod storage_traits;
pub mod config;
pub mod monitoring;
pub mod stats;
pub mod traits;

// Common utilities and shared types
pub mod common;

// Re-exports for convenience
pub use adapter::StorageAdapter;
pub use config::{
    BufferPoolConfig, CacheConfig, CompactionConfig, CompactionStrategy, CompactionStyle,
    EvictionPolicy, LsmTreeConfig, MemoryConfig, PartitionedStorageConfig, StorageBackend,
    StorageConfig, TieringPolicy, WALConfig,
};
pub use monitoring::{
    Alert, AlertCondition, AlertManager, AlertRule, AlertSeverity, AlertStatus, AlertThresholds,
    HealthCheckResult, HealthChecker, MetricSnapshot, MetricType, MetricValue, MetricsCollector,
    MonitoringConfig, NotificationChannel, NotificationType, StorageMonitor,
};
pub use stats::{
    BloomFilterStats, CacheMetrics, CacheStats, ColumnFamilyStats, CompactionStats, HealthStats,
    HealthStatus, IoMetrics, IoStats, LevelStats, MemoryStats, StorageStats, WALStats,
};
pub use traits::{
    Compactable, CompactionConfig as TraitCompactionConfig,
    CompactionStats as TraitCompactionStats, HealthCheck, HealthStatus as TraitHealthStatus,
    KeyValueIterator, LevelStats as TraitLevelStats, Snapshot, Storage, StorageEngine, Transaction,
};

/// Version information for the storage core module
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        assert!(!VERSION.is_empty());
    }

    #[test]
    fn test_storage_backend_display() {
        assert_eq!(StorageBackend::Memory.to_string(), "memory");
        assert_eq!(StorageBackend::LsmTree.to_string(), "lsm-tree");
        assert_eq!(StorageBackend::Partitioned.to_string(), "partitioned");
    }
}
