//! Performance metrics for the LSM engine
//!
//! This module contains types for tracking and reporting LSM engine performance.

/// Performance metrics for the LSM engine
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    /// Total number of operations performed
    pub total_operations: u64,
    /// Total number of read operations
    pub read_operations: u64,
    /// Total number of write operations
    pub write_operations: u64,
    /// Total number of delete operations
    pub delete_operations: u64,
    /// Total number of SSTable files
    pub total_sstables: usize,
    /// Total size of all data in bytes
    pub total_size_bytes: u64,
    /// Number of entries in memtables
    pub memtable_entries: u64,
    /// Number of immutable memtables
    pub immutable_memtables: u64,
    /// Metrics for each LSM level
    pub level_metrics: Vec<LevelMetrics>,
    /// Average read latency in milliseconds
    pub avg_read_latency_ms: f64,
    /// Average write latency in milliseconds
    pub avg_write_latency_ms: f64,
    /// Number of compaction operations performed
    pub compaction_count: u64,
    /// Timestamp of last compaction
    pub last_compaction_time: Option<u64>,
}

/// Metrics for a specific LSM level
#[derive(Debug, Clone)]
pub struct LevelMetrics {
    /// Level number (0-based)
    pub level: u32,
    /// Number of files in this level
    pub file_count: u32,
    /// Total size of files in this level in bytes
    pub total_size_bytes: u64,
    /// Average file size in this level in bytes
    pub avg_file_size_bytes: u64,
}

/// Optimization recommendation for storage performance
#[derive(Debug, Clone)]
pub struct OptimizationRecommendation {
    /// Category of the recommendation
    pub category: String,
    /// Priority level of the recommendation
    pub priority: OptimizationPriority,
    /// Title of the recommendation
    pub title: String,
    /// Detailed description of the issue
    pub description: String,
    /// Recommended action to take
    pub action: String,
}

/// Priority level for optimization recommendations
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum OptimizationPriority {
    /// Low priority - minor optimization
    Low,
    /// Medium priority - moderate optimization
    Medium,
    /// High priority - important optimization
    High,
    /// Critical priority - urgent optimization needed
    Critical,
}
