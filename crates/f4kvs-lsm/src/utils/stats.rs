//! Statistics for LSM Tree Engine

use serde::{Deserialize, Serialize};

/// LSM engine statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LsmStats {
    /// Total number of keys
    pub total_keys: u64,

    /// Total number of reads
    pub total_reads: u64,

    /// Total number of writes
    pub total_writes: u64,

    /// Total number of deletes
    pub total_deletes: u64,

    /// Total bytes written
    pub total_bytes_written: u64,

    /// Total bytes read
    pub total_bytes_read: u64,

    /// Memtable hits
    pub memtable_hits: u64,

    /// SSTable hits
    pub sstable_hits: u64,

    /// Cache misses
    pub misses: u64,

    /// Memory usage in bytes
    pub memory_usage: u64,

    /// Disk usage in bytes
    pub disk_usage: u64,

    /// Number of SSTables
    pub sstable_count: u64,

    /// Number of levels
    pub level_count: u64,

    /// Compaction operations
    pub compaction_count: u64,

    /// Last compaction time
    pub last_compaction: Option<u64>,

    /// Compaction metrics
    pub compaction_metrics: CompactionMetrics,

    /// Memtable metrics
    pub memtable_metrics: MemtableMetrics,

    /// SSTable level metrics (per level)
    pub level_metrics: Vec<LevelMetrics>,
}

/// Compaction metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CompactionMetrics {
    /// Total number of compactions performed
    pub total_compactions: u64,
    /// Total entries processed during compaction
    pub total_entries_processed: u64,
    /// Total entries removed (duplicates/deleted) during compaction
    pub total_entries_removed: u64,
    /// Total space reclaimed in bytes
    pub total_space_reclaimed: u64,
    /// Total compaction duration in milliseconds
    pub total_compaction_duration_ms: u64,
    /// Average compaction duration in milliseconds
    pub avg_compaction_duration_ms: f64,
    /// Write amplification factor
    pub write_amplification: f64,
    /// Number of levels compacted
    pub levels_compacted: u64,
    /// Number of SSTables merged
    pub sstables_merged: u64,
}

/// Memtable metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MemtableMetrics {
    /// Current active memtable size in bytes
    pub active_memtable_size: u64,
    /// Current active memtable entry count
    pub active_memtable_entries: u64,
    /// Number of immutable memtables
    pub immutable_memtable_count: u64,
    /// Total size of immutable memtables in bytes
    pub immutable_memtable_size: u64,
    /// Number of memtable flushes
    pub flush_count: u64,
    /// Average flush duration in milliseconds
    pub avg_flush_duration_ms: f64,
    /// Last flush time
    pub last_flush_time: Option<u64>,
}

/// SSTable level metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LevelMetrics {
    /// Level number (0-based)
    pub level: usize,
    /// Number of SSTables in this level
    pub sstable_count: u64,
    /// Total size of all SSTables in this level in bytes
    pub total_size_bytes: u64,
    /// Average SSTable size in bytes
    pub avg_sstable_size_bytes: u64,
    /// Smallest key in this level
    pub smallest_key: Option<String>,
    /// Largest key in this level
    pub largest_key: Option<String>,
}

impl LsmStats {
    /// Get total operations
    pub fn total_operations(&self) -> u64 {
        self.total_reads + self.total_writes + self.total_deletes
    }

    /// Get hit rate
    pub fn hit_rate(&self) -> f64 {
        let total_hits = self.memtable_hits + self.sstable_hits;
        let total_requests = total_hits + self.misses;

        if total_requests == 0 {
            0.0
        } else {
            total_hits as f64 / total_requests as f64
        }
    }

    /// Get memtable hit rate
    pub fn memtable_hit_rate(&self) -> f64 {
        let total_hits = self.memtable_hits + self.sstable_hits;

        if total_hits == 0 {
            0.0
        } else {
            self.memtable_hits as f64 / total_hits as f64
        }
    }

    /// Get average read size
    pub fn average_read_size(&self) -> f64 {
        if self.total_reads == 0 {
            0.0
        } else {
            self.total_bytes_read as f64 / self.total_reads as f64
        }
    }

    /// Get average write size
    pub fn average_write_size(&self) -> f64 {
        if self.total_writes == 0 {
            0.0
        } else {
            self.total_bytes_written as f64 / self.total_writes as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lsm_stats_default() {
        let stats: LsmStats = Default::default();

        assert_eq!(stats.total_keys, 0);
        assert_eq!(stats.total_reads, 0);
        assert_eq!(stats.total_writes, 0);
        assert_eq!(stats.total_deletes, 0);
        assert!(stats.compaction_metrics.total_compactions == 0);
    }

    #[test]
    fn test_lsm_stats_total_operations_empty() {
        let stats = LsmStats::default();

        assert_eq!(stats.total_operations(), 0);
    }

    #[test]
    fn test_lsm_stats_total_operations_with_data() {
        let mut stats = LsmStats::default();
        stats.total_reads = 100;
        stats.total_writes = 50;
        stats.total_deletes = 25;

        assert_eq!(stats.total_operations(), 175);
    }

    #[test]
    fn test_lsm_stats_hit_rate_zero_requests() {
        let stats = LsmStats::default();

        // No requests means hit rate should be 0.0, not NaN or infinity
        assert_eq!(stats.hit_rate(), 0.0);
    }

    #[test]
    fn test_lsm_stats_hit_rate_with_hits() {
        let mut stats = LsmStats::default();
        stats.memtable_hits = 80;
        stats.sstable_hits = 20;
        stats.misses = 100; // Total requests = 200

        let hit_rate = stats.hit_rate();

        assert_eq!(hit_rate, 0.5); // 100 hits / 200 requests
    }

    #[test]
    fn test_lsm_stats_hit_rate_all_hits() {
        let mut stats = LsmStats::default();
        stats.memtable_hits = 90;
        stats.sstable_hits = 10;

        // No misses, so hit rate should be 1.0
        assert_eq!(stats.hit_rate(), 1.0);
    }

    #[test]
    fn test_lsm_stats_hit_rate_no_hits() {
        let mut stats = LsmStats::default();
        stats.misses = 50;

        // All misses, so hit rate should be 0.0
        assert_eq!(stats.hit_rate(), 0.0);
    }

    #[test]
    fn test_lsm_stats_memtable_hit_rate() {
        let mut stats = LsmStats::default();
        stats.memtable_hits = 75;
        stats.sstable_hits = 25;

        // Memtable hit rate = memtable hits / total hits = 75/100 = 0.75
        assert_eq!(stats.memtable_hit_rate(), 0.75);
    }

    #[test]
    fn test_lsm_stats_memtable_hit_rate_no_hits() {
        let stats = LsmStats::default();

        // No hits means memtable hit rate should be 0.0, not NaN
        assert_eq!(stats.memtable_hit_rate(), 0.0);
    }

    #[test]
    fn test_compaction_metrics_default() {
        let metrics = CompactionMetrics::default();

        assert_eq!(metrics.total_compactions, 0);
        assert_eq!(metrics.avg_compaction_duration_ms, 0.0);
        assert_eq!(metrics.write_amplification, 0.0);
    }

    #[test]
    fn test_compaction_metrics_with_data() {
        let mut metrics = CompactionMetrics::default();
        metrics.total_compactions = 10;
        metrics.total_entries_processed = 10000;
        metrics.total_entries_removed = 500;
        metrics.total_space_reclaimed = 1_000_000;
        metrics.avg_compaction_duration_ms = 250.5;
        metrics.write_amplification = 1.5;

        assert_eq!(metrics.total_compactions, 10);
        assert_eq!(metrics.total_entries_processed, 10000);
        assert_eq!(metrics.total_space_reclaimed, 1_000_000);
    }

    #[test]
    fn test_memtable_metrics_default() {
        let metrics = MemtableMetrics::default();

        assert_eq!(metrics.active_memtable_size, 0);
        assert_eq!(metrics.flush_count, 0);
        assert_eq!(metrics.avg_flush_duration_ms, 0.0);
    }

    #[test]
    fn test_memtable_metrics_with_data() {
        let mut metrics = MemtableMetrics::default();
        metrics.active_memtable_size = 1_048_576; // 1MB
        metrics.active_memtable_entries = 10000;
        metrics.flush_count = 25;
        metrics.avg_flush_duration_ms = 50.3;

        assert_eq!(metrics.active_memtable_size, 1_048_576);
        assert_eq!(metrics.flush_count, 25);
    }

    #[test]
    fn test_level_metrics_default() {
        let metrics = LevelMetrics::default();

        assert_eq!(metrics.level, 0);
        assert_eq!(metrics.sstable_count, 0);
        assert!(metrics.smallest_key.is_none());
        assert!(metrics.largest_key.is_none());
    }

    #[test]
    fn test_level_metrics_with_data() {
        let mut metrics = LevelMetrics::default();
        metrics.level = 2;
        metrics.sstable_count = 5;
        metrics.total_size_bytes = 10_000_000; // 10MB
        metrics.avg_sstable_size_bytes = 2_000_000;
        metrics.smallest_key = Some("a".to_string());
        metrics.largest_key = Some("z".to_string());

        assert_eq!(metrics.level, 2);
        assert_eq!(metrics.sstable_count, 5);
        assert_eq!(metrics.total_size_bytes, 10_000_000);
    }

    #[test]
    fn test_lsm_stats_serialization() {
        let mut stats = LsmStats::default();
        stats.total_keys = 1000;
        stats.total_reads = 500;

        // Test serialization to JSON
        let json = serde_json::to_string(&stats);
        assert!(json.is_ok());

        // Deserialize from JSON
        let deserialized: LsmStats = serde_json::from_str(&json.unwrap()).unwrap();
        assert_eq!(deserialized.total_keys, 1000);
        assert_eq!(deserialized.total_reads, 500);
    }

    #[test]
    fn test_lsm_stats_clone() {
        let mut stats = LsmStats::default();
        stats.total_writes = 200;

        // Test Clone trait
        let cloned = stats.clone();

        assert_eq!(cloned.total_writes, stats.total_writes);
    }

    #[test]
    fn test_lsm_stats_debug_formatting() {
        let mut stats = LsmStats::default();
        stats.total_keys = 100;

        // Test Debug trait
        let debug_str = format!("{:?}", stats);
        assert!(debug_str.contains("LsmStats"));
    }

    #[test]
    fn test_compaction_metrics_clone() {
        let mut metrics = CompactionMetrics::default();
        metrics.total_compactions = 5;

        let cloned = metrics.clone();
        assert_eq!(cloned.total_compactions, 5);
    }

    #[test]
    fn test_memtable_metrics_clone() {
        let mut metrics = MemtableMetrics::default();
        metrics.flush_count = 10;

        let cloned = metrics.clone();
        assert_eq!(cloned.flush_count, 10);
    }

    #[test]
    fn test_level_metrics_clone() {
        let mut metrics = LevelMetrics::default();
        metrics.level = 1;
        metrics.smallest_key = Some("a".to_string());

        let cloned = metrics.clone();
        assert_eq!(cloned.level, 1);
        assert_eq!(cloned.smallest_key, Some("a".to_string()));
    }

    #[test]
    fn test_lsm_stats_all_metrics_zero() {
        let stats = LsmStats::default();

        assert_eq!(stats.total_keys, 0);
        assert_eq!(stats.total_reads, 0);
        assert_eq!(stats.total_writes, 0);
        assert_eq!(stats.total_deletes, 0);
        assert_eq!(stats.total_bytes_written, 0);
        assert_eq!(stats.total_bytes_read, 0);
        assert_eq!(stats.memtable_hits, 0);
        assert_eq!(stats.sstable_hits, 0);
        assert_eq!(stats.misses, 0);
    }

    #[test]
    fn test_lsm_stats_with_all_metrics() {
        let mut stats = LsmStats::default();
        stats.total_keys = 10000;
        stats.total_reads = 50000;
        stats.total_writes = 25000;
        stats.total_deletes = 5000;
        stats.total_bytes_written = 1_000_000_000; // 1GB
        stats.total_bytes_read = 5_000_000_000; // 5GB
        stats.memtable_hits = 30000;
        stats.sstable_hits = 15000;
        stats.misses = 5000;

        assert_eq!(stats.total_operations(), 80000);
        assert_eq!(stats.hit_rate(), 0.9); // 45000 / (45000 + 5000)
    }

    #[test]
    fn test_compaction_metrics_amplification() {
        let mut metrics = CompactionMetrics::default();
        metrics.total_entries_processed = 1000;
        metrics.write_amplification = 2.0; // Write amplification of 2x

        assert_eq!(metrics.write_amplification, 2.0);
    }

    #[test]
    fn test_level_metrics_no_keys() {
        let metrics = LevelMetrics::default();

        assert!(metrics.smallest_key.is_none());
        assert!(metrics.largest_key.is_none());
    }

    #[test]
    fn test_compaction_metrics_all_zero_values() {
        let metrics = CompactionMetrics::default();

        assert_eq!(metrics.total_entries_processed, 0);
        assert_eq!(metrics.total_entries_removed, 0);
        assert_eq!(metrics.total_space_reclaimed, 0);
        assert_eq!(metrics.total_compaction_duration_ms, 0);
    }

    #[test]
    fn test_memtable_metrics_all_zero_values() {
        let metrics = MemtableMetrics::default();

        assert_eq!(metrics.active_memtable_size, 0);
        assert_eq!(metrics.active_memtable_entries, 0);
        assert_eq!(metrics.immutable_memtable_count, 0);
    }

    #[test]
    fn test_lsm_stats_operations_calculation() {
        let mut stats = LsmStats::default();

        // Test various combinations of operations
        let test_cases = vec![
            (0, 0, 0, 0),
            (10, 0, 0, 10),
            (5, 5, 0, 10),
            (3, 2, 5, 10),
            (100, 200, 50, 350),
        ];

        for (reads, writes, deletes, expected_total) in test_cases {
            stats.total_reads = reads;
            stats.total_writes = writes;
            stats.total_deletes = deletes;

            assert_eq!(stats.total_operations(), expected_total);
        }
    }

    #[test]
    fn test_compaction_metrics_average_calculation() {
        let mut metrics = CompactionMetrics::default();

        // Test that average can be set and retrieved
        metrics.avg_compaction_duration_ms = 150.75;
        assert_eq!(metrics.avg_compaction_duration_ms, 150.75);
    }

    #[test]
    fn test_memtable_metrics_flush_timing() {
        let mut metrics = MemtableMetrics::default();

        // Test optional last flush time
        assert!(metrics.last_flush_time.is_none());

        metrics.last_flush_time = Some(1234567890);
        assert_eq!(metrics.last_flush_time, Some(1234567890));
    }

    #[test]
    fn test_level_metrics_key_range() {
        let mut metrics = LevelMetrics::default();

        // Test with full key range
        metrics.smallest_key = Some("a".to_string());
        metrics.largest_key = Some("z".to_string());

        assert_eq!(metrics.smallest_key, Some("a".to_string()));
        assert_eq!(metrics.largest_key, Some("z".to_string()));
    }

    #[test]
    fn test_lsm_stats_partial_metrics() {
        let mut stats = LsmStats::default();

        // Only set some metrics to ensure defaults work correctly
        stats.total_writes = 100;

        assert_eq!(stats.total_operations(), 100);
        assert_eq!(stats.hit_rate(), 0.0); // No hits or misses set
    }

    #[test]
    fn test_compaction_metrics_sstable_merging() {
        let mut metrics = CompactionMetrics::default();

        metrics.sstables_merged = 5;
        metrics.levels_compacted = 2;

        assert_eq!(metrics.sstables_merged, 5);
        assert_eq!(metrics.levels_compacted, 2);
    }

    #[test]
    fn test_memtable_metrics_immutables() {
        let mut metrics = MemtableMetrics::default();

        metrics.immutable_memtable_count = 3;
        metrics.immutable_memtable_size = 5_000_000; // 5MB

        assert_eq!(metrics.immutable_memtable_count, 3);
        assert_eq!(metrics.immutable_memtable_size, 5_000_000);
    }

    #[test]
    fn test_lsm_stats_level_metrics_empty() {
        let stats = LsmStats::default();

        // Default should have empty level metrics vector
        assert!(stats.level_metrics.is_empty());
    }

    #[test]
    fn test_compaction_metrics_duration_calculation() {
        let mut metrics = CompactionMetrics::default();

        // Test duration tracking
        metrics.total_compaction_duration_ms = 15000; // 15 seconds in ms
        assert_eq!(metrics.total_compaction_duration_ms, 15000);
    }

    #[test]
    fn test_lsm_stats_clone_preserves_all_fields() {
        let mut stats = LsmStats::default();
        stats.total_keys = 1234;
        stats.compaction_metrics.total_compactions = 567;

        let cloned = stats.clone();

        assert_eq!(cloned.total_keys, stats.total_keys);
        assert_eq!(
            cloned.compaction_metrics.total_compactions,
            stats.compaction_metrics.total_compactions
        );
    }

    #[test]
    fn test_lsm_stats_hit_rate_edge_cases() {
        let mut stats = LsmStats::default();

        // All hits (no misses) - should be 1.0
        stats.memtable_hits = 100;
        stats.sstable_hits = 50;
        assert_eq!(stats.hit_rate(), 1.0);

        // No hits at all - should be 0.0
        stats.memtable_hits = 0;
        stats.sstable_hits = 0;
        stats.misses = 100;
        assert_eq!(stats.hit_rate(), 0.0);
    }

    #[test]
    fn test_compaction_metrics_write_amplification_zero() {
        let metrics = CompactionMetrics::default();

        // Default should have zero write amplification
        assert_eq!(metrics.write_amplification, 0.0);
    }
}
