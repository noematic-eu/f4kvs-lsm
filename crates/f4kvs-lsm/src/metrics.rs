//! Metrics integration for LSM engine operations
//!
//! This module provides metrics recording for WAL and LSM operations.
//! Metrics are only compiled when the "metrics" feature is enabled.

#[cfg(feature = "metrics")]
use f4kvs_monitoring::storage_metrics_integration::StorageMetricsRecorder;
#[cfg(feature = "metrics")]
use std::sync::Arc;
use std::time::Duration;

/// Optional metrics recorder for storage operations
#[cfg(feature = "metrics")]
pub type MetricsRecorder = Option<Arc<dyn StorageMetricsRecorder + Send + Sync>>;

#[cfg(not(feature = "metrics"))]
pub type MetricsRecorder = ();

/// Record WAL write operation metrics
pub fn record_wal_write(metrics: &MetricsRecorder, bytes: usize, duration: Duration) {
    #[cfg(feature = "metrics")]
    if let Some(recorder) = metrics {
        recorder.record_wal_write(bytes, duration);
    }
}

/// Record WAL fsync operation metrics
pub fn record_wal_fsync(metrics: &MetricsRecorder, duration: Duration) {
    #[cfg(feature = "metrics")]
    if let Some(recorder) = metrics {
        recorder.record_wal_fsync(duration);
    }
}

/// Record WAL recovery operation metrics
pub fn record_wal_recovery(metrics: &MetricsRecorder, duration: Duration) {
    #[cfg(feature = "metrics")]
    if let Some(recorder) = metrics {
        recorder.record_wal_recovery(duration);
    }
}

/// Record WAL error metrics
pub fn record_wal_error(metrics: &MetricsRecorder) {
    #[cfg(feature = "metrics")]
    if let Some(recorder) = metrics {
        recorder.record_wal_error();
    }
}

/// Record LSM compaction operation metrics
pub fn record_compaction(
    metrics: &MetricsRecorder,
    bytes_read: usize,
    bytes_written: usize,
    duration: Duration,
) {
    #[cfg(feature = "metrics")]
    if let Some(recorder) = metrics {
        recorder.record_compaction(bytes_read, bytes_written, duration);
    }
}

/// Record LSM memtable flush operation metrics
pub fn record_memtable_flush(metrics: &MetricsRecorder, entries: usize, duration: Duration) {
    #[cfg(feature = "metrics")]
    if let Some(recorder) = metrics {
        recorder.record_memtable_flush(entries, duration);
    }
}

/// Record LSM SSTable read operation metrics
pub fn record_sstable_read(metrics: &MetricsRecorder) {
    #[cfg(feature = "metrics")]
    if let Some(recorder) = metrics {
        recorder.record_sstable_read();
    }
}

/// Record bloom filter hit metrics
pub fn record_bloom_filter_hit(metrics: &MetricsRecorder) {
    #[cfg(feature = "metrics")]
    if let Some(recorder) = metrics {
        recorder.record_bloom_filter_hit();
    }
}

/// Record bloom filter miss metrics
pub fn record_bloom_filter_miss(metrics: &MetricsRecorder) {
    #[cfg(feature = "metrics")]
    if let Some(recorder) = metrics {
        recorder.record_bloom_filter_miss();
    }
}
