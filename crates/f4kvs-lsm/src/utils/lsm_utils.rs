//! Utility functions for LSM Tree Engine
//!
//! This module provides LSM-specific utilities and re-exports common utilities.

use crate::error::Result;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

// Re-export common utilities from storage core
pub use f4kvs_storage_core::common::formatting::format_bytes;
pub use f4kvs_storage_core::common::io::ensure_directory_exists_async as ensure_dir;

/// Get the current timestamp in seconds since UNIX epoch
pub fn timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Generate a unique filename for SSTable
pub fn generate_sstable_filename(level: usize, sequence: u64) -> String {
    format!("L{:02}_{:016}.sst", level, sequence)
}

/// Generate a unique filename for WAL segment
pub fn generate_wal_filename(sequence: u64) -> String {
    format!("wal_{:016}.log", sequence)
}

/// Ensure directory exists, create if it doesn't (legacy function)
///
/// This function is kept for backward compatibility. New code should use
/// `ensure_dir` from the common module.
pub async fn ensure_dir_legacy(path: &PathBuf) -> Result<()> {
    ensure_dir(path)
        .await
        .map_err(|e| crate::error::LsmError::Io(std::io::Error::other(e.to_string())))
}

/// Calculate optimal bloom filter size
pub fn calculate_bloom_filter_size(expected_elements: usize, false_positive_rate: f64) -> usize {
    // m = -n * ln(p) / (ln(2)^2)
    let n = expected_elements as f64;
    let p = false_positive_rate;
    let m = (-n * p.ln()) / (2.0_f64.ln().powi(2));
    m.ceil() as usize
}

/// Calculate optimal number of hash functions
pub fn calculate_hash_functions(expected_elements: usize, filter_size: usize) -> usize {
    // k = (m/n) * ln(2)
    let m = filter_size as f64;
    let n = expected_elements as f64;
    let k = (m / n) * 2.0_f64.ln();
    k.ceil() as usize
}
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_sstable_filename_format() {
        assert_eq!(generate_sstable_filename(0, 1), "L00_0000000000000001.sst");
        assert_eq!(
            generate_sstable_filename(5, 999),
            "L05_0000000000000999.sst"
        );
    }

    #[test]
    fn test_wal_filename_format() {
        assert_eq!(generate_wal_filename(0), "wal_0000000000000000.log");
        assert_eq!(
            generate_wal_filename(u64::MAX),
            "wal_18446744073709551615.log"
        );
    }

    #[test]
    fn test_unique_filenames() {
        let sstable = generate_sstable_filename(0, 1);
        let wal = generate_wal_filename(1);
        assert_ne!(sstable, wal);
    }

    #[test]
    fn test_bloom_filter_size_formula() {
        let size = calculate_bloom_filter_size(1000, 0.01);
        assert!(size > 0);
    }

    #[test]
    fn test_bloom_filter_edge_cases() {
        let size_min = calculate_bloom_filter_size(1, 0.01);
        assert!(size_min > 0);
        let size_loose = calculate_bloom_filter_size(1000, 0.5);
        let size_strict = calculate_bloom_filter_size(1000, 0.001);
        assert!(size_loose < size_strict);
    }

    #[test]
    fn test_hash_functions_calculation() {
        let k = calculate_hash_functions(1000, 9585);
        assert!(k >= 1 && k <= 20);
    }

    #[test]
    fn test_timestamp_secs_returns_u64() {
        let ts = timestamp_secs();
        assert!(ts > 0);
    }

    #[tokio::test]
    async fn test_ensure_dir_legacy_creates_directory() {
        let temp_dir = TempDir::new().unwrap();
        let new_path: PathBuf = temp_dir.path().join("subdir");
        ensure_dir_legacy(&new_path).await.unwrap();
        assert!(new_path.exists());
    }

    #[tokio::test]
    async fn test_bloom_filter_properties() {
        let m_loose = calculate_bloom_filter_size(1000, 0.1);
        let m_strict = calculate_bloom_filter_size(1000, 0.001);
        assert!(m_loose < m_strict);
    }

    #[test]
    fn test_bloom_filter_formula_accuracy() {
        let size_100 = calculate_bloom_filter_size(100, 0.01);
        assert!(size_100 > 0 && size_100 < 2000);
    }
}
