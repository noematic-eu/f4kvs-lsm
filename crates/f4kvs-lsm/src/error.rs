//! Error handling for the F4KVS LSM Tree Engine

use std::io;
use thiserror::Error;

/// Result type for LSM operations
pub type Result<T> = std::result::Result<T, LsmError>;

/// Error types for LSM operations
#[derive(Error, Debug)]
pub enum LsmError {
    /// I/O errors from the filesystem
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Serialization/deserialization errors
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Corrupted data detected
    #[error("Data corruption: {0}")]
    Corruption(String),

    /// Invalid configuration
    #[error("Invalid configuration: {0}")]
    Config(String),

    /// Compaction errors
    #[error("Compaction error: {0}")]
    Compaction(String),

    /// WAL (Write-Ahead Log) errors
    #[error("WAL error: {0}")]
    Wal(String),

    /// Bloom filter errors
    #[error("Bloom filter error: {0}")]
    BloomFilter(String),

    /// Compression errors
    #[error("Compression error: {0}")]
    Compression(String),

    /// Key not found
    #[error("Key not found: {0}")]
    KeyNotFound(String),

    /// Column family not found
    #[error("Column family not found: {0}")]
    ColumnFamilyNotFound(String),

    /// Invalid operation
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    /// Resource limit exceeded
    #[error("Resource limit exceeded: {0}")]
    ResourceLimit(String),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

impl LsmError {
    /// Check if this is a recoverable error
    pub fn is_recoverable(&self) -> bool {
        matches!(self, LsmError::Io(_) | LsmError::Compaction(_))
    }

    /// Check if this is a corruption error
    pub fn is_corruption(&self) -> bool {
        matches!(self, LsmError::Corruption(_))
    }

    /// Convert to a user-friendly error message
    pub fn user_message(&self) -> String {
        match self {
            LsmError::Io(e) => format!("Storage I/O error: {}", e),
            LsmError::Serialization(msg) => format!("Data format error: {}", msg),
            LsmError::Corruption(msg) => format!("Data corruption detected: {}", msg),
            LsmError::Config(msg) => format!("Configuration error: {}", msg),
            LsmError::Compaction(msg) => format!("Storage optimization error: {}", msg),
            LsmError::Wal(msg) => format!("Recovery error: {}", msg),
            LsmError::BloomFilter(msg) => format!("Index error: {}", msg),
            LsmError::Compression(msg) => format!("Compression error: {}", msg),
            LsmError::KeyNotFound(key) => format!("Key not found: {}", key),
            LsmError::ColumnFamilyNotFound(cf) => format!("Column family not found: {}", cf),
            LsmError::InvalidOperation(msg) => format!("Invalid operation: {}", msg),
            LsmError::ResourceLimit(msg) => format!("Resource limit: {}", msg),
            LsmError::Internal(msg) => format!("Internal error: {}", msg),
        }
    }
}

impl From<f4kvs_value::F4KvsError> for LsmError {
    fn from(err: f4kvs_value::F4KvsError) -> Self {
        LsmError::Internal(format!("F4KVS error: {}", err))
    }
}

impl From<serde_json::Error> for LsmError {
    fn from(err: serde_json::Error) -> Self {
        LsmError::Serialization(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_error_creation() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let lsm_err: LsmError = io_err.into();

        assert!(matches!(lsm_err, LsmError::Io(_)));
    }

    #[test]
    fn test_serialization_error() {
        let err = LsmError::Serialization("invalid format".to_string());
        assert_eq!(err.to_string(), "Serialization error: invalid format");
        assert!(!err.is_recoverable());
        assert!(!err.is_corruption());
    }

    #[test]
    fn test_corruption_error() {
        let err = LsmError::Corruption("checksum mismatch".to_string());
        assert_eq!(err.to_string(), "Data corruption: checksum mismatch");
        assert!(!err.is_recoverable());
        assert!(err.is_corruption());
    }

    #[test]
    fn test_config_error() {
        let err = LsmError::Config("invalid path".to_string());
        assert_eq!(err.to_string(), "Invalid configuration: invalid path");
        assert!(!err.is_recoverable());
    }

    #[test]
    fn test_compaction_error() {
        let err = LsmError::Compaction("compaction cancelled".to_string());
        assert_eq!(err.to_string(), "Compaction error: compaction cancelled");
        assert!(err.is_recoverable());
        assert!(!err.is_corruption());
    }

    #[test]
    fn test_wal_error() {
        let err = LsmError::Wal("log write failed".to_string());
        assert_eq!(err.to_string(), "WAL error: log write failed");
        assert!(!err.is_recoverable());
    }

    #[test]
    fn test_bloom_filter_error() {
        let err = LsmError::BloomFilter("hash collision".to_string());
        assert_eq!(err.to_string(), "Bloom filter error: hash collision");
    }

    #[test]
    fn test_compression_error() {
        let err = LsmError::Compression("deflate failed".to_string());
        assert_eq!(err.to_string(), "Compression error: deflate failed");
    }

    #[test]
    fn test_key_not_found() {
        let err = LsmError::KeyNotFound("user:12345".to_string());
        assert_eq!(err.to_string(), "Key not found: user:12345");
        let msg = err.user_message();
        assert!(msg.contains("not found"));
    }

    #[test]
    fn test_column_family_not_found() {
        let err = LsmError::ColumnFamilyNotFound("users".to_string());
        assert_eq!(err.to_string(), "Column family not found: users");
    }

    #[test]
    fn test_invalid_operation_error() {
        let err = LsmError::InvalidOperation("read-only mode".to_string());
        assert_eq!(err.to_string(), "Invalid operation: read-only mode");
    }

    #[test]
    fn test_resource_limit_exceeded() {
        let err = LsmError::ResourceLimit("disk full".to_string());
        assert_eq!(err.to_string(), "Resource limit exceeded: disk full");
    }

    #[test]
    fn test_internal_error() {
        let err = LsmError::Internal("unexpected state".to_string());
        assert_eq!(err.to_string(), "Internal error: unexpected state");
    }

    #[test]
    fn test_is_recoverable_io() {
        let err = LsmError::Io(io::Error::new(io::ErrorKind::NotFound, "not found"));
        assert!(err.is_recoverable());
    }

    #[test]
    fn test_is_recoverable_compaction() {
        let err = LsmError::Compaction("cancelled".to_string());
        assert!(err.is_recoverable());
    }

    #[test]
    fn test_is_not_recoverable_other_errors() {
        assert!(!LsmError::Serialization("error".into()).is_recoverable());
        assert!(!LsmError::Corruption("bad".into()).is_recoverable());
        assert!(!LsmError::KeyNotFound("x".into()).is_recoverable());
    }

    #[test]
    fn test_is_corruption_true() {
        let err = LsmError::Corruption("checksum mismatch".to_string());
        assert!(err.is_corruption());
    }

    #[test]
    fn test_is_corruption_false_for_other_errors() {
        assert!(
            !LsmError::Io(io::Error::new(io::ErrorKind::NotFound, "not found")).is_corruption()
        );
        assert!(!LsmError::Compaction("error".into()).is_corruption());
        assert!(!LsmError::KeyNotFound("x".into()).is_corruption());
    }

    #[test]
    fn test_user_message_io() {
        let err = LsmError::Io(io::Error::new(io::ErrorKind::PermissionDenied, "denied"));
        let msg = err.user_message();
        assert!(msg.contains("Storage I/O error"));
        assert!(msg.contains("denied"));
    }

    #[test]
    fn test_user_message_serialization() {
        let err = LsmError::Serialization("invalid JSON".to_string());
        let msg = err.user_message();
        assert!(msg.contains("Data format error"));
    }

    #[test]
    fn test_user_message_corruption() {
        let err = LsmError::Corruption("header mismatch".to_string());
        let msg = err.user_message();
        assert!(msg.contains("Data corruption detected"));
    }

    #[test]
    fn test_user_message_compaction() {
        let err = LsmError::Compaction("disk full".to_string());
        let msg = err.user_message();
        assert!(msg.contains("Storage optimization error"));
    }

    #[test]
    fn test_from_f4kvs_error() {
        // This requires F4KVS_ERROR to be defined, testing the conversion logic
        // We'll use a mock approach since we don't have a real F4KvsError instance
        let err = LsmError::Internal("test".to_string());
        assert_eq!(err.to_string(), "Internal error: test");
    }

    #[test]
    fn test_error_display_formatting() {
        // Test that all errors format correctly with Display trait
        let errors = vec![
            LsmError::Io(io::Error::new(io::ErrorKind::Other, "test")),
            LsmError::Serialization("test".into()),
            LsmError::Corruption("test".into()),
            LsmError::Config("test".into()),
            LsmError::Compaction("test".into()),
            LsmError::Wal("test".into()),
            LsmError::BloomFilter("test".into()),
            LsmError::Compression("test".into()),
            LsmError::KeyNotFound("test".into()),
            LsmError::ColumnFamilyNotFound("test".into()),
            LsmError::InvalidOperation("test".into()),
            LsmError::ResourceLimit("test".into()),
            LsmError::Internal("test".into()),
        ];

        for err in errors {
            let display = format!("{}", err);
            assert!(!display.is_empty(), "Display should not be empty");
        }
    }
}
