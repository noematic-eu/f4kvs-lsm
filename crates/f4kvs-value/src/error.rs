//! Error handling for F4KVS Core
//!
//! This module provides comprehensive error handling for F4KVS Core operations.
//! It includes detailed error types, severity levels, and recovery strategies.
//!
//! ## Error Handling Philosophy
//!
//! F4KVS Core uses a comprehensive error handling approach that provides:
//!
//! - **Detailed Error Information**: Rich error context with specific failure reasons
//! - **Error Severity Levels**: Categorized errors by impact and urgency
//! - **Recovery Strategies**: Suggested actions for common error scenarios
//! - **Error Chaining**: Preserve error context through error propagation
//! - **User-Friendly Messages**: Clear, actionable error messages
//!
//! ## Error Categories
//!
//! Errors are categorized by severity and type:
//!
//! - **Critical**: System cannot continue (memory allocation failures, corruption)
//! - **High**: Significant functionality affected (storage failures, I/O errors)
//! - **Medium**: Some functionality affected (validation errors, configuration issues)
//! - **Low**: Minor issues that don't affect core functionality (warnings, deprecations)
//!
//! ## Error Recovery Strategies
//!
//! Common recovery strategies for different error types:
//!
//! - **Memory Errors**: Retry with smaller allocations, enable memory pools
//! - **Storage Errors**: Check disk space, verify permissions, retry operations
//! - **Validation Errors**: Fix input data, update configuration
//! - **I/O Errors**: Check network connectivity, verify file paths
//! - **Concurrency Errors**: Implement backoff strategies, reduce contention
//!
//! ## Example Error Handling Patterns
//!
//! ### Basic Error Handling
//! ```rust
//! use f4kvs_value::{Result, F4KvsError};
//!
//! # fn some_operation() -> Result<()> {
//! #     // Simulate a storage error to exercise the match arms
//! #     Err(F4KvsError::Storage {
//! #         message: "disk unavailable".to_string(),
//! #     })
//! # }
//! fn handle_operation() -> Result<()> {
//!     match some_operation() {
//!         Ok(result) => Ok(result),
//!         Err(F4KvsError::Storage { message }) => {
//!             eprintln!("Storage error: {}", message);
//!             // Implement retry logic or fallback
//!             Err(F4KvsError::Storage { message })
//!         }
//!         Err(e) => {
//!             eprintln!("Unexpected error: {}", e);
//!             Err(e)
//!         }
//!     }
//! }
//! #
//! # fn main() {
//! #     let _ = handle_operation();
//! # }
//! ```
//!
//! ### Error Recovery with Retry
//! ```rust
//! use f4kvs_value::{Result, F4KvsError};
//! use std::time::Duration;
//! use tokio::time::sleep;
//!
//! async fn retry_operation<F, T>(mut operation: F, max_retries: usize) -> Result<T>
//! where
//!     F: FnMut() -> Result<T>,
//! {
//!     for attempt in 0..max_retries {
//!         match operation() {
//!             Ok(result) => return Ok(result),
//!             Err(F4KvsError::Io { .. }) if attempt < max_retries - 1 => {
//!                 let delay = Duration::from_millis(100 * (attempt + 1) as u64);
//!                 sleep(delay).await;
//!                 continue;
//!             }
//!             Err(e) => return Err(e),
//!         }
//!     }
//!     Err(F4KvsError::Io { message: "Max retries exceeded".to_string() })
//! }
//! ```
//!
//! ### Error Context and Chaining
//! ```rust
//! use f4kvs_value::{Result, F4KvsError};
//!
//! # fn validate_data(data: &[u8]) -> Result<()> {
//! #     if data.is_empty() {
//! #         Err(F4KvsError::InvalidValue {
//! #             reason: "empty payload".to_string(),
//! #         })
//! #     } else {
//! #         Ok(())
//! #     }
//! # }
//! #
//! # fn store_data(_data: &[u8]) -> Result<()> {
//! #     // Stub storage write
//! #     Ok(())
//! # }
//! fn process_data(data: &[u8]) -> Result<()> {
//!     validate_data(data).map_err(|e| F4KvsError::InvalidValue {
//!         reason: format!("Data validation failed: {}", e),
//!     })?;
//!
//!     store_data(data).map_err(|e| F4KvsError::Storage {
//!         message: format!("Failed to store validated data: {}", e),
//!     })?;
//!
//!     Ok(())
//! }
//! #
//! # fn main() -> Result<()> {
//! #     process_data(b"hello world")
//! # }
//! ```
#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use thiserror::Error;

/// F4KVS Core result type
pub type Result<T> = std::result::Result<T, F4KvsError>;

/// Error severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ErrorSeverity {
    /// Low severity - minor issues that don't affect functionality
    Low,
    /// Medium severity - issues that may affect some functionality
    Medium,
    /// High severity - issues that significantly impact functionality
    High,
    /// Critical severity - issues that make the system unusable
    Critical,
}

/// F4KVS Core error types
#[derive(Error, Debug, Clone, PartialEq)]
pub enum F4KvsError {
    /// Key validation errors
    #[error("Invalid key: {reason}")]
    InvalidKey {
        /// Reason why the key is invalid
        reason: String,
    },

    /// Value validation errors
    #[error("Invalid value: {reason}")]
    InvalidValue {
        /// Reason why the value is invalid
        reason: String,
    },

    /// Storage backend errors
    #[error("Storage error: {message}")]
    Storage {
        /// Error message describing the storage issue
        message: String,
    },

    /// I/O errors
    #[error("I/O error: {message}")]
    Io {
        /// Error message describing the I/O issue
        message: String,
    },

    /// Serialization errors
    #[error("Serialization error: {message}")]
    Serialization {
        /// Error message describing the serialization issue
        message: String,
    },

    /// Operation timeout
    #[error("Operation '{operation}' timed out after {timeout_ms}ms")]
    Timeout {
        /// The operation that timed out
        operation: String,
        /// Timeout duration in milliseconds
        timeout_ms: u64,
    },

    /// Key not found (not an error, but used internally)
    #[error("Key not found: {key}")]
    KeyNotFound {
        /// The key that was not found
        key: String,
    },

    /// Configuration errors
    #[error("Configuration error: {message}")]
    Config {
        /// Error message describing the configuration issue
        message: String,
    },

    /// Internal errors (should not happen in production)
    #[error("Internal error: {message}")]
    Internal {
        /// Error message describing the internal error
        message: String,
    },

    /// Encryption errors
    #[error("Encryption error: {message}")]
    Encryption {
        /// Error message describing the encryption issue
        message: String,
    },

    /// Invalid configuration errors
    #[error("Invalid configuration: {message}")]
    InvalidConfiguration {
        /// Error message describing the configuration issue
        message: String,
    },
}

impl F4KvsError {
    /// Create an invalid key error with detailed context
    pub fn invalid_key(reason: impl Into<String>) -> Self {
        F4KvsError::InvalidKey {
            reason: reason.into(),
        }
    }

    /// Create an invalid key error with key context
    pub fn invalid_key_with_context(key: &str, reason: impl Into<String>) -> Self {
        F4KvsError::InvalidKey {
            reason: format!("Key '{}': {}", key, reason.into()),
        }
    }

    /// Create an invalid value error with detailed context
    pub fn invalid_value(reason: impl Into<String>) -> Self {
        F4KvsError::InvalidValue {
            reason: reason.into(),
        }
    }

    /// Create an invalid value error with value context
    pub fn invalid_value_with_context(value_type: &str, reason: impl Into<String>) -> Self {
        F4KvsError::InvalidValue {
            reason: format!("Value type '{}': {}", value_type, reason.into()),
        }
    }

    /// Create a storage error with detailed context
    pub fn storage(message: impl Into<String>) -> Self {
        F4KvsError::Storage {
            message: message.into(),
        }
    }

    /// Create a storage error with operation context
    pub fn storage_with_operation(operation: &str, message: impl Into<String>) -> Self {
        F4KvsError::Storage {
            message: format!(
                "Storage operation '{}' failed: {}",
                operation,
                message.into()
            ),
        }
    }

    /// Create an I/O error with detailed context
    pub fn io(message: impl Into<String>) -> Self {
        F4KvsError::Io {
            message: message.into(),
        }
    }

    /// Create an I/O error with file context
    pub fn io_with_path(path: &str, message: impl Into<String>) -> Self {
        F4KvsError::Io {
            message: format!("I/O error for path '{}': {}", path, message.into()),
        }
    }

    /// Create a serialization error with detailed context
    pub fn serialization(message: impl Into<String>) -> Self {
        F4KvsError::Serialization {
            message: message.into(),
        }
    }

    /// Create a serialization error with format context
    pub fn serialization_with_format(format: &str, message: impl Into<String>) -> Self {
        F4KvsError::Serialization {
            message: format!(
                "Serialization error for format '{}': {}",
                format,
                message.into()
            ),
        }
    }

    /// Create a configuration error with detailed context
    pub fn config(message: impl Into<String>) -> Self {
        F4KvsError::Config {
            message: message.into(),
        }
    }

    /// Create a configuration error with field context
    pub fn config_with_field(field: &str, message: impl Into<String>) -> Self {
        F4KvsError::Config {
            message: format!(
                "Configuration error for field '{}': {}",
                field,
                message.into()
            ),
        }
    }

    /// Create an internal error with detailed context
    pub fn internal(message: impl Into<String>) -> Self {
        F4KvsError::Internal {
            message: message.into(),
        }
    }

    /// Create an internal error with component context
    pub fn internal_with_component(component: &str, message: impl Into<String>) -> Self {
        F4KvsError::Internal {
            message: format!(
                "Internal error in component '{}': {}",
                component,
                message.into()
            ),
        }
    }

    /// Create an encryption error with detailed context
    pub fn encryption(message: impl Into<String>) -> Self {
        F4KvsError::Encryption {
            message: message.into(),
        }
    }

    /// Create an encryption error with algorithm context
    pub fn encryption_with_algorithm(algorithm: &str, message: impl Into<String>) -> Self {
        F4KvsError::Encryption {
            message: format!(
                "Encryption error for algorithm '{}': {}",
                algorithm,
                message.into()
            ),
        }
    }

    /// Create an invalid configuration error with detailed context
    pub fn invalid_configuration(message: impl Into<String>) -> Self {
        F4KvsError::InvalidConfiguration {
            message: message.into(),
        }
    }

    /// Create a timeout error with operation context
    pub fn timeout_with_operation(operation: &str, timeout_ms: u64) -> Self {
        F4KvsError::Timeout {
            operation: operation.to_string(),
            timeout_ms,
        }
    }

    /// Create a simple timeout error (backward compatibility)
    pub fn timeout() -> Self {
        F4KvsError::Timeout {
            operation: "unknown".to_string(),
            timeout_ms: 0,
        }
    }

    /// Get error severity level
    pub fn severity(&self) -> ErrorSeverity {
        match self {
            F4KvsError::InvalidKey { .. } => ErrorSeverity::Low,
            F4KvsError::InvalidValue { .. } => ErrorSeverity::Low,
            F4KvsError::KeyNotFound { .. } => ErrorSeverity::Low,
            F4KvsError::Config { .. } => ErrorSeverity::Medium,
            F4KvsError::InvalidConfiguration { .. } => ErrorSeverity::Medium,
            F4KvsError::Serialization { .. } => ErrorSeverity::Medium,
            F4KvsError::Encryption { .. } => ErrorSeverity::High,
            F4KvsError::Storage { .. } => ErrorSeverity::High,
            F4KvsError::Io { .. } => ErrorSeverity::High,
            F4KvsError::Timeout { .. } => ErrorSeverity::High,
            F4KvsError::Internal { .. } => ErrorSeverity::Critical,
        }
    }

    /// Get suggested recovery action
    pub fn recovery_suggestion(&self) -> Option<&'static str> {
        match self {
            F4KvsError::InvalidKey { .. } => Some("Check key format and length requirements"),
            F4KvsError::InvalidValue { .. } => Some("Verify value type and content"),
            F4KvsError::KeyNotFound { .. } => Some("Key may not exist or may have been deleted"),
            F4KvsError::Config { .. } => Some("Check configuration file and environment variables"),
            F4KvsError::InvalidConfiguration { .. } => {
                Some("Validate configuration values and format")
            }
            F4KvsError::Serialization { .. } => Some("Check data format and encoding"),
            F4KvsError::Encryption { .. } => Some("Verify encryption keys and algorithms"),
            F4KvsError::Storage { .. } => Some("Check storage backend and disk space"),
            F4KvsError::Io { .. } => Some("Check file permissions and disk space"),
            F4KvsError::Timeout { .. } => Some("Retry operation or increase timeout"),
            F4KvsError::Internal { .. } => Some("Contact support - this should not happen"),
        }
    }

    /// Check if this error is recoverable
    pub fn is_recoverable(&self) -> bool {
        match self {
            F4KvsError::Storage { .. } => true,
            F4KvsError::Io { .. } => true,
            F4KvsError::Timeout { .. } => true,
            F4KvsError::Serialization { .. } => false,
            F4KvsError::InvalidKey { .. } => false,
            F4KvsError::InvalidValue { .. } => false,
            F4KvsError::KeyNotFound { .. } => false,
            F4KvsError::Config { .. } => false,
            F4KvsError::Internal { .. } => false,
            F4KvsError::Encryption { .. } => false,
            F4KvsError::InvalidConfiguration { .. } => false,
        }
    }
}

// Convert from std::io::Error
impl From<std::io::Error> for F4KvsError {
    fn from(err: std::io::Error) -> Self {
        F4KvsError::io(err.to_string())
    }
}

// Convert from serde_json::Error
impl From<serde_json::Error> for F4KvsError {
    fn from(err: serde_json::Error) -> Self {
        F4KvsError::serialization(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        // Test all error creation methods
        let invalid_key = F4KvsError::invalid_key("too long");
        assert!(matches!(invalid_key, F4KvsError::InvalidKey { reason } if reason == "too long"));

        let invalid_value = F4KvsError::invalid_value("invalid type");
        assert!(
            matches!(invalid_value, F4KvsError::InvalidValue { reason } if reason == "invalid type")
        );

        let storage = F4KvsError::storage("disk full");
        assert!(matches!(storage, F4KvsError::Storage { message } if message == "disk full"));

        let io = F4KvsError::io("file not found");
        assert!(matches!(io, F4KvsError::Io { message } if message == "file not found"));

        let serialization = F4KvsError::serialization("invalid json");
        assert!(
            matches!(serialization, F4KvsError::Serialization { message } if message == "invalid json")
        );

        let config = F4KvsError::config("missing field");
        assert!(matches!(config, F4KvsError::Config { message } if message == "missing field"));

        let internal = F4KvsError::internal("unexpected state");
        assert!(
            matches!(internal, F4KvsError::Internal { message } if message == "unexpected state")
        );

        let encryption = F4KvsError::encryption("key not found");
        assert!(
            matches!(encryption, F4KvsError::Encryption { message } if message == "key not found")
        );

        let invalid_config = F4KvsError::invalid_configuration("invalid value");
        assert!(
            matches!(invalid_config, F4KvsError::InvalidConfiguration { message } if message == "invalid value")
        );
    }

    #[test]
    fn test_error_variants() {
        // Test all error variants
        let timeout = F4KvsError::timeout();
        assert!(matches!(timeout, F4KvsError::Timeout { .. }));

        let key_not_found = F4KvsError::KeyNotFound {
            key: "test".to_string(),
        };
        assert!(matches!(key_not_found, F4KvsError::KeyNotFound { key } if key == "test"));
    }

    #[test]
    fn test_error_display() {
        // Test error display formatting
        let invalid_key = F4KvsError::invalid_key("too long");
        assert_eq!(format!("{}", invalid_key), "Invalid key: too long");

        let invalid_value = F4KvsError::invalid_value("invalid type");
        assert_eq!(format!("{}", invalid_value), "Invalid value: invalid type");

        let storage = F4KvsError::storage("disk full");
        assert_eq!(format!("{}", storage), "Storage error: disk full");

        let io = F4KvsError::io("file not found");
        assert_eq!(format!("{}", io), "I/O error: file not found");

        let serialization = F4KvsError::serialization("invalid json");
        assert_eq!(
            format!("{}", serialization),
            "Serialization error: invalid json"
        );

        let timeout = F4KvsError::timeout_with_operation("get", 5000);
        assert_eq!(
            format!("{}", timeout),
            "Operation 'get' timed out after 5000ms"
        );

        let key_not_found = F4KvsError::KeyNotFound {
            key: "test".to_string(),
        };
        assert_eq!(format!("{}", key_not_found), "Key not found: test");

        let config = F4KvsError::config("missing field");
        assert_eq!(format!("{}", config), "Configuration error: missing field");

        let internal = F4KvsError::internal("unexpected state");
        assert_eq!(format!("{}", internal), "Internal error: unexpected state");

        let encryption = F4KvsError::encryption("key not found");
        assert_eq!(format!("{}", encryption), "Encryption error: key not found");

        let invalid_config = F4KvsError::invalid_configuration("invalid value");
        assert_eq!(
            format!("{}", invalid_config),
            "Invalid configuration: invalid value"
        );
    }

    #[test]
    fn test_error_from_std_io_error() {
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let f4kvs_error: F4KvsError = io_error.into();

        assert!(
            matches!(f4kvs_error, F4KvsError::Io { message } if message.contains("file not found"))
        );
    }

    #[test]
    fn test_error_from_serde_json_error() {
        let json_error = serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err();
        let f4kvs_error: F4KvsError = json_error.into();

        assert!(
            matches!(f4kvs_error, F4KvsError::Serialization { message } if !message.is_empty())
        );
    }

    #[test]
    fn test_error_clone_and_partial_eq() {
        let error1 = F4KvsError::invalid_key("test");
        let error2 = error1.clone();
        let error3 = F4KvsError::invalid_key("test");
        let error4 = F4KvsError::invalid_key("different");

        assert_eq!(error1, error2);
        assert_eq!(error1, error3);
        assert_ne!(error1, error4);
    }

    #[test]
    fn test_error_debug() {
        let error = F4KvsError::invalid_key("test");
        let debug_str = format!("{:?}", error);
        assert!(debug_str.contains("InvalidKey"));
        assert!(debug_str.contains("test"));
    }

    #[test]
    fn test_result_type_alias() {
        // Test that the Result type alias works correctly
        fn success_function() -> Result<String> {
            Ok("success".to_string())
        }

        fn error_function() -> Result<String> {
            Err(F4KvsError::invalid_key("test"))
        }

        assert!(success_function().is_ok());
        assert!(error_function().is_err());
    }

    #[test]
    fn test_enhanced_error_creation() {
        // Test enhanced error creation methods
        let invalid_key = F4KvsError::invalid_key_with_context("test-key", "too long");
        assert!(
            matches!(invalid_key, F4KvsError::InvalidKey { reason } if reason.contains("test-key"))
        );

        let invalid_value = F4KvsError::invalid_value_with_context("String", "empty value");
        assert!(
            matches!(invalid_value, F4KvsError::InvalidValue { reason } if reason.contains("String"))
        );

        let storage = F4KvsError::storage_with_operation("put", "disk full");
        assert!(matches!(storage, F4KvsError::Storage { message } if message.contains("put")));

        let io = F4KvsError::io_with_path("/path/to/file", "permission denied");
        assert!(matches!(io, F4KvsError::Io { message } if message.contains("/path/to/file")));

        let serialization = F4KvsError::serialization_with_format("JSON", "invalid syntax");
        assert!(
            matches!(serialization, F4KvsError::Serialization { message } if message.contains("JSON"))
        );

        let config = F4KvsError::config_with_field("port", "invalid value");
        assert!(matches!(config, F4KvsError::Config { message } if message.contains("port")));

        let internal = F4KvsError::internal_with_component("storage", "unexpected state");
        assert!(
            matches!(internal, F4KvsError::Internal { message } if message.contains("storage"))
        );

        let encryption = F4KvsError::encryption_with_algorithm("AES-256", "key not found");
        assert!(
            matches!(encryption, F4KvsError::Encryption { message } if message.contains("AES-256"))
        );
    }

    #[test]
    fn test_error_severity() {
        // Test error severity levels
        let low_severity = F4KvsError::invalid_key("test");
        assert_eq!(low_severity.severity(), ErrorSeverity::Low);

        let medium_severity = F4KvsError::config("test");
        assert_eq!(medium_severity.severity(), ErrorSeverity::Medium);

        let high_severity = F4KvsError::storage("test");
        assert_eq!(high_severity.severity(), ErrorSeverity::High);

        let critical_severity = F4KvsError::internal("test");
        assert_eq!(critical_severity.severity(), ErrorSeverity::Critical);
    }

    #[test]
    fn test_recovery_suggestions() {
        // Test recovery suggestions
        let invalid_key = F4KvsError::invalid_key("test");
        assert!(invalid_key.recovery_suggestion().is_some());
        assert!(invalid_key
            .recovery_suggestion()
            .unwrap()
            .contains("key format"));

        let storage = F4KvsError::storage("test");
        assert!(storage.recovery_suggestion().is_some());
        assert!(storage
            .recovery_suggestion()
            .unwrap()
            .contains("storage backend"));

        let internal = F4KvsError::internal("test");
        assert!(internal.recovery_suggestion().is_some());
        assert!(internal
            .recovery_suggestion()
            .unwrap()
            .contains("Contact support"));
    }

    #[test]
    fn test_error_severity_ordering() {
        // Test that severity levels can be compared
        assert!(ErrorSeverity::Low < ErrorSeverity::Medium);
        assert!(ErrorSeverity::Medium < ErrorSeverity::High);
        assert!(ErrorSeverity::High < ErrorSeverity::Critical);
        assert_eq!(ErrorSeverity::Low, ErrorSeverity::Low);
    }
}
