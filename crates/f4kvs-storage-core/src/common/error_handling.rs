//! Centralized error handling utilities for F4KVS storage

use crate::{F4KvsError, Result};

/// Error context trait for adding context to errors
pub trait ErrorContext {
    /// Add context to an error
    fn with_context(self, context: &str) -> Self;

    /// Add context with a closure for lazy evaluation
    fn with_context_lazy<F>(self, context_fn: F) -> Self
    where
        F: FnOnce() -> String;
}

impl<T> ErrorContext for Result<T> {
    fn with_context(self, context: &str) -> Self {
        self.map_err(|e| match e {
            F4KvsError::Io { message } => F4KvsError::Io {
                message: format!("{}: {}", context, message),
            },
            F4KvsError::Storage { message } => F4KvsError::Storage {
                message: format!("{}: {}", context, message),
            },
            F4KvsError::Serialization { message } => F4KvsError::Serialization {
                message: format!("{}: {}", context, message),
            },
            F4KvsError::Config { message } => F4KvsError::Config {
                message: format!("{}: {}", context, message),
            },
            F4KvsError::Internal { message } => F4KvsError::Internal {
                message: format!("{}: {}", context, message),
            },
            F4KvsError::Encryption { message } => F4KvsError::Encryption {
                message: format!("{}: {}", context, message),
            },
            F4KvsError::InvalidConfiguration { message } => F4KvsError::InvalidConfiguration {
                message: format!("{}: {}", context, message),
            },
            other => F4KvsError::Internal {
                message: format!("{}: {}", context, other),
            },
        })
    }

    fn with_context_lazy<F>(self, context_fn: F) -> Self
    where
        F: FnOnce() -> String,
    {
        self.map_err(|e| {
            let context = context_fn();
            match e {
                F4KvsError::Io { message } => F4KvsError::Io {
                    message: format!("{}: {}", context, message),
                },
                F4KvsError::Storage { message } => F4KvsError::Storage {
                    message: format!("{}: {}", context, message),
                },
                F4KvsError::Serialization { message } => F4KvsError::Serialization {
                    message: format!("{}: {}", context, message),
                },
                F4KvsError::Config { message } => F4KvsError::Config {
                    message: format!("{}: {}", context, message),
                },
                F4KvsError::Internal { message } => F4KvsError::Internal {
                    message: format!("{}: {}", context, message),
                },
                F4KvsError::Encryption { message } => F4KvsError::Encryption {
                    message: format!("{}: {}", context, message),
                },
                F4KvsError::InvalidConfiguration { message } => F4KvsError::InvalidConfiguration {
                    message: format!("{}: {}", context, message),
                },
                other => F4KvsError::Internal {
                    message: format!("{}: {}", context, other),
                },
            }
        })
    }
}

/// Error recovery strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryStrategy {
    /// Retry the operation
    Retry,
    /// Skip the operation
    Skip,
    /// Fail the operation
    Fail,
    /// Use a fallback approach
    Fallback,
}

/// Error severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ErrorSeverity {
    /// Low severity - operation can continue
    Low,
    /// Medium severity - operation may be affected
    Medium,
    /// High severity - operation should be retried
    High,
    /// Critical severity - operation must fail
    Critical,
}

/// Error classification
#[derive(Debug, Clone)]
pub struct ErrorClassification {
    /// The error severity
    pub severity: ErrorSeverity,
    /// Whether the error is recoverable
    pub recoverable: bool,
    /// Suggested recovery strategy
    pub recovery_strategy: RecoveryStrategy,
    /// Whether the error should be logged
    pub should_log: bool,
    /// Whether the error should be reported to monitoring
    pub should_report: bool,
}

impl ErrorClassification {
    /// Classify an F4KVS error
    pub fn classify(error: &F4KvsError) -> Self {
        match error {
            F4KvsError::InvalidKey { .. } => Self {
                severity: ErrorSeverity::Medium,
                recoverable: false,
                recovery_strategy: RecoveryStrategy::Fail,
                should_log: true,
                should_report: false,
            },
            F4KvsError::InvalidValue { .. } => Self {
                severity: ErrorSeverity::Medium,
                recoverable: false,
                recovery_strategy: RecoveryStrategy::Fail,
                should_log: true,
                should_report: false,
            },
            F4KvsError::Storage { .. } => Self {
                severity: ErrorSeverity::High,
                recoverable: true,
                recovery_strategy: RecoveryStrategy::Retry,
                should_log: true,
                should_report: true,
            },
            F4KvsError::Io { .. } => Self {
                severity: ErrorSeverity::High,
                recoverable: true,
                recovery_strategy: RecoveryStrategy::Retry,
                should_log: true,
                should_report: true,
            },
            F4KvsError::Serialization { .. } => Self {
                severity: ErrorSeverity::Critical,
                recoverable: false,
                recovery_strategy: RecoveryStrategy::Fail,
                should_log: true,
                should_report: true,
            },
            F4KvsError::Timeout { .. } => Self {
                severity: ErrorSeverity::High,
                recoverable: true,
                recovery_strategy: RecoveryStrategy::Retry,
                should_log: true,
                should_report: true,
            },
            F4KvsError::KeyNotFound { .. } => Self {
                severity: ErrorSeverity::Low,
                recoverable: false,
                recovery_strategy: RecoveryStrategy::Skip,
                should_log: false,
                should_report: false,
            },
            F4KvsError::Config { .. } => Self {
                severity: ErrorSeverity::Critical,
                recoverable: false,
                recovery_strategy: RecoveryStrategy::Fail,
                should_log: true,
                should_report: true,
            },
            F4KvsError::Internal { .. } => Self {
                severity: ErrorSeverity::Critical,
                recoverable: false,
                recovery_strategy: RecoveryStrategy::Fail,
                should_log: true,
                should_report: true,
            },
            F4KvsError::Encryption { .. } => Self {
                severity: ErrorSeverity::Critical,
                recoverable: false,
                recovery_strategy: RecoveryStrategy::Fail,
                should_log: true,
                should_report: true,
            },
            F4KvsError::InvalidConfiguration { .. } => Self {
                severity: ErrorSeverity::Critical,
                recoverable: false,
                recovery_strategy: RecoveryStrategy::Fail,
                should_log: true,
                should_report: true,
            },
        }
    }
}

/// Error handler for consistent error processing
pub struct ErrorHandler {
    /// Maximum retry attempts
    pub max_retries: u32,
    /// Retry delay in milliseconds
    pub retry_delay_ms: u64,
    /// Whether to log errors
    pub enable_logging: bool,
    /// Whether to report errors to monitoring
    pub enable_reporting: bool,
}

impl Default for ErrorHandler {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_delay_ms: 100,
            enable_logging: true,
            enable_reporting: true,
        }
    }
}

impl ErrorHandler {
    /// Create a new error handler
    pub fn new(max_retries: u32, retry_delay_ms: u64) -> Self {
        Self {
            max_retries,
            retry_delay_ms,
            enable_logging: true,
            enable_reporting: true,
        }
    }

    /// Handle an error with appropriate logging and recovery
    pub async fn handle_error<T, F, Fut>(&self, error: F4KvsError, operation: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let classification = ErrorClassification::classify(&error);

        // Log error if needed
        if classification.should_log && self.enable_logging {
            tracing::error!("Operation failed: {}", error);
        }

        // Report error if needed
        if classification.should_report && self.enable_reporting {
            // In a real implementation, this would send to monitoring system
            tracing::warn!("Error reported to monitoring: {}", error);
        }

        // Apply recovery strategy
        match classification.recovery_strategy {
            RecoveryStrategy::Retry if classification.recoverable => {
                self.retry_operation(operation).await
            }
            RecoveryStrategy::Skip => Err(F4KvsError::internal("Operation skipped due to error")),
            RecoveryStrategy::Fallback => Err(F4KvsError::internal("Fallback not implemented")),
            _ => Err(error),
        }
    }

    /// Retry an operation with exponential backoff
    async fn retry_operation<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut last_error = None;

        for attempt in 1..=self.max_retries {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(error) => {
                    last_error = Some(error);

                    if attempt < self.max_retries {
                        let delay = self.retry_delay_ms * 2_u64.pow(attempt - 1);
                        tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| F4KvsError::internal("Max retries exceeded")))
    }
}

/// Error metrics for monitoring
#[derive(Debug, Default)]
pub struct ErrorMetrics {
    /// Total error count
    pub total_errors: u64,
    /// Error count by severity
    pub errors_by_severity: std::collections::HashMap<ErrorSeverity, u64>,
    /// Error count by type
    pub errors_by_type: std::collections::HashMap<String, u64>,
    /// Recovery success rate
    pub recovery_success_rate: f64,
    /// Total recovery attempts
    pub recovery_attempts: u64,
    /// Successful recoveries
    pub successful_recoveries: u64,
}

impl ErrorMetrics {
    /// Record an error
    pub fn record_error(&mut self, error: &F4KvsError) {
        self.total_errors += 1;

        let classification = ErrorClassification::classify(error);
        *self
            .errors_by_severity
            .entry(classification.severity)
            .or_insert(0) += 1;

        let error_type = format!("{:?}", error);
        *self.errors_by_type.entry(error_type).or_insert(0) += 1;
    }

    /// Record a recovery attempt
    pub fn record_recovery_attempt(&mut self) {
        self.recovery_attempts += 1;
    }

    /// Record a successful recovery
    pub fn record_successful_recovery(&mut self) {
        self.successful_recoveries += 1;
        self.recovery_success_rate =
            self.successful_recoveries as f64 / self.recovery_attempts as f64;
    }

    /// Reset metrics
    pub fn reset(&mut self) {
        self.total_errors = 0;
        self.errors_by_severity.clear();
        self.errors_by_type.clear();
        self.recovery_success_rate = 0.0;
        self.recovery_attempts = 0;
        self.successful_recoveries = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_context() {
        let result: Result<()> = Err(F4KvsError::io("Test error"));
        let result_with_context = result.with_context("Operation failed");

        match result_with_context {
            Err(F4KvsError::Io { message }) => {
                assert!(message.contains("Operation failed"));
                assert!(message.contains("Test error"));
            }
            _ => panic!("Expected Io error"),
        }
    }

    #[test]
    fn test_error_classification() {
        let io_error = F4KvsError::io("Test IO error");
        let classification = ErrorClassification::classify(&io_error);

        assert_eq!(classification.severity, ErrorSeverity::High);
        assert!(classification.recoverable);
        assert_eq!(classification.recovery_strategy, RecoveryStrategy::Retry);
        assert!(classification.should_log);
        assert!(classification.should_report);
    }

    #[test]
    fn test_error_metrics() {
        let mut metrics = ErrorMetrics::default();

        metrics.record_error(&F4KvsError::io("Test error"));
        metrics.record_recovery_attempt();
        metrics.record_successful_recovery();

        assert_eq!(metrics.total_errors, 1);
        assert_eq!(metrics.recovery_attempts, 1);
        assert_eq!(metrics.successful_recoveries, 1);
        assert_eq!(metrics.recovery_success_rate, 1.0);
    }
}
