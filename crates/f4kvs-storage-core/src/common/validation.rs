//! Centralized validation utilities for F4KVS storage

use crate::{F4KvsError, Result, Value};

/// Key validation configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KeyValidationConfig {
    /// Maximum key length
    pub max_length: usize,
    /// Minimum key length
    pub min_length: usize,
    /// Whether to allow empty keys
    pub allow_empty: bool,
    /// Whether to enforce ASCII-only keys
    pub ascii_only: bool,
    /// Whether to allow control characters
    pub allow_control_chars: bool,
    /// Custom validation patterns
    pub patterns: Vec<String>,
}

impl Default for KeyValidationConfig {
    fn default() -> Self {
        Self {
            max_length: 1024,
            min_length: 1,
            allow_empty: false,
            ascii_only: false,
            allow_control_chars: false,
            patterns: Vec::new(),
        }
    }
}

/// Value validation configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ValueValidationConfig {
    /// Maximum value size in bytes
    pub max_size: usize,
    /// Whether to allow null values
    pub allow_null: bool,
    /// Whether to allow empty strings
    pub allow_empty_strings: bool,
    /// Maximum string length
    pub max_string_length: usize,
    /// Maximum JSON depth
    pub max_json_depth: usize,
}

impl Default for ValueValidationConfig {
    fn default() -> Self {
        Self {
            max_size: 1024 * 1024, // 1MB
            allow_null: true,
            allow_empty_strings: true,
            max_string_length: 1024 * 1024, // 1MB
            max_json_depth: 32,
        }
    }
}

/// Validation configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ValidationConfig {
    /// Enable validation (default: true)
    pub enabled: bool,
    /// Key validation configuration
    pub key: KeyValidationConfig,
    /// Value validation configuration
    pub value: ValueValidationConfig,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            key: KeyValidationConfig::default(),
            value: ValueValidationConfig::default(),
        }
    }
}

/// Validation manager for keys and values
#[derive(Debug)]
pub struct ValidationManager {
    config: ValidationConfig,
}

impl ValidationManager {
    /// Create a new validation manager
    pub fn new(key_config: KeyValidationConfig, value_config: ValueValidationConfig) -> Self {
        Self {
            config: ValidationConfig {
                enabled: true,
                key: key_config,
                value: value_config,
            },
        }
    }

    /// Create a new validation manager from config
    pub fn from_config(config: ValidationConfig) -> Self {
        Self { config }
    }

    /// Check if validation is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Validate a key
    pub fn validate_key(&self, key: &str) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        // Check length
        if key.is_empty() && !self.config.key.allow_empty {
            return Err(F4KvsError::invalid_key("key cannot be empty"));
        }

        if key.len() < self.config.key.min_length {
            return Err(F4KvsError::invalid_key(format!(
                "key length {} is less than minimum {}",
                key.len(),
                self.config.key.min_length
            )));
        }

        if key.len() > self.config.key.max_length {
            return Err(F4KvsError::invalid_key(format!(
                "key length {} exceeds maximum {}",
                key.len(),
                self.config.key.max_length
            )));
        }

        // Check ASCII-only requirement
        if self.config.key.ascii_only && !key.is_ascii() {
            return Err(F4KvsError::invalid_key("key must be ASCII"));
        }

        // Check control characters
        if !self.config.key.allow_control_chars && key.chars().any(|c| c.is_control()) {
            return Err(F4KvsError::invalid_key("key contains control characters"));
        }

        // Check custom patterns
        for pattern in &self.config.key.patterns {
            if !self.matches_pattern(key, pattern) {
                return Err(F4KvsError::invalid_key(format!(
                    "key does not match required pattern: {}",
                    pattern
                )));
            }
        }

        Ok(())
    }

    /// Validate a value
    pub fn validate_value(&self, value: &Value) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        // Check null values
        if matches!(value, Value::Null) && !self.config.value.allow_null {
            return Err(F4KvsError::invalid_value("null values are not allowed"));
        }

        // Check value size
        let size = value.serialized_size();
        if size > self.config.value.max_size {
            return Err(F4KvsError::invalid_value(format!(
                "value size {} exceeds maximum {}",
                size, self.config.value.max_size
            )));
        }

        // Check string-specific validations
        if let Value::String(s) = value {
            if s.is_empty() && !self.config.value.allow_empty_strings {
                return Err(F4KvsError::invalid_value("empty strings are not allowed"));
            }

            if s.len() > self.config.value.max_string_length {
                return Err(F4KvsError::invalid_value(format!(
                    "string length {} exceeds maximum {}",
                    s.len(),
                    self.config.value.max_string_length
                )));
            }
        }

        // Check JSON-specific validations
        if let Value::Json(json) = value {
            if let Err(e) = self.validate_json_depth(json) {
                return Err(F4KvsError::invalid_value(format!(
                    "JSON depth validation failed: {}",
                    e
                )));
            }
        }

        Ok(())
    }

    /// Validate both key and value
    pub fn validate_key_value(&self, key: &str, value: &Value) -> Result<()> {
        self.validate_key(key)?;
        self.validate_value(value)?;
        Ok(())
    }

    /// Check if a key matches a pattern
    fn matches_pattern(&self, key: &str, pattern: &str) -> bool {
        // Simple pattern matching - in a real implementation, this would use regex
        if pattern.starts_with('*') && pattern.ends_with('*') {
            let inner = &pattern[1..pattern.len() - 1];
            key.contains(inner)
        } else if let Some(suffix) = pattern.strip_prefix('*') {
            key.ends_with(suffix)
        } else if let Some(prefix) = pattern.strip_suffix('*') {
            key.starts_with(prefix)
        } else {
            key == pattern
        }
    }

    /// Validate JSON depth
    fn validate_json_depth(&self, json: &serde_json::Value) -> Result<()> {
        let depth = self.calculate_json_depth(json);
        if depth > self.config.value.max_json_depth {
            return Err(F4KvsError::invalid_value(format!(
                "JSON depth {} exceeds maximum {}",
                depth, self.config.value.max_json_depth
            )));
        }
        Ok(())
    }

    /// Calculate JSON depth
    #[allow(clippy::only_used_in_recursion)]
    fn calculate_json_depth(&self, json: &serde_json::Value) -> usize {
        match json {
            serde_json::Value::Object(map) => {
                if map.is_empty() {
                    1
                } else {
                    1 + map
                        .values()
                        .map(|v| self.calculate_json_depth(v))
                        .max()
                        .unwrap_or(0)
                }
            }
            serde_json::Value::Array(arr) => {
                if arr.is_empty() {
                    1
                } else {
                    1 + arr
                        .iter()
                        .map(|v| self.calculate_json_depth(v))
                        .max()
                        .unwrap_or(0)
                }
            }
            _ => 1,
        }
    }
}

/// Data integrity validator
pub struct IntegrityValidator;

impl IntegrityValidator {
    /// Validate data integrity using checksum
    pub fn validate_checksum(data: &[u8], expected_checksum: u32) -> Result<()> {
        let actual_checksum = Self::calculate_checksum(data);
        if actual_checksum != expected_checksum {
            return Err(F4KvsError::storage(format!(
                "Checksum mismatch: expected {}, got {}",
                expected_checksum, actual_checksum
            )));
        }
        Ok(())
    }

    /// Calculate simple checksum
    pub fn calculate_checksum(data: &[u8]) -> u32 {
        let mut checksum: u32 = 0;
        for &byte in data {
            checksum = checksum.wrapping_add(byte as u32);
        }
        checksum
    }

    /// Validate data size
    pub fn validate_size(data: &[u8], expected_size: usize) -> Result<()> {
        if data.len() != expected_size {
            return Err(F4KvsError::storage(format!(
                "Size mismatch: expected {}, got {}",
                expected_size,
                data.len()
            )));
        }
        Ok(())
    }

    /// Validate data is not empty
    pub fn validate_not_empty(data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Err(F4KvsError::storage("Data cannot be empty"));
        }
        Ok(())
    }
}

/// Configuration validator
pub struct ConfigValidator;

impl ConfigValidator {
    /// Validate storage configuration
    pub fn validate_storage_config<T>(config: &T) -> Result<()>
    where
        T: StorageConfigValidator,
    {
        config.validate()
    }
}

/// Trait for storage configuration validation
pub trait StorageConfigValidator {
    /// Validate the configuration
    fn validate(&self) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_validation() {
        let config = KeyValidationConfig::default();
        let manager = ValidationManager::new(config, ValueValidationConfig::default());

        // Valid key
        assert!(manager.validate_key("valid_key").is_ok());

        // Empty key
        assert!(manager.validate_key("").is_err());

        // Too long key
        let long_key = "a".repeat(2000);
        assert!(manager.validate_key(&long_key).is_err());
    }

    #[test]
    fn test_value_validation() {
        let config = ValueValidationConfig::default();
        let manager = ValidationManager::new(KeyValidationConfig::default(), config);

        // Valid value
        assert!(manager
            .validate_value(&Value::String("test".to_string()))
            .is_ok());

        // Null value (allowed by default)
        assert!(manager.validate_value(&Value::Null).is_ok());

        // Large value
        let large_string = "a".repeat(2 * 1024 * 1024); // 2MB
        assert!(manager
            .validate_value(&Value::String(large_string))
            .is_err());
    }

    #[test]
    fn test_integrity_validator() {
        let data = b"test data";
        let checksum = IntegrityValidator::calculate_checksum(data);

        // Valid checksum
        assert!(IntegrityValidator::validate_checksum(data, checksum).is_ok());

        // Invalid checksum
        assert!(IntegrityValidator::validate_checksum(data, checksum + 1).is_err());

        // Size validation
        assert!(IntegrityValidator::validate_size(data, data.len()).is_ok());
        assert!(IntegrityValidator::validate_size(data, data.len() + 1).is_err());

        // Empty data validation
        assert!(IntegrityValidator::validate_not_empty(data).is_ok());
        assert!(IntegrityValidator::validate_not_empty(b"").is_err());
    }

    #[test]
    fn test_json_depth_validation() {
        let config = ValueValidationConfig {
            max_json_depth: 3,
            ..Default::default()
        };
        let manager = ValidationManager::new(KeyValidationConfig::default(), config);

        // Shallow JSON
        let shallow_json = serde_json::json!({"key": "value"});
        let value = Value::Json(shallow_json);
        assert!(manager.validate_value(&value).is_ok());

        // Deep JSON
        let deep_json = serde_json::json!({
            "level1": {
                "level2": {
                    "level3": {
                        "level4": "value"
                    }
                }
            }
        });
        let value = Value::Json(deep_json);
        assert!(manager.validate_value(&value).is_err());
    }
}
