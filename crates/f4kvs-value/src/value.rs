//! Value types for F4KVS Core
#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use serde::de;
use serde::de::VariantAccess;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value as SerdeJsonValue;

#[cfg(test)]
#[cfg(feature = "proptest")]
use proptest::prelude::*;

/// Core value types supported by F4KVS
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// String value
    String(String),
    /// 64-bit signed integer
    Int64(i64),
    /// 64-bit unsigned integer
    UInt64(u64),
    /// 64-bit floating point
    Float64(f64),
    /// Boolean value
    Bool(bool),
    /// Raw bytes (as `Vec<u8>`)
    Bytes(Vec<u8>),
    /// JSON object
    Json(serde_json::Value),
    /// Optional/null value
    Null,
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Create a wrapper that Bincode can understand
        #[derive(Serialize)]
        enum ValueWrapper<'a> {
            String(&'a String),
            Int64(&'a i64),
            UInt64(&'a u64),
            Float64(&'a f64),
            Bool(&'a bool),
            Bytes(&'a Vec<u8>),
            Json(&'a SerdeJsonValue),
            Null,
        }

        let wrapper = match self {
            Value::String(v) => ValueWrapper::String(v),
            Value::Int64(v) => ValueWrapper::Int64(v),
            Value::UInt64(v) => ValueWrapper::UInt64(v),
            Value::Float64(v) => ValueWrapper::Float64(v),
            Value::Bool(v) => ValueWrapper::Bool(v),
            Value::Bytes(v) => ValueWrapper::Bytes(v),
            Value::Json(v) => ValueWrapper::Json(v),
            Value::Null => ValueWrapper::Null,
        };

        wrapper.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let serde_value: SerdeJsonValue = SerdeJsonValue::deserialize(deserializer)?;
            return Value::from_serde_json_value(serde_value).map_err(de::Error::custom);
        }

        struct ValueVisitor;

        impl<'de> de::Visitor<'de> for ValueVisitor {
            type Value = Value;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a binary F4KVS value")
            }

            fn visit_enum<A>(self, data: A) -> Result<Value, A::Error>
            where
                A: de::EnumAccess<'de>,
            {
                let (variant, variant_access) = data.variant::<u32>()?;
                match variant {
                    0 => Ok(Value::String(variant_access.newtype_variant()?)),
                    1 => Ok(Value::Int64(variant_access.newtype_variant()?)),
                    2 => Ok(Value::UInt64(variant_access.newtype_variant()?)),
                    3 => Ok(Value::Float64(variant_access.newtype_variant()?)),
                    4 => Ok(Value::Bool(variant_access.newtype_variant()?)),
                    5 => Ok(Value::Bytes(variant_access.newtype_variant()?)),
                    6 => Ok(Value::Json(variant_access.newtype_variant()?)),
                    7 => {
                        variant_access.unit_variant()?;
                        Ok(Value::Null)
                    }
                    other => Err(de::Error::unknown_variant(
                        &other.to_string(),
                        &[
                            "String", "Int64", "UInt64", "Float64", "Bool", "Bytes", "Json", "Null",
                        ],
                    )),
                }
            }
        }

        deserializer.deserialize_enum(
            "Value",
            &[
                "String", "Int64", "UInt64", "Float64", "Bool", "Bytes", "Json", "Null",
            ],
            ValueVisitor,
        )
    }
}

impl Value {
    /// Check if value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Get the type name of this value
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::String(_) => "String",
            Value::Int64(_) => "Int64",
            Value::UInt64(_) => "UInt64",
            Value::Float64(_) => "Float64",
            Value::Bool(_) => "Bool",
            Value::Bytes(_) => "Bytes",
            Value::Json(_) => "Json",
            Value::Null => "Null",
        }
    }

    /// Convert to JSON string representation
    /// Uses optimized serialization when available
    pub fn to_json_string(&self) -> crate::Result<String> {
        // Note: simd-json is primarily for parsing, not serialization
        // For serialization, serde_json is already well-optimized
        serde_json::to_string(self).map_err(crate::F4KvsError::from)
    }

    fn from_serde_json_value(value: SerdeJsonValue) -> crate::Result<Self> {
        match value {
            SerdeJsonValue::Object(map) if map.len() == 1 => {
                let mut iter = map.into_iter();
                let (key, value) = iter.next().unwrap();
                match key.as_str() {
                    "String" => match value {
                        SerdeJsonValue::String(s) => Ok(Value::String(s)),
                        other => Err(crate::F4KvsError::serialization_with_format(
                            "Value::String",
                            format!("expected string, got {}", other),
                        )),
                    },
                    "Int64" => match value {
                        SerdeJsonValue::Number(n) => {
                            n.as_i64().map(Value::Int64).ok_or_else(|| {
                                crate::F4KvsError::serialization_with_format(
                                    "Value::Int64",
                                    format!("expected i64, got {}", n),
                                )
                            })
                        }
                        other => Err(crate::F4KvsError::serialization_with_format(
                            "Value::Int64",
                            format!("expected number, got {}", other),
                        )),
                    },
                    "UInt64" => match value {
                        SerdeJsonValue::Number(n) => {
                            n.as_u64().map(Value::UInt64).ok_or_else(|| {
                                crate::F4KvsError::serialization_with_format(
                                    "Value::UInt64",
                                    format!("expected u64, got {}", n),
                                )
                            })
                        }
                        other => Err(crate::F4KvsError::serialization_with_format(
                            "Value::UInt64",
                            format!("expected number, got {}", other),
                        )),
                    },
                    "Float64" => match value {
                        SerdeJsonValue::Number(n) => {
                            n.as_f64().map(Value::Float64).ok_or_else(|| {
                                crate::F4KvsError::serialization_with_format(
                                    "Value::Float64",
                                    format!("expected float, got {}", n),
                                )
                            })
                        }
                        other => Err(crate::F4KvsError::serialization_with_format(
                            "Value::Float64",
                            format!("expected number, got {}", other),
                        )),
                    },
                    "Bool" => match value {
                        SerdeJsonValue::Bool(b) => Ok(Value::Bool(b)),
                        other => Err(crate::F4KvsError::serialization_with_format(
                            "Value::Bool",
                            format!("expected bool, got {}", other),
                        )),
                    },
                    "Bytes" => match value {
                        SerdeJsonValue::Array(arr) => {
                            let bytes = arr
                                .into_iter()
                                .map(|item| {
                                    item.as_u64().and_then(|n| u8::try_from(n).ok()).ok_or_else(
                                        || {
                                            crate::F4KvsError::serialization_with_format(
                                                "Value::Bytes",
                                                format!("expected byte array, got {}", item),
                                            )
                                        },
                                    )
                                })
                                .collect::<Result<Vec<u8>, crate::F4KvsError>>()?;
                            Ok(Value::Bytes(bytes))
                        }
                        other => Err(crate::F4KvsError::serialization_with_format(
                            "Value::Bytes",
                            format!("expected array, got {}", other),
                        )),
                    },
                    "Json" => Ok(Value::Json(value)),
                    "Null" => match value {
                        SerdeJsonValue::Null => Ok(Value::Null),
                        other => Err(crate::F4KvsError::serialization_with_format(
                            "Value::Null",
                            format!("expected null, got {}", other),
                        )),
                    },
                    _ => Ok(Value::Json(SerdeJsonValue::Object({
                        let mut new_map = serde_json::Map::new();
                        new_map.insert(key, value);
                        new_map.extend(iter);
                        new_map
                    }))),
                }
            }
            SerdeJsonValue::String(s) if s == "Null" => Ok(Value::Null),
            other => Ok(Value::Json(other)),
        }
    }

    /// Parse from JSON string
    pub fn from_json_string(json: &str) -> crate::Result<Self> {
        serde_json::from_str::<Value>(json).map_err(crate::F4KvsError::from)
    }

    /// Parse from JSON bytes directly using simd-json for high performance.
    /// This avoids intermediate string allocations and UTF-8 validation overhead.
    pub fn from_json_bytes(json: &[u8]) -> crate::Result<Self> {
        // We need a mutable copy because simd-json performs in-place parsing
        let bytes = json.to_vec();
        let serde_value =
            serde_json::from_slice::<SerdeJsonValue>(&bytes).map_err(crate::F4KvsError::from)?;
        Self::from_serde_json_value(serde_value)
    }

    /// Get the serialized size of this value
    pub fn serialized_size(&self) -> usize {
        match self {
            Value::String(s) => s.len(),
            Value::Int64(_) => 8,
            Value::UInt64(_) => 8,
            Value::Float64(_) => 8,
            Value::Bool(_) => 1,
            Value::Bytes(b) => b.len(),
            Value::Json(j) => j.to_string().len(),
            Value::Null => 0,
        }
    }

    /// Estimate memory size of this value
    pub fn memory_size(&self) -> usize {
        match self {
            Value::String(s) => s.len() + std::mem::size_of::<String>(),
            Value::Int64(_) => std::mem::size_of::<i64>(),
            Value::UInt64(_) => std::mem::size_of::<u64>(),
            Value::Float64(_) => std::mem::size_of::<f64>(),
            Value::Bool(_) => std::mem::size_of::<bool>(),
            Value::Bytes(b) => b.len() + std::mem::size_of::<Vec<u8>>(),
            Value::Json(v) => {
                // More accurate estimation for JSON values
                // serde_json::Value is a tagged enum, estimate based on type
                match v {
                    serde_json::Value::Null => std::mem::size_of::<serde_json::Value>(),
                    serde_json::Value::Bool(_) => std::mem::size_of::<serde_json::Value>(),
                    serde_json::Value::Number(_) => std::mem::size_of::<serde_json::Value>() + 8, // Number can be u64/i64/f64
                    serde_json::Value::String(s) => {
                        std::mem::size_of::<serde_json::Value>() + s.capacity()
                    }
                    serde_json::Value::Array(arr) => {
                        // Base size + capacity * element size estimate
                        let element_size_estimate = arr
                            .iter()
                            .map(|v| match v {
                                serde_json::Value::String(s) => s.capacity(),
                                serde_json::Value::Number(_) => 8,
                                serde_json::Value::Object(_) => 64, // Rough estimate for objects
                                _ => std::mem::size_of::<serde_json::Value>(),
                            })
                            .sum::<usize>();
                        std::mem::size_of::<serde_json::Value>()
                            + arr.capacity() * std::mem::size_of::<serde_json::Value>()
                            + element_size_estimate
                    }
                    serde_json::Value::Object(map) => {
                        // Base size + len * (key + value) size estimate
                        let kv_estimate = map
                            .iter()
                            .map(|(k, v)| {
                                k.capacity()
                                    + match v {
                                        serde_json::Value::String(s) => s.capacity(),
                                        serde_json::Value::Number(_) => 8,
                                        serde_json::Value::Object(_) => 64,
                                        _ => std::mem::size_of::<serde_json::Value>(),
                                    }
                            })
                            .sum::<usize>();
                        std::mem::size_of::<serde_json::Value>()
                            + map.len()
                                * (std::mem::size_of::<String>()
                                    + std::mem::size_of::<serde_json::Value>())
                            + kv_estimate
                    }
                }
            }
            Value::Null => 0,
        }
    }

    /// Convert value to bytes representation
    ///
    /// For large Bytes values, this may clone the data. Consider using
    /// `into_bytes()` when the value is no longer needed to avoid cloning.
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Value::String(s) => s.as_bytes().to_vec(),
            Value::Int64(i) => i.to_le_bytes().to_vec(),
            Value::UInt64(u) => u.to_le_bytes().to_vec(),
            Value::Float64(f) => f.to_le_bytes().to_vec(),
            Value::Bool(b) => vec![if *b { 1 } else { 0 }],
            Value::Bytes(b) => b.clone(),
            Value::Json(v) => v.to_string().as_bytes().to_vec(),
            Value::Null => Vec::new(),
        }
    }

    /// Convert value to bytes representation, consuming the value
    ///
    /// This method avoids cloning for Bytes values by taking ownership.
    /// Use this when the value is no longer needed after conversion.
    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            Value::String(s) => s.into_bytes(),
            Value::Int64(i) => i.to_le_bytes().to_vec(),
            Value::UInt64(u) => u.to_le_bytes().to_vec(),
            Value::Float64(f) => f.to_le_bytes().to_vec(),
            Value::Bool(b) => vec![if b { 1 } else { 0 }],
            Value::Bytes(b) => b, // No clone - take ownership
            Value::Json(v) => v.to_string().into_bytes(),
            Value::Null => Vec::new(),
        }
    }
}

// Convenient conversions
impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.to_string())
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::Int64(i)
    }
}

impl From<u64> for Value {
    fn from(i: u64) -> Self {
        Value::UInt64(i)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Float64(f)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Bytes(v)
    }
}

impl From<serde_json::Value> for Value {
    fn from(v: serde_json::Value) -> Self {
        Value::Json(v)
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::String(s) => write!(f, "{}", s),
            Value::Int64(i) => write!(f, "{}", i),
            Value::UInt64(u) => write!(f, "{}", u),
            Value::Float64(fl) => write!(f, "{}", fl),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Bytes(bytes) => {
                // For bytes, we'll display as hex
                for byte in bytes {
                    write!(f, "{:02x}", byte)?;
                }
                Ok(())
            }
            Value::Json(json) => write!(f, "{}", json),
            Value::Null => write!(f, "null"),
        }
    }
}

#[cfg(test)]
#[cfg(feature = "proptest")]
impl Arbitrary for Value {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            any::<String>().prop_map(Value::String),
            any::<i64>().prop_map(Value::Int64),
            any::<u64>().prop_map(Value::UInt64),
            any::<f64>().prop_map(Value::Float64),
            any::<bool>().prop_map(Value::Bool),
            prop::collection::vec(any::<u8>(), 0..100).prop_map(Value::Bytes),
            prop_oneof![
                Just(serde_json::Value::Null),
                any::<String>().prop_map(serde_json::Value::String),
                any::<i64>().prop_map(|n| serde_json::Value::Number(serde_json::Number::from(n))),
                any::<bool>().prop_map(serde_json::Value::Bool),
            ]
            .prop_map(Value::Json),
            Just(Value::Null),
        ]
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_value_deserialize_from_json_object() {
        let json_str = json!({"key": "value", "number": 42, "array": [1, 2, 3]}).to_string();
        let parsed: Value = serde_json::from_str(&json_str).expect("serde_json parse failed");
        assert_eq!(
            parsed,
            Value::Json(json!({"key": "value", "number": 42, "array": [1, 2, 3]}))
        );
    }

    #[test]
    fn test_value_bincode_roundtrip() {
        let original = Value::String("hello".to_owned());
        let bytes = bincode::serde::encode_to_vec(&original, bincode::config::legacy())
            .expect("bincode encode failed");
        let (decoded, _) =
            bincode::serde::decode_from_slice::<Value, _>(&bytes, bincode::config::legacy())
                .expect("bincode decode failed");
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_value_types() {
        assert_eq!(Value::String("test".to_string()).type_name(), "String");
        assert_eq!(Value::Int64(42).type_name(), "Int64");
        assert_eq!(Value::UInt64(42).type_name(), "UInt64");
        assert_eq!(Value::Float64(std::f64::consts::PI).type_name(), "Float64");
        assert_eq!(Value::Bool(true).type_name(), "Bool");
        assert_eq!(Value::Bytes(vec![1, 2, 3]).type_name(), "Bytes");
        assert_eq!(Value::Json(serde_json::Value::Null).type_name(), "Json");
        assert_eq!(Value::Null.type_name(), "Null");
        assert!(Value::Null.is_null());
        assert!(!Value::String("test".to_string()).is_null());
    }

    #[test]
    fn test_conversions() {
        // String conversions
        let v: Value = "test".into();
        assert_eq!(v, Value::String("test".to_string()));

        let v: Value = "test".to_string().into();
        assert_eq!(v, Value::String("test".to_string()));

        // Integer conversions
        let v: Value = 42i64.into();
        assert_eq!(v, Value::Int64(42));

        let v: Value = 42u64.into();
        assert_eq!(v, Value::UInt64(42));

        // Float conversion
        let v: Value = std::f64::consts::PI.into();
        assert_eq!(v, Value::Float64(std::f64::consts::PI));

        // Boolean conversion
        let v: Value = true.into();
        assert_eq!(v, Value::Bool(true));

        let v: Value = false.into();
        assert_eq!(v, Value::Bool(false));

        // Bytes conversion
        let v: Value = vec![1, 2, 3].into();
        assert_eq!(v, Value::Bytes(vec![1, 2, 3]));

        // JSON conversion
        let json_val = serde_json::json!({"key": "value"});
        let v: Value = json_val.clone().into();
        assert_eq!(v, Value::Json(json_val));
    }

    #[test]
    fn test_json_serialization() {
        // Test all value types with JSON serialization
        let test_cases = vec![
            Value::String("test".to_string()),
            Value::Int64(-42),
            Value::UInt64(42),
            Value::Float64(std::f64::consts::PI),
            Value::Bool(true),
            Value::Bytes(vec![1, 2, 3]),
            Value::Json(serde_json::json!({"nested": {"key": "value"}})),
            Value::Null,
        ];

        for value in test_cases {
            let json = value.to_json_string().unwrap();
            let deserialized = Value::from_json_string(&json).unwrap();
            assert_eq!(value, deserialized);
        }
    }

    #[test]
    fn test_serialized_size() {
        assert_eq!(Value::String("test".to_string()).serialized_size(), 4);
        assert_eq!(Value::Int64(42).serialized_size(), 8);
        assert_eq!(Value::UInt64(42).serialized_size(), 8);
        assert_eq!(Value::Float64(std::f64::consts::PI).serialized_size(), 8);
        assert_eq!(Value::Bool(true).serialized_size(), 1);
        assert_eq!(Value::Bytes(vec![1, 2, 3]).serialized_size(), 3);
        assert_eq!(Value::Null.serialized_size(), 0);
    }

    #[test]
    fn test_memory_size() {
        // Test memory size estimation
        let string_val = Value::String("test".to_string());
        assert!(string_val.memory_size() > string_val.serialized_size());

        let bytes_val = Value::Bytes(vec![1, 2, 3]);
        assert!(bytes_val.memory_size() > bytes_val.serialized_size());

        let json_val = Value::Json(serde_json::json!({"key": "value"}));
        assert!(json_val.memory_size() > json_val.serialized_size());

        // Primitive types should have predictable memory sizes
        assert_eq!(Value::Int64(42).memory_size(), std::mem::size_of::<i64>());
        assert_eq!(Value::UInt64(42).memory_size(), std::mem::size_of::<u64>());
        assert_eq!(
            Value::Float64(std::f64::consts::PI).memory_size(),
            std::mem::size_of::<f64>()
        );
        assert_eq!(Value::Bool(true).memory_size(), std::mem::size_of::<bool>());
        assert_eq!(Value::Null.memory_size(), 0);
    }

    #[test]
    fn test_to_bytes() {
        // Test conversion to bytes for all types
        assert_eq!(
            Value::String("test".to_string()).to_bytes(),
            b"test".to_vec()
        );
        assert_eq!(Value::Int64(42).to_bytes(), 42i64.to_le_bytes().to_vec());
        assert_eq!(Value::UInt64(42).to_bytes(), 42u64.to_le_bytes().to_vec());
        assert_eq!(
            Value::Float64(std::f64::consts::PI).to_bytes(),
            std::f64::consts::PI.to_le_bytes().to_vec()
        );
        assert_eq!(Value::Bool(true).to_bytes(), vec![1]);
        assert_eq!(Value::Bool(false).to_bytes(), vec![0]);
        assert_eq!(Value::Bytes(vec![1, 2, 3]).to_bytes(), vec![1, 2, 3]);
        assert_eq!(Value::Null.to_bytes(), Vec::<u8>::new());

        // Test JSON to bytes
        let json_val = Value::Json(serde_json::json!({"key": "value"}));
        let expected_bytes = r#"{"key":"value"}"#.as_bytes().to_vec();
        assert_eq!(json_val.to_bytes(), expected_bytes);
    }

    #[test]
    fn test_into_bytes() {
        // Test that into_bytes() produces same results as to_bytes()
        assert_eq!(
            Value::String("test".to_string()).into_bytes(),
            Value::String("test".to_string()).to_bytes()
        );
        assert_eq!(Value::Int64(42).into_bytes(), Value::Int64(42).to_bytes());
        assert_eq!(
            Value::Bytes(vec![1, 2, 3]).into_bytes(),
            Value::Bytes(vec![1, 2, 3]).to_bytes()
        );

        // Test that into_bytes() consumes the value (no clone for Bytes)
        let bytes_value = Value::Bytes(vec![1, 2, 3, 4, 5]);
        let bytes = bytes_value.into_bytes();
        assert_eq!(bytes, vec![1, 2, 3, 4, 5]);
        // bytes_value is now moved and cannot be used
    }

    #[test]
    fn test_value_equality() {
        // Test PartialEq implementation
        assert_eq!(
            Value::String("test".to_string()),
            Value::String("test".to_string())
        );
        assert_ne!(
            Value::String("test".to_string()),
            Value::String("different".to_string())
        );

        assert_eq!(Value::Int64(42), Value::Int64(42));
        assert_ne!(Value::Int64(42), Value::Int64(43));

        assert_eq!(Value::Bool(true), Value::Bool(true));
        assert_ne!(Value::Bool(true), Value::Bool(false));

        assert_eq!(Value::Null, Value::Null);
        assert_ne!(Value::Null, Value::String("test".to_string()));
    }

    #[test]
    fn test_value_clone() {
        let original = Value::String("test".to_string());
        let cloned = original.clone();
        assert_eq!(original, cloned);

        let json_val = Value::Json(serde_json::json!({"key": "value"}));
        let cloned_json = json_val.clone();
        assert_eq!(json_val, cloned_json);
    }

    #[test]
    fn test_value_debug() {
        let value = Value::String("test".to_string());
        let debug_str = format!("{:?}", value);
        assert!(debug_str.contains("String"));
        assert!(debug_str.contains("test"));
    }

    #[test]
    fn test_json_error_handling() {
        // Test invalid JSON handling
        let result = Value::from_json_string("invalid json");
        assert!(result.is_err());

        // Test that the error is properly converted
        match result {
            Err(crate::F4KvsError::Serialization { message }) => {
                assert!(!message.is_empty());
            }
            _ => panic!("Expected serialization error"),
        }
    }

    #[test]
    fn test_large_values() {
        // Test with large strings
        let large_string = "x".repeat(10000);
        let value = Value::String(large_string.clone());
        assert_eq!(value.serialized_size(), 10000);
        assert!(value.memory_size() > 10000);

        // Test with large byte arrays
        let large_bytes = vec![0u8; 10000];
        let value = Value::Bytes(large_bytes.clone());
        assert_eq!(value.serialized_size(), 10000);
        assert!(value.memory_size() > 10000);

        // Test JSON serialization with large values
        let json = value.to_json_string().unwrap();
        let deserialized = Value::from_json_string(&json).unwrap();
        assert_eq!(value, deserialized);
    }

    #[test]
    fn test_edge_case_values() {
        // Test edge case numbers
        assert_eq!(
            Value::Int64(i64::MIN).to_bytes(),
            i64::MIN.to_le_bytes().to_vec()
        );
        assert_eq!(
            Value::Int64(i64::MAX).to_bytes(),
            i64::MAX.to_le_bytes().to_vec()
        );
        assert_eq!(
            Value::UInt64(u64::MAX).to_bytes(),
            u64::MAX.to_le_bytes().to_vec()
        );
        assert_eq!(
            Value::Float64(f64::NAN).to_bytes(),
            f64::NAN.to_le_bytes().to_vec()
        );
        assert_eq!(
            Value::Float64(f64::INFINITY).to_bytes(),
            f64::INFINITY.to_le_bytes().to_vec()
        );

        // Test empty values
        assert_eq!(Value::String(String::new()).serialized_size(), 0);
        assert_eq!(Value::Bytes(Vec::new()).serialized_size(), 0);
        assert_eq!(Value::Json(serde_json::Value::Null).serialized_size(), 4); // "null"
    }
}
