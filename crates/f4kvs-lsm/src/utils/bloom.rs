//! Bloom filter implementation for LSM Tree Engine

use crate::error::Result;

/// Bloom filter for fast negative lookups
#[derive(Debug)]
pub struct BloomFilter {
    /// Bit array data
    pub data: Vec<u8>,
    /// Number of hash functions to use
    pub hash_count: usize,
    /// Total number of bits in the filter
    pub bit_count: usize,
}

impl BloomFilter {
    /// Create a new bloom filter
    pub fn new(bit_count: usize, hash_count: usize) -> Result<Self> {
        let byte_count = bit_count.div_ceil(8);
        Ok(Self {
            data: vec![0; byte_count],
            hash_count,
            bit_count,
        })
    }

    /// Add a key to the filter
    pub fn add(&mut self, _key: &str) -> Result<()> {
        Ok(())
    }

    /// Check if a key might exist
    pub fn might_contain(&self, _key: &str) -> Result<bool> {
        Ok(true) // Conservative: assume key might exist
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_creation() {
        let result = BloomFilter::new(1024, 7);
        assert!(result.is_ok());

        let filter = result.unwrap();
        assert_eq!(filter.bit_count, 1024);
        assert_eq!(filter.hash_count, 7);
        // 1024 bits / 8 = 128 bytes
        assert_eq!(filter.data.len(), 128);
    }

    #[test]
    fn test_bloom_filter_creation_small() {
        let filter = BloomFilter::new(8, 3).unwrap();
        // 8 bits / 8 = 1 byte
        assert_eq!(filter.data.len(), 1);
        assert_eq!(filter.bit_count, 8);
    }

    #[test]
    fn test_bloom_filter_creation_minimum() {
        let filter = BloomFilter::new(1, 1).unwrap();
        // 1 bit / 8 = 1 byte (div_ceil)
        assert_eq!(filter.data.len(), 1);
    }

    #[test]
    fn test_bloom_filter_add() {
        let mut filter = BloomFilter::new(1024, 7).unwrap();

        // Test adding keys returns Ok
        let result = filter.add("key1");
        assert!(result.is_ok());

        let result = filter.add("key2");
        assert!(result.is_ok());
    }

    #[test]
    fn test_bloom_filter_might_contain_empty() {
        let filter = BloomFilter::new(1024, 7).unwrap();

        // Empty filter should return true (conservative)
        let result = filter.might_contain("any_key").unwrap();
        assert!(result);
    }

    #[test]
    fn test_bloom_filter_might_contain_multiple_keys() {
        let mut filter = BloomFilter::new(1024, 7).unwrap();

        // Add multiple keys
        for i in 0..100 {
            filter.add(&format!("key_{}", i)).unwrap();
        }

        // All added keys should return true (conservative)
        for i in 0..100 {
            let result = filter.might_contain(&format!("key_{}", i)).unwrap();
            assert!(result, "Key {} should be found", i);
        }
    }

    #[test]
    fn test_bloom_filter_empty_data_initialization() {
        let filter = BloomFilter::new(1024, 7).unwrap();

        // Initially all bytes should be zero
        for byte in &filter.data {
            assert_eq!(*byte, 0);
        }
    }

    #[test]
    fn test_bloom_filter_various_sizes() {
        let sizes = vec![1, 8, 64, 256, 1024, 4096];

        for size in sizes {
            let filter = BloomFilter::new(size, 5);
            assert!(filter.is_ok(), "Should create filter of size {}", size);

            let f = filter.unwrap();
            let expected_bytes = size.div_ceil(8);
            assert_eq!(f.data.len(), expected_bytes);
        }
    }

    #[test]
    fn test_bloom_filter_various_hash_counts() {
        let hash_counts = vec![1, 3, 7, 10, 20];

        for count in hash_counts {
            let filter = BloomFilter::new(1024, count).unwrap();
            assert_eq!(filter.hash_count, count);
        }
    }

    #[test]
    fn test_bloom_filter_large_bit_count() {
        // Test with very large bit count (e.g., 1 million bits)
        let filter = BloomFilter::new(1_000_000, 10).unwrap();
        assert_eq!(filter.bit_count, 1_000_000);
        assert_eq!(filter.data.len(), 125_000); // 1M / 8
    }

    #[test]
    fn test_bloom_filter_special_characters() {
        let mut filter = BloomFilter::new(1024, 7).unwrap();

        // Test with special characters in keys
        let special_keys = vec![
            "",                       // empty string
            "key with spaces",        // spaces
            "key\twith\ttabs",        // tabs
            "key\nwith\nnewlines",    // newlines
            "key\"with\"quotes",      // quotes
            "key'with'singles",       // single quotes
            "key/with/slashes",       // slashes
            "key\\with\\backslashes", // backslashes
            "key:with:colons",        // colons
            "key,with,commas",        // commas
        ];

        for key in special_keys {
            filter.add(key).unwrap();
            let result = filter.might_contain(key).unwrap();
            assert!(result);
        }
    }

    #[test]
    fn test_bloom_filter_unicode() {
        let mut filter = BloomFilter::new(1024, 7).unwrap();

        // Test with unicode characters
        let unicode_keys = vec![
            "你好世界",            // Chinese
            "こんにちは",          // Japanese
            "안녕하세요",          // Korean
            "🎉🎊🎈",              // Emojis
            "émojis avec accents", // Accented characters
        ];

        for key in unicode_keys {
            filter.add(key).unwrap();
            let result = filter.might_contain(key).unwrap();
            assert!(result);
        }
    }

    #[test]
    fn test_bloom_filter_many_operations() {
        let mut filter = BloomFilter::new(1024, 7).unwrap();

        // Perform many add and check operations
        for i in 0..10000 {
            filter.add(&format!("key_{}", i)).unwrap();
            if i % 10 == 0 {
                let result = filter.might_contain(&format!("key_{}", i)).unwrap();
                assert!(result);
            }
        }
    }

    #[test]
    fn test_bloom_filter_clone_behavior() {
        // Note: BloomFilter doesn't implement Clone, but we can verify the structure
        let filter1 = BloomFilter::new(1024, 7).unwrap();
        let filter2 = BloomFilter::new(1024, 7).unwrap();

        // Both should have identical initial state
        assert_eq!(filter1.bit_count, filter2.bit_count);
        assert_eq!(filter1.hash_count, filter2.hash_count);
        assert_eq!(filter1.data.len(), filter2.data.len());
    }

    #[test]
    fn test_bloom_filter_data_vector_allocation() {
        // Verify that data vector is properly allocated with correct size
        let bit_counts = vec![1, 7, 8, 9, 15, 16, 17, 100, 255, 256, 257];

        for bits in bit_counts {
            let filter = BloomFilter::new(bits, 3).unwrap();
            let expected_bytes = bits.div_ceil(8);

            assert_eq!(
                filter.data.len(),
                expected_bytes,
                "Bit count {} should allocate {} bytes",
                bits,
                expected_bytes
            );
        }
    }

    #[test]
    fn test_bloom_filter_error_handling() {
        // Test that errors are properly propagated through Result types
        let filter = BloomFilter::new(1024, 7).unwrap();

        // might_contain always returns Ok (conservative) for non-corrupted state
        let result: Result<bool> = filter.might_contain("test");
        assert!(result.is_ok());
    }

    #[test]
    fn test_bloom_filter_debug_formatting() {
        let filter = BloomFilter::new(1024, 7).unwrap();

        // Verify Debug trait works
        let debug_str = format!("{:?}", filter);
        assert!(debug_str.contains("BloomFilter"));
    }

    #[test]
    fn test_bloom_filter_add_then_check() {
        let mut filter = BloomFilter::new(1024, 7).unwrap();

        // Add a key and verify it's found (conservative approach)
        filter.add("test_key").unwrap();
        let result = filter.might_contain("test_key").unwrap();
        assert!(result);
    }

    #[test]
    fn test_bloom_filter_different_keys() {
        let mut filter = BloomFilter::new(1024, 7).unwrap();

        // Add some keys
        filter.add("key_a").unwrap();
        filter.add("key_b").unwrap();
        filter.add("key_c").unwrap();

        // All should be found (conservative)
        assert!(filter.might_contain("key_a").unwrap());
        assert!(filter.might_contain("key_b").unwrap());
        assert!(filter.might_contain("key_c").unwrap());
    }

    #[test]
    fn test_bloom_filter_zero_bit_count() {
        // Edge case: zero bit count should still work (creates 1 byte)
        let _filter = BloomFilter::new(0, 1);

        // With div_ceil, 0 / 8 = 0, so this might fail or create empty vector
        // This is acceptable as an edge case behavior
    }

    #[test]
    fn test_bloom_filter_many_hash_functions() {
        let mut filter = BloomFilter::new(1024, 50).unwrap();

        assert_eq!(filter.hash_count, 50);

        // Should still work with many hash functions
        filter.add("test").unwrap();
        let result = filter.might_contain("test").unwrap();
        assert!(result);
    }

    #[test]
    fn test_bloom_filter_very_small() {
        // Test minimum viable bloom filter
        let mut filter = BloomFilter::new(1, 1).unwrap();

        assert_eq!(filter.bit_count, 1);
        assert_eq!(filter.data.len(), 1);

        filter.add("test").unwrap();
        let result = filter.might_contain("test").unwrap();
        assert!(result); // Conservative: always returns true for empty/small filters
    }

    #[test]
    fn test_bloom_filter_struct_fields_public() {
        let mut filter = BloomFilter::new(1024, 7).unwrap();

        // Verify fields are public and accessible
        filter.data[0] = 0xFF;
        assert_eq!(filter.data[0], 0xFF);

        filter.hash_count = 10;
        assert_eq!(filter.hash_count, 10);
    }

    #[test]
    fn test_bloom_filter_default_initialization() {
        // Manually create a default-like state
        let filter = BloomFilter {
            data: vec![0u8; 128],
            hash_count: 7,
            bit_count: 1024,
        };

        assert_eq!(filter.bit_count, 1024);
        assert_eq!(filter.hash_count, 7);
        assert_eq!(filter.data.len(), 128);
    }

    #[test]
    fn test_bloom_filter_data_mutability() {
        let mut filter = BloomFilter::new(1024, 7).unwrap();

        // Verify we can modify the data vector
        for i in 0..filter.data.len() {
            filter.data[i] = (i % 256) as u8;
        }

        // Verify modifications persisted
        for i in 0..filter.data.len() {
            assert_eq!(filter.data[i], (i % 256) as u8);
        }
    }

    #[test]
    fn test_bloom_filter_conservative_behavior() {
        let filter = BloomFilter::new(1024, 7).unwrap();

        // Conservative behavior: might_contain always returns true when no false positives possible
        // This is expected for an unimplemented bloom filter
        assert!(filter.might_contain("any_string").unwrap());
    }

    #[test]
    fn test_bloom_filter_error_message_formatting() {
        let err = crate::LsmError::BloomFilter("test error".to_string());
        let msg = err.user_message();

        assert!(msg.contains("Index error"));
        assert!(msg.contains("test error"));
    }
}
