//! SSTable implementation for LSM Tree Engine

use crate::core::config::SstableConfig;
use crate::error::{LsmError, Result};
use crate::utils;
use crc32fast::Hasher as Crc32Hasher;
use f4kvs_value::Value;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter};
use tokio::time::{sleep, Duration};
use tracing::{error, warn};

/// Bloom filter implementation for SSTables
///
/// This module provides a simple bloom filter implementation used by SSTables
/// for fast key existence checks. It includes bounds checking and validation
/// to prevent index out of bounds panics.
pub mod bloom_filter {
    use serde::{Deserialize, Serialize};
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use tracing::warn;

    /// Simple bloom filter implementation
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct BloomFilter {
        bits: Vec<bool>,
        hash_count: usize,
        size: usize,
    }

    impl BloomFilter {
        /// Create a new bloom filter
        /// size: number of bits in the filter
        /// hash_count: number of hash functions to use
        pub fn new(size: usize, hash_count: usize) -> Self {
            Self {
                bits: vec![false; size],
                hash_count,
                size,
            }
        }

        /// Create a bloom filter with optimal parameters for given number of items
        /// Uses 10 bits per item and 7 hash functions (RocksDB defaults)
        pub fn with_optimal_params(item_count: usize) -> Self {
            let size = item_count * 10; // 10 bits per item
            let hash_count = 7; // 7 hash functions
            Self::new(size, hash_count)
        }

        /// Add a key to the bloom filter
        pub fn add(&mut self, key: &str) {
            // Validate and fix invariant before operations
            self.validate_and_fix_invariant();

            // Defensive check: ensure bloom filter is valid before adding
            if self.bits.is_empty() || self.size == 0 {
                warn!(
                    "Attempted to add key '{}' to invalid bloom filter (bits.len={}, size={})",
                    key,
                    self.bits.len(),
                    self.size
                );
                return;
            }

            // After fixing, invariant should hold: size == bits.len()
            // So index % size will always be < bits.len()
            for i in 0..self.hash_count {
                let hash = self.hash(key, i);
                let index = hash % self.size;

                // With invariant enforced, this should never panic
                // But keep defensive check for safety
                if index < self.bits.len() {
                    self.bits[index] = true;
                } else {
                    // This should never happen if invariant is maintained
                    warn!(
                        "Bloom filter index {} out of bounds (bits.len={}, size={}) after validation",
                        index,
                        self.bits.len(),
                        self.size
                    );
                    // Fix and retry
                    self.validate_and_fix_invariant();
                    if index < self.bits.len() {
                        self.bits[index] = true;
                    }
                }
            }
        }

        /// Check if a key might be in the filter
        /// Returns false if definitely not present, true if might be present
        ///
        /// Note: This is a read-only operation. If the invariant is violated,
        /// this method returns true (conservative) to avoid false negatives.
        pub fn might_contain(&self, key: &str) -> bool {
            // If bloom filter is invalid, return true (conservative)
            // This prevents false negatives which could cause data loss
            if !self.is_valid() {
                warn!(
                    "Bloom filter invalid (bits.len()={} != size={}), returning conservative result",
                    self.bits.len(),
                    self.size
                );
                return true;
            }

            // With invariant satisfied, size == bits.len()
            // So index % size will always be < bits.len()
            for i in 0..self.hash_count {
                let hash = self.hash(key, i);
                let index = hash % self.size;

                // Defensive check - should never fail if invariant holds
                if index >= self.bits.len() {
                    warn!("Bloom filter index {} out of bounds (bits.len()={}, size={}), assuming key '{}' might be present",
                          index, self.bits.len(), self.size, key);
                    return true; // Conservative fallback
                }

                if !self.bits[index] {
                    return false;
                }
            }
            true
        }

        /// Get the hash count used by this bloom filter
        pub fn hash_count(&self) -> usize {
            self.hash_count
        }

        /// Validate and fix the invariant that size == bits.len()
        /// This ensures the bloom filter is always in a consistent state
        /// Returns true if the filter was fixed, false if already valid
        pub fn validate_and_fix_invariant(&mut self) -> bool {
            if self.bits.len() != self.size {
                warn!(
                    "Bloom filter invariant violated: bits.len()={} != size={}, fixing",
                    self.bits.len(),
                    self.size
                );

                if self.bits.len() < self.size {
                    // Bits vector is too small - extend it
                    self.bits.resize(self.size, false);
                } else {
                    // Bits vector is too large - truncate it
                    self.bits.truncate(self.size);
                }
                true
            } else {
                false
            }
        }

        /// Check if the bloom filter is in a valid state
        /// Returns true if bits.len() == size and size > 0
        pub fn is_valid(&self) -> bool {
            self.bits.len() == self.size && self.size > 0
        }

        /// Get the size of the bloom filter
        pub fn size(&self) -> usize {
            self.size
        }

        /// Get the length of the bits vector
        pub fn bits_len(&self) -> usize {
            self.bits.len()
        }

        /// Check if the bits vector is empty
        pub fn bits_is_empty(&self) -> bool {
            self.bits.is_empty()
        }

        /// Clear the bits vector (for testing purposes)
        /// This also resets the size to maintain the invariant
        pub fn clear_bits(&mut self) {
            self.bits.clear();
            self.size = 0;
        }

        /// Clear all bits but maintain size (resets filter while keeping structure)
        pub fn clear(&mut self) {
            // Validate and fix invariant first
            self.validate_and_fix_invariant();
            // Set all bits to false while maintaining size
            for bit in &mut self.bits {
                *bit = false;
            }
        }

        /// Hash function that produces different hashes for different hash_count values
        fn hash(&self, key: &str, hash_index: usize) -> usize {
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            (hash_index as u64).hash(&mut hasher);
            hasher.finish() as usize
        }

        /// Get the size of the bloom filter in bytes
        pub fn size_bytes(&self) -> usize {
            self.size.div_ceil(8) // Round up to nearest byte
        }

        /// Serialize the bloom filter to bytes
        pub fn to_bytes(&self) -> Vec<u8> {
            let mut bytes = Vec::new();

            // First 4 bytes: original size (u32)
            bytes.extend_from_slice(&(self.size as u32).to_le_bytes());

            // Then the actual bit data
            for chunk in self.bits.chunks(8) {
                let mut byte = 0u8;
                for (i, &bit) in chunk.iter().enumerate() {
                    if bit {
                        byte |= 1 << i;
                    }
                }
                bytes.push(byte);
            }
            bytes
        }

        /// Deserialize bloom filter from bytes
        pub fn from_bytes(bytes: &[u8], hash_count: usize) -> Self {
            // Handle empty data - return empty valid filter
            if bytes.is_empty() {
                warn!("Empty bloom filter data, creating empty filter");
                return Self::new(0, hash_count);
            }

            if bytes.len() < 4 {
                // Fallback for old format - assume size is bytes.len() * 8
                let size = bytes.len() * 8;
                let mut bits = Vec::with_capacity(size);

                for &byte in bytes {
                    for i in 0..8 {
                        bits.push((byte & (1 << i)) != 0);
                    }
                }

                let mut filter = Self {
                    bits,
                    hash_count,
                    size,
                };

                // Validate and fix invariant before returning
                filter.validate_and_fix_invariant();

                // Final check - if still invalid, return empty filter
                if !filter.is_valid() {
                    warn!("Old format bloom filter is invalid after fix, creating empty filter");
                    return Self::new(0, hash_count);
                }

                return filter;
            }

            // Read original size from first 4 bytes
            let size = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;

            // Validate size is reasonable (max 100MB bloom filter = ~12.5M elements)
            const MAX_BLOOM_SIZE: usize = 100_000_000;
            if size > MAX_BLOOM_SIZE {
                warn!(
                    "Bloom filter size {} exceeds maximum {}, creating empty filter",
                    size, MAX_BLOOM_SIZE
                );
                return Self::new(0, hash_count);
            }

            let mut bits = Vec::with_capacity(size);

            // Read bit data from remaining bytes
            for &byte in bytes.iter().skip(4) {
                for i in 0..8 {
                    bits.push((byte & (1 << i)) != 0);
                }
            }

            // Truncate to original size in case of padding
            // Ensure we have exactly `size` bits
            if bits.len() < size {
                // Not enough bits - extend with false
                bits.resize(size, false);
            } else if bits.len() > size {
                // Too many bits - truncate
                bits.truncate(size);
            }

            let mut filter = Self {
                bits,
                hash_count,
                size,
            };

            // Validate and fix invariant before returning
            // This ensures the filter is always in a valid state
            filter.validate_and_fix_invariant();

            // Final check - if still invalid, return empty filter
            if !filter.is_valid() {
                warn!(
                    "Bloom filter still invalid after fix (bits.len()={} != size={}), creating empty filter",
                    filter.bits.len(),
                    filter.size
                );
                return Self::new(0, hash_count);
            }

            filter
        }
    }
}

use bloom_filter::BloomFilter;

/// SSTable entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableEntry {
    /// Key for this entry
    pub key: String,
    /// Value for this entry
    pub value: Value,
    /// Timestamp when this entry was created
    pub timestamp: u64,
    /// Whether this entry is marked as deleted
    pub deleted: bool,
}

/// SSTable metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableMetadata {
    /// Number of entries in this SSTable
    pub entry_count: usize,
    /// Total file size in bytes
    pub file_size: u64,
    /// Smallest key in this SSTable
    pub smallest_key: String,
    /// Largest key in this SSTable
    pub largest_key: String,
    /// Level this SSTable belongs to
    pub level: usize,
    /// Checksum for data integrity
    pub checksum: u32,
    /// Creation timestamp
    pub created_at: u64,
    /// Offset of the index in the file
    pub index_offset: u64,
    /// Size of the index in bytes
    pub index_size: u64,
    /// Offset of the bloom filter in the file
    pub bloom_filter_offset: u64,
    /// Size of the bloom filter in bytes
    pub bloom_filter_size: u64,
    /// Number of hash functions used in bloom filter
    pub bloom_filter_hash_count: usize,
}

/// SSTable index entry
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexEntry {
    key: String,
    offset: u64,
    size: u32,
}

/// SSTable implementation
pub struct SSTable {
    /// File path
    path: PathBuf,

    /// Configuration
    config: SstableConfig,

    /// Metadata
    metadata: SSTableMetadata,

    /// In-memory index for fast lookups
    index: BTreeMap<String, (u64, u32)>,

    /// Bloom filter for fast key existence checks
    bloom_filter: Option<BloomFilter>,

    /// File handle protected by RwLock to prevent concurrent access races
    /// Each read operation needs exclusive access to the file handle due to seek position
    file: tokio::sync::RwLock<Option<File>>,

    /// Last access time for LRU eviction (nanoseconds since epoch)
    last_access: std::sync::atomic::AtomicU64,

    /// Active reader count (for preventing deletion during reads)
    reader_count: std::sync::atomic::AtomicUsize,

    /// Marked for deletion (pending until all readers done)
    marked_for_deletion: std::sync::atomic::AtomicBool,

    /// Ready flag: indicates SSTable is fully written, synced, and metadata/index are loaded
    /// This prevents reads from happening before the SSTable is in a consistent state
    is_ready: std::sync::atomic::AtomicBool,
}

impl SSTable {
    /// Create a new SSTable
    pub fn new(path: PathBuf, config: SstableConfig, level: usize) -> Result<Self> {
        let metadata = SSTableMetadata {
            entry_count: 0,
            file_size: 0,
            smallest_key: String::new(),
            largest_key: String::new(),
            level,
            checksum: 0,
            created_at: utils::timestamp_secs(),
            index_offset: 0,
            index_size: 0,
            bloom_filter_offset: 0,
            bloom_filter_size: 0,
            bloom_filter_hash_count: 7, // Default hash count
        };

        Ok(Self {
            path,
            config,
            metadata,
            index: BTreeMap::new(),
            bloom_filter: None,
            file: tokio::sync::RwLock::new(None),
            last_access: std::sync::atomic::AtomicU64::new(0),
            reader_count: std::sync::atomic::AtomicUsize::new(0),
            marked_for_deletion: std::sync::atomic::AtomicBool::new(false),
            is_ready: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Write entries to SSTable
    pub async fn write_entries(&mut self, entries: Vec<SSTableEntry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        // Create file
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
            .await
            .map_err(LsmError::Io)?;

        let mut writer = BufWriter::new(file);

        // Update metadata
        self.metadata.entry_count = entries.len();
        // Safe to unwrap: entries.is_empty() is checked at function start
        self.metadata.smallest_key = entries
            .first()
            .expect("entries should not be empty")
            .key
            .clone();
        self.metadata.largest_key = entries
            .last()
            .expect("entries should not be empty")
            .key
            .clone();

        // Write entries with checksums
        let mut offset = 0u64;
        let mut file_hasher = Crc32Hasher::new();

        for entry in &entries {
            let entry_data = bincode::serialize(entry).map_err(|e| {
                LsmError::Serialization(format!("Failed to serialize entry: {}", e))
            })?;

            let entry_size = entry_data.len() as u32;

            // Compute checksum for this entry (size + data)
            let mut entry_hasher = Crc32Hasher::new();
            entry_hasher.update(&entry_size.to_le_bytes());
            entry_hasher.update(&entry_data);
            let entry_checksum = entry_hasher.finalize();

            // Update file-level checksum
            file_hasher.update(&entry_size.to_le_bytes());
            file_hasher.update(&entry_data);
            file_hasher.update(&entry_checksum.to_le_bytes());

            // Write entry size
            writer
                .write_u32_le(entry_size)
                .await
                .map_err(LsmError::Io)?;

            // Write entry data
            writer.write_all(&entry_data).await.map_err(LsmError::Io)?;

            // Write entry checksum
            writer
                .write_u32_le(entry_checksum)
                .await
                .map_err(LsmError::Io)?;

            // Store in index (offset and size including checksum)
            let total_entry_size = 4 + entry_size + 4; // size + data + checksum
            self.index
                .insert(entry.key.clone(), (offset, total_entry_size));
            offset += total_entry_size as u64;
        }

        // Update metadata
        self.metadata.file_size = offset;
        self.metadata.index_offset = offset;

        // Write index with checksum
        let index_data = bincode::serialize(&self.index)
            .map_err(|e| LsmError::Serialization(format!("Failed to serialize index: {}", e)))?;

        self.metadata.index_size = index_data.len() as u64;

        // Compute and write index checksum
        let mut index_hasher = Crc32Hasher::new();
        index_hasher.update(&index_data);
        let index_checksum = index_hasher.finalize();
        file_hasher.update(&index_data);
        file_hasher.update(&index_checksum.to_le_bytes());

        writer.write_all(&index_data).await.map_err(LsmError::Io)?;
        writer
            .write_u32_le(index_checksum)
            .await
            .map_err(LsmError::Io)?;

        // Update offset for bloom filter
        offset += index_data.len() as u64 + 4; // index + checksum

        // Create and write bloom filter
        let mut bloom_filter = BloomFilter::with_optimal_params(entries.len());
        for entry in &entries {
            if !entry.deleted {
                bloom_filter.add(&entry.key);
            }
        }

        let bloom_filter_data = bloom_filter.to_bytes();
        self.metadata.bloom_filter_offset = offset;
        self.metadata.bloom_filter_size = bloom_filter_data.len() as u64;
        self.metadata.bloom_filter_hash_count = bloom_filter.hash_count();

        // Compute and write bloom filter checksum
        let mut bloom_hasher = Crc32Hasher::new();
        bloom_hasher.update(&bloom_filter_data);
        let bloom_checksum = bloom_hasher.finalize();
        file_hasher.update(&bloom_filter_data);
        file_hasher.update(&bloom_checksum.to_le_bytes());

        writer
            .write_all(&bloom_filter_data)
            .await
            .map_err(LsmError::Io)?;
        writer
            .write_u32_le(bloom_checksum)
            .await
            .map_err(LsmError::Io)?;

        // Store bloom filter in memory for fast access
        self.bloom_filter = Some(bloom_filter);

        // Compute final file checksum (checksum of all data)
        let file_checksum = file_hasher.finalize();
        self.metadata.checksum = file_checksum;

        // Write metadata at the end
        let metadata_data = bincode::serialize(&self.metadata)
            .map_err(|e| LsmError::Serialization(format!("Failed to serialize metadata: {}", e)))?;

        // Compute metadata checksum
        let mut metadata_hasher = Crc32Hasher::new();
        metadata_hasher.update(&metadata_data);
        let metadata_checksum = metadata_hasher.finalize();

        writer
            .write_all(&metadata_data)
            .await
            .map_err(LsmError::Io)?;
        writer
            .write_u32_le(metadata_checksum)
            .await
            .map_err(LsmError::Io)?;

        // Flush and sync to ensure data is persisted to disk
        writer.flush().await.map_err(LsmError::Io)?;

        // Get the underlying file and sync to disk
        // This ensures data is fully written before the SSTable is made available for reads
        let file = writer.into_inner();
        file.sync_all().await.map_err(LsmError::Io)?;

        // Mark SSTable as ready since:
        // 1. File is fully written and synced
        // 2. Index is already in memory (built during write)
        // 3. Bloom filter is already in memory (built during write)
        // 4. Metadata is already populated
        // Note: File handle is closed, but will be re-opened lazily when needed for reading
        self.is_ready
            .store(true, std::sync::atomic::Ordering::Release);

        Ok(())
    }

    /// Open SSTable for reading
    pub async fn open(&mut self) -> Result<()> {
        // Update last access time
        self.update_last_access();

        // If already open, just update access time
        {
            let file_guard = self.file.read().await;
            if file_guard.is_some() {
                return Ok(());
            }
        }

        let file = OpenOptions::new()
            .read(true)
            .open(&self.path)
            .await
            .map_err(LsmError::Io)?;

        {
            let mut file_guard = self.file.write().await;
            *file_guard = Some(file);
        }

        // Read metadata and index
        self.read_metadata_and_index().await?;

        // Mark SSTable as ready only after metadata and index are fully loaded
        // This ensures reads can safely access the index and metadata
        self.is_ready
            .store(true, std::sync::atomic::Ordering::Release);

        Ok(())
    }

    /// Update last access time to current time
    fn update_last_access(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        self.last_access
            .store(now, std::sync::atomic::Ordering::Relaxed);
    }

    /// Get last access time
    pub fn last_access(&self) -> u64 {
        self.last_access.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Ensure file is open, re-opening if necessary
    pub async fn ensure_file_open(&self) -> Result<()> {
        let needs_open = {
            let file_guard = self.file.read().await;
            file_guard.is_none()
        };

        if needs_open {
            if self.config.enable_resilient_handling {
                warn!("Re-opening closed SSTable file: {:?}", self.path);
                let file = OpenOptions::new()
                    .read(true)
                    .open(&self.path)
                    .await
                    .map_err(LsmError::Io)?;
                {
                    let mut file_guard = self.file.write().await;
                    *file_guard = Some(file);
                }
                self.update_last_access();
            } else {
                return Err(LsmError::Internal(
                    "File not open and resilient handling is disabled".to_string(),
                ));
            }
        } else {
            // Update access time even if already open
            self.update_last_access();
        }
        Ok(())
    }

    /// Read metadata and index from file with retry logic
    async fn read_metadata_and_index(&mut self) -> Result<()> {
        let mut attempts = 0;
        let max_attempts = self.config.file_retry_attempts;
        let retry_delay = Duration::from_millis(self.config.retry_delay_ms);

        loop {
            match self.try_read_metadata_and_index().await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    attempts += 1;
                    if attempts >= max_attempts {
                        return Err(e);
                    }

                    error!("Failed to read metadata and index (attempt {attempts}): {e}");

                    // Try to re-open the file
                    if let Err(reopen_err) = self.ensure_file_open().await {
                        error!("Failed to re-open file: {reopen_err}");
                        return Err(reopen_err);
                    }

                    // Wait before retry
                    sleep(retry_delay).await;
                }
            }
        }
    }

    /// Try to read metadata and index from file
    async fn try_read_metadata_and_index(&mut self) -> Result<()> {
        self.ensure_file_open().await?;

        let mut file_guard = self.file.write().await;
        let file = file_guard
            .as_mut()
            .ok_or_else(|| LsmError::Internal("File not open".to_string()))?;

        // Seek to end to read metadata
        let file_size = file.metadata().await.map_err(LsmError::Io)?.len();

        // Read metadata from end of file (metadata is at the end, followed by its checksum)
        // We need to read enough to get metadata + checksum (4 bytes)
        let mut buffer = vec![0u8; 2048]; // Increased buffer size for metadata + checksum
        let mut metadata_size = 0usize;
        let mut metadata_offset = 0u64;

        // Read backwards to find metadata
        // Metadata is at the end, followed by 4-byte checksum
        let mut pos = file_size;
        while pos > 0 && metadata_size == 0 {
            let read_size = std::cmp::min(pos, buffer.len() as u64) as usize;
            pos -= read_size as u64;

            file.seek(tokio::io::SeekFrom::Start(pos))
                .await
                .map_err(LsmError::Io)?;

            let bytes_read = file
                .read(&mut buffer[..read_size])
                .await
                .map_err(LsmError::Io)?;

            // Try to deserialize metadata from this chunk
            // Metadata is followed by 4-byte checksum, so we need at least metadata size + 4 bytes
            for i in (0..bytes_read.saturating_sub(4)).rev() {
                if let Ok(metadata) =
                    bincode::deserialize::<SSTableMetadata>(&buffer[i..bytes_read - 4])
                {
                    self.metadata = metadata;
                    metadata_size = bytes_read - 4 - i;
                    metadata_offset = pos + i as u64;
                    break;
                }
            }
        }

        if metadata_size == 0 {
            return Err(LsmError::Corruption("Failed to read metadata".to_string()));
        }

        // Read and validate metadata checksum
        let metadata_checksum_offset = metadata_offset + metadata_size as u64;
        file.seek(tokio::io::SeekFrom::Start(metadata_checksum_offset))
            .await
            .map_err(LsmError::Io)?;

        let stored_metadata_checksum = file.read_u32_le().await.map_err(LsmError::Io)?;

        // Read the metadata bytes to validate checksum
        file.seek(tokio::io::SeekFrom::Start(metadata_offset))
            .await
            .map_err(LsmError::Io)?;
        let mut metadata_buffer = vec![0u8; metadata_size];
        file.read_exact(&mut metadata_buffer)
            .await
            .map_err(LsmError::Io)?;

        // Validate metadata checksum
        let mut metadata_hasher = Crc32Hasher::new();
        metadata_hasher.update(&metadata_buffer);
        let computed_metadata_checksum = metadata_hasher.finalize();

        if stored_metadata_checksum != computed_metadata_checksum {
            return Err(LsmError::Corruption(format!(
                "SSTable metadata checksum mismatch: stored={}, computed={}. \
                Metadata may be corrupted.",
                stored_metadata_checksum, computed_metadata_checksum
            )));
        }

        // Read index with checksum validation
        let index_start = self.metadata.index_offset;
        let index_size = self.metadata.index_size;

        file.seek(tokio::io::SeekFrom::Start(index_start))
            .await
            .map_err(LsmError::Io)?;

        let mut index_buffer = vec![0u8; index_size as usize];
        file.read_exact(&mut index_buffer)
            .await
            .map_err(LsmError::Io)?;

        // Read index checksum
        let stored_index_checksum = file.read_u32_le().await.map_err(LsmError::Io)?;

        // Validate index checksum
        let mut index_hasher = Crc32Hasher::new();
        index_hasher.update(&index_buffer);
        let computed_index_checksum = index_hasher.finalize();

        if stored_index_checksum != computed_index_checksum {
            return Err(LsmError::Corruption(format!(
                "SSTable index checksum mismatch: stored={}, computed={}. \
                Index data may be corrupted.",
                stored_index_checksum, computed_index_checksum
            )));
        }

        self.index = bincode::deserialize(&index_buffer)
            .map_err(|e| LsmError::Serialization(format!("Failed to deserialize index: {}", e)))?;

        // Read bloom filter with checksum validation
        let bloom_filter_start = self.metadata.bloom_filter_offset;
        let bloom_filter_size = self.metadata.bloom_filter_size;

        if bloom_filter_size > 0 {
            // Read bloom filter data
            file.seek(tokio::io::SeekFrom::Start(bloom_filter_start))
                .await
                .map_err(LsmError::Io)?;

            let mut bloom_filter_buffer = vec![0u8; bloom_filter_size as usize];
            file.read_exact(&mut bloom_filter_buffer)
                .await
                .map_err(LsmError::Io)?;

            // Read bloom filter checksum
            let stored_bloom_checksum = file.read_u32_le().await.map_err(LsmError::Io)?;

            // Validate bloom filter checksum
            let mut bloom_hasher = Crc32Hasher::new();
            bloom_hasher.update(&bloom_filter_buffer);
            let computed_bloom_checksum = bloom_hasher.finalize();

            if stored_bloom_checksum != computed_bloom_checksum {
                warn!(
                    "SSTable bloom filter checksum mismatch: stored={}, computed={}. \
                    Bloom filter may be corrupted, continuing without it.",
                    stored_bloom_checksum, computed_bloom_checksum
                );
                // Continue without bloom filter - it's not critical for correctness
                self.bloom_filter = None;
            } else {
                // Checksum is valid, try to load bloom filter
                match Self::try_load_bloom_filter(
                    &bloom_filter_buffer,
                    self.metadata.bloom_filter_hash_count,
                ) {
                    Ok(filter) => {
                        // Validate the loaded bloom filter
                        if filter.is_valid() {
                            self.bloom_filter = Some(filter);
                        } else {
                            warn!("Loaded bloom filter is invalid, continuing without it");
                            self.bloom_filter = None;
                        }
                    }
                    Err(e) => {
                        warn!("Failed to load bloom filter: {}, continuing without it", e);
                        self.bloom_filter = None;
                    }
                }
            }
        }

        Ok(())
    }

    /// Try to load bloom filter from file with error handling
    /// Load bloom filter from bytes (helper for checksum-validated loading)
    fn try_load_bloom_filter(bloom_filter_buffer: &[u8], hash_count: usize) -> Result<BloomFilter> {
        let filter = BloomFilter::from_bytes(bloom_filter_buffer, hash_count);
        Ok(filter)
    }

    /// Fast check: key could exist in this SSTable (range + bloom). Does not touch the file.
    pub fn key_may_exist(&self, key: &str) -> bool {
        if !self.is_ready.load(std::sync::atomic::Ordering::Acquire) {
            return false;
        }
        if self.is_marked_for_deletion() {
            return false;
        }
        if key < self.metadata.smallest_key.as_str() || key > self.metadata.largest_key.as_str() {
            return false;
        }
        if let Some(ref bloom_filter) = self.bloom_filter {
            if !bloom_filter.might_contain(key) {
                return false;
            }
        }
        true
    }

    /// Get value by key with resilient file handling
    pub async fn get(&self, key: &str) -> Result<Option<Value>> {
        // CRITICAL: Check if SSTable is ready before allowing reads
        // This prevents reads from happening before metadata/index are fully loaded
        if !self.is_ready.load(std::sync::atomic::Ordering::Acquire) {
            return Err(LsmError::Internal(format!(
                "Cannot read from SSTable that is not ready: {:?}. \
                The SSTable may still be being written or metadata/index may not be loaded.",
                self.path
            )));
        }

        if !self.key_may_exist(key) {
            return Ok(None);
        }

        // Increment reader count FIRST before any checks
        // This ensures we're counted as a reader even if deletion happens during the read
        // Use a guard pattern to ensure reader count is always decremented on error paths
        self.reader_count
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);

        // Check if marked for deletion AFTER incrementing reader count
        // This ensures we're counted as a reader before checking deletion status
        // If marked for deletion, we still allow the read to proceed to prevent data loss
        // The deletion will wait for us to finish via the reader count mechanism
        if self.is_marked_for_deletion() {
            // Log warning but allow read to proceed - compaction will wait for us
            warn!(
                "Reading from SSTable marked for deletion: {:?} (reader_count: {})",
                self.path,
                self.reader_count()
            );
        }

        // Guard to ensure reader count is decremented on error return paths
        // On success, we manually decrement before returning
        struct ReaderGuard<'a> {
            reader_count: &'a std::sync::atomic::AtomicUsize,
            decremented: bool,
        }

        impl<'a> ReaderGuard<'a> {
            fn new(reader_count: &'a std::sync::atomic::AtomicUsize) -> Self {
                Self {
                    reader_count,
                    decremented: false,
                }
            }

            fn decrement(&mut self) {
                if !self.decremented {
                    self.reader_count
                        .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                    self.decremented = true;
                }
            }
        }

        impl<'a> Drop for ReaderGuard<'a> {
            fn drop(&mut self) {
                if !self.decremented {
                    self.reader_count
                        .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                }
            }
        }

        let mut guard = ReaderGuard::new(&self.reader_count);

        // Update last access time for LRU tracking
        self.update_last_access();

        log::trace!("=== SSTABLE GET DEBUG ===");
        log::trace!("SSTable: {:?}", self.path.file_name().unwrap_or_default());
        log::trace!("Looking for key: '{}'", key);
        log::trace!(
            "Key range: '{}' to '{}'",
            self.metadata.smallest_key,
            self.metadata.largest_key
        );
        log::trace!("Index size: {}", self.index.len());

        // Check if key is in range
        if key < self.metadata.smallest_key.as_str() || key > self.metadata.largest_key.as_str() {
            log::trace!("Key '{}' is out of range", key);
            return Ok(None);
        }

        // Use bloom filter for fast key existence check
        if let Some(ref bloom_filter) = self.bloom_filter {
            if !bloom_filter.might_contain(key) {
                log::trace!("Bloom filter says key '{}' is not present", key);
                return Ok(None); // Key definitely not present
            }
            log::trace!("Bloom filter says key '{}' might be present", key);
        }

        // Look up in index - this is a read-only operation on the BTreeMap which is safe for concurrent access
        // The index is only modified during SSTable creation/writing, not during reads
        let (offset, size) = match self.index.get(key) {
            Some(entry) => {
                log::trace!("Found key '{}' in index at offset {}", key, entry.0);
                let (offset, size) = *entry;

                // CRITICAL: Validate that the index offset is reasonable
                // The offset should be within the data section (before index_offset)
                // If index_offset is 0, it means metadata hasn't been loaded yet, which is an error
                if self.metadata.index_offset == 0 {
                    guard.decrement();
                    return Err(LsmError::Internal(format!(
                        "SSTable metadata index_offset is 0 (uninitialized). \
                        This indicates the SSTable was not properly opened. Path: {:?}",
                        self.path
                    )));
                }

                // Validate offset is within data section
                if offset >= self.metadata.index_offset {
                    guard.decrement();
                    return Err(LsmError::Corruption(format!(
                        "Index offset {} for key '{}' is in index/metadata section (index starts at {}). \
                        This indicates index corruption. SSTable: {:?}",
                        offset, key, self.metadata.index_offset, self.path
                    )));
                }

                // Validate size is reasonable
                if size == 0 {
                    guard.decrement();
                    return Err(LsmError::Corruption(format!(
                        "Index entry size is zero for key '{}' at offset {}. SSTable: {:?}",
                        key, offset, self.path
                    )));
                }

                if size > 100 * 1024 * 1024 {
                    guard.decrement();
                    return Err(LsmError::Corruption(format!(
                        "Index entry size {} for key '{}' at offset {} is unreasonably large. SSTable: {:?}",
                        size, key, offset, self.path
                    )));
                }

                (offset, size)
            }
            None => {
                log::trace!("Key '{}' not found in index", key);
                log::trace!(
                    "Available keys in index: {:?}",
                    self.index.keys().collect::<Vec<_>>()
                );
                // Decrement reader count before returning
                guard.decrement();
                return Ok(None);
            }
        };

        // Validate offset and size are reasonable before attempting to read
        // This prevents reading from obviously corrupted index entries
        if offset > self.metadata.file_size {
            error!(
                "Index offset {} exceeds file size {} for key '{}' in SSTable {:?}",
                offset, self.metadata.file_size, key, self.path
            );
            guard.decrement();
            return Err(LsmError::Corruption(format!(
                "Index offset {} exceeds file size {}",
                offset, self.metadata.file_size
            )));
        }

        if size == 0 || size > 100 * 1024 * 1024 {
            // 100MB max entry size
            error!(
                "Invalid entry size {} for key '{}' in SSTable {:?}",
                size, key, self.path
            );
            guard.decrement();
            return Err(LsmError::Corruption(format!(
                "Invalid entry size {} (expected 1-{} bytes)",
                size,
                100 * 1024 * 1024
            )));
        }

        // Read entry from file with retry logic
        // Distinguish between transient errors (file being written) and permanent errors (corruption)
        let mut attempts = 0;
        let max_attempts = self.config.file_retry_attempts;
        let base_retry_delay = Duration::from_millis(self.config.retry_delay_ms / 2);

        loop {
            match self.try_read_entry(offset).await {
                Ok(entry) => {
                    // Manually decrement reader count before returning on success
                    guard.decrement();
                    if entry.deleted {
                        return Ok(None);
                    } else {
                        return Ok(Some(entry.value));
                    }
                }
                Err(e) => {
                    attempts += 1;

                    // Check if this is a permanent error (corruption) - don't retry
                    let is_permanent =
                        matches!(&e, LsmError::Corruption(_) | LsmError::Serialization(_));

                    if is_permanent || attempts >= max_attempts {
                        // Guard will decrement reader count on error return via Drop
                        if is_permanent {
                            error!("Permanent error reading entry (no retry): {e}");
                        } else {
                            error!("Failed to read entry after {attempts} attempts: {e}");
                        }
                        return Err(e);
                    }

                    // Check if this is a transient error (EOF, incomplete file)
                    let is_transient = match &e {
                        LsmError::Io(io_err) => {
                            io_err.kind() == std::io::ErrorKind::UnexpectedEof
                                || io_err.to_string().contains("EOF")
                                || io_err.to_string().contains("incomplete")
                        }
                        _ => false,
                    };

                    if is_transient {
                        // Use exponential backoff for transient errors
                        let delay = base_retry_delay * (1 << (attempts - 1).min(5)); // Cap at 32x base delay
                        warn!("Transient error reading entry (attempt {attempts}/{max_attempts}), retrying after {:?}: {e}", delay);
                        sleep(delay).await;
                    } else {
                        // For other errors, use fixed delay
                        warn!("Error reading entry (attempt {attempts}/{max_attempts}), retrying: {e}");
                        sleep(base_retry_delay).await;
                    }
                }
            }
        }
    }

    /// Try to read an entry from file with checksum validation
    async fn try_read_entry(&self, offset: u64) -> Result<SSTableEntry> {
        // CRITICAL: Check if SSTable is ready before reading
        // This prevents reads from happening before metadata/index are fully loaded
        if !self.is_ready.load(std::sync::atomic::Ordering::Acquire) {
            return Err(LsmError::Internal(format!(
                "Cannot read entry from SSTable that is not ready: {:?}",
                self.path
            )));
        }

        // Check if SSTable is marked for deletion - don't read from deleted SSTables
        if self.is_marked_for_deletion() {
            return Err(LsmError::Internal(format!(
                "Cannot read from SSTable marked for deletion: {:?}",
                self.path
            )));
        }

        // Acquire write lock to get exclusive access to file handle during read
        // This prevents concurrent reads from interfering with each other's seek positions
        let mut file_guard = self.file.write().await;
        let file = file_guard
            .as_mut()
            .ok_or_else(|| LsmError::Internal("File not open".to_string()))?;

        // Double-check that SSTable is still ready after getting file handle
        // This prevents race conditions where the SSTable becomes not ready between check and file access
        if !self.is_ready.load(std::sync::atomic::Ordering::Acquire) {
            return Err(LsmError::Internal(format!(
                "SSTable became not ready during read: {:?}",
                self.path
            )));
        }

        // Double-check that SSTable is still not marked for deletion after getting file handle
        // This prevents race conditions where deletion happens between check and file access
        if self.is_marked_for_deletion() {
            return Err(LsmError::Internal(format!(
                "SSTable was marked for deletion during read: {:?}",
                self.path
            )));
        }

        // Validate file size before reading
        let file_size = file.metadata().await.map_err(LsmError::Io)?.len();

        // Validate file size matches expected metadata (with tolerance for metadata/index/bloom)
        // The file should be at least as large as the metadata indicates
        let expected_min_size = self.metadata.file_size;
        if file_size < expected_min_size {
            return Err(LsmError::Corruption(format!(
                "File size {} is smaller than expected minimum {} for SSTable {:?}. File may be incomplete or corrupted.",
                file_size, expected_min_size, self.path
            )));
        }

        // CRITICAL: Validate that offset is within the data section, not in index/metadata section
        // The data section ends at index_offset, so offsets must be < index_offset
        // Only validate if index_offset has been set (non-zero), as it's 0 for uninitialized metadata
        if self.metadata.index_offset > 0 && offset >= self.metadata.index_offset {
            return Err(LsmError::Corruption(format!(
                "Read offset {} is in index/metadata section (index starts at {}). \
                This indicates index corruption or reading from wrong offset. SSTable: {:?}",
                offset, self.metadata.index_offset, self.path
            )));
        }

        // Check if offset is within file bounds
        if offset >= file_size {
            return Err(LsmError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "Read offset {} exceeds file size {}. File may be incomplete.",
                    offset, file_size
                ),
            )));
        }

        // Minimum size needed: 4 bytes (entry_size) + 4 bytes (checksum)
        if offset + 8 > file_size {
            return Err(LsmError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "Insufficient data at offset {}: need at least 8 bytes, file has {} bytes remaining. File may be incomplete.",
                    offset, file_size.saturating_sub(offset)
                ),
            )));
        }

        file.seek(tokio::io::SeekFrom::Start(offset))
            .await
            .map_err(LsmError::Io)?;

        // Read entry size
        let entry_size = match file.read_u32_le().await {
            Ok(size) => size,
            Err(e) => {
                // Check if this is an EOF error
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return Err(LsmError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        format!(
                            "Early EOF while reading entry size at offset {}. File may be incomplete (file size: {}).",
                            offset, file_size
                        ),
                    )));
                }
                return Err(LsmError::Io(e));
            }
        };

        // Validate entry size is reasonable (not too large)
        // Unreasonably large sizes often indicate reading from wrong offset or corrupted data
        if entry_size > 100 * 1024 * 1024 {
            // 100MB max entry size
            // This is likely corruption or reading from wrong offset, not a transient error
            return Err(LsmError::Corruption(format!(
                "Entry size {} at offset {} is unreasonably large (max: {}). \
                This may indicate file corruption, reading from wrong offset, or race condition. \
                SSTable: {:?}, file_size: {}",
                entry_size,
                offset,
                100 * 1024 * 1024,
                self.path,
                file_size
            )));
        }

        // Validate entry size is not zero (would indicate corruption)
        if entry_size == 0 {
            return Err(LsmError::Corruption(format!(
                "Entry size is zero at offset {} in SSTable {:?}. This indicates corruption.",
                offset, self.path
            )));
        }

        // Check if we have enough data for entry + checksum
        let required_size = offset + 4 + entry_size as u64 + 4;
        if required_size > file_size {
            return Err(LsmError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "Insufficient data for entry at offset {}: need {} bytes, file has {} bytes. File may be incomplete.",
                    offset, required_size, file_size
                ),
            )));
        }

        // Read entry data
        let mut entry_buffer = vec![0u8; entry_size as usize];
        match file.read_exact(&mut entry_buffer).await {
            Ok(_) => {}
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return Err(LsmError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        format!(
                            "Early EOF while reading entry data at offset {} (size: {}). File may be incomplete (file size: {}).",
                            offset, entry_size, file_size
                        ),
                    )));
                }
                return Err(LsmError::Io(e));
            }
        }

        // Read stored checksum
        let stored_checksum = match file.read_u32_le().await {
            Ok(checksum) => checksum,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return Err(LsmError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        format!(
                            "Early EOF while reading checksum at offset {} (entry size: {}). File may be incomplete (file size: {}).",
                            offset, entry_size, file_size
                        ),
                    )));
                }
                return Err(LsmError::Io(e));
            }
        };

        // Compute checksum of read data
        let mut entry_hasher = Crc32Hasher::new();
        entry_hasher.update(&entry_size.to_le_bytes());
        entry_hasher.update(&entry_buffer);
        let computed_checksum = entry_hasher.finalize();

        // Validate checksum
        if stored_checksum != computed_checksum {
            // Checksum mismatch can indicate:
            // 1. File corruption (permanent error)
            // 2. Reading from wrong offset (corruption or race condition)
            // 3. File being written while reading (transient, but should be prevented)
            // 4. File handle pointing to wrong file (should not happen)

            // Check if this might be a race condition (file being written)
            // If the file size changed between metadata check and read, it might be a race
            let current_file_size = file.metadata().await.map_err(LsmError::Io)?.len();
            if current_file_size != file_size {
                // File size changed - this is a race condition, treat as transient
                return Err(LsmError::Io(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    format!(
                        "File size changed during read ({} -> {}), possible race condition. Retry may succeed.",
                        file_size, current_file_size
                    ),
                )));
            }

            // Otherwise, this is likely corruption
            return Err(LsmError::Corruption(format!(
                "SSTable entry checksum mismatch at offset {}: stored={}, computed={}. \
                Data may be corrupted. SSTable: {:?}, file_size: {}, entry_size: {}",
                offset, stored_checksum, computed_checksum, self.path, file_size, entry_size
            )));
        }

        // Deserialize entry
        let entry: SSTableEntry = bincode::deserialize(&entry_buffer)
            .map_err(|e| LsmError::Serialization(format!("Failed to deserialize entry: {}", e)))?;

        Ok(entry)
    }

    /// Mark SSTable for deletion (will be deleted when all readers done)
    pub fn mark_for_deletion(&self) {
        self.marked_for_deletion
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Check if marked for deletion
    pub fn is_marked_for_deletion(&self) -> bool {
        self.marked_for_deletion
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Get current reader count
    pub fn reader_count(&self) -> usize {
        self.reader_count.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Check if safe to delete (no active readers)
    pub fn can_delete(&self) -> bool {
        self.reader_count.load(std::sync::atomic::Ordering::Acquire) == 0
    }

    /// Wait for all readers to complete (with timeout)
    pub async fn wait_for_readers(&self, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        while self.reader_count.load(std::sync::atomic::Ordering::Acquire) > 0 {
            if start.elapsed() > timeout {
                return false; // Timeout
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        true // All readers done
    }

    /// Scan keys with a prefix
    pub async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();

        for (key, _) in self.index.range(prefix.to_string()..) {
            if !key.starts_with(prefix) {
                break;
            }
            keys.push(key.clone());
        }

        Ok(keys)
    }

    /// Scan keys in a range
    pub async fn scan_range(&self, start: &str, end: &str) -> Result<Vec<String>> {
        let entries = self.scan_range_layer(start, end).await?;
        Ok(entries
            .into_iter()
            .filter(|(_, _, deleted)| !deleted)
            .map(|(key, _, _)| key)
            .collect())
    }

    /// Scan prefix entries with values (includes tombstones for layer merge).
    pub async fn scan_prefix_layer(&self, prefix: &str) -> Result<Vec<(String, Value, bool)>> {
        if !self.is_ready.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(Vec::new());
        }
        self.ensure_file_open().await?;

        let mut entries = Vec::new();
        for (key, (offset, _)) in self.index.range(prefix.to_string()..) {
            if !key.starts_with(prefix) {
                break;
            }
            if let Ok(entry) = self.try_read_entry(*offset).await {
                entries.push((key.clone(), entry.value, entry.deleted));
            }
        }
        Ok(entries)
    }

    /// Scan range entries with values (includes tombstones for layer merge).
    pub async fn scan_range_layer(
        &self,
        start: &str,
        end: &str,
    ) -> Result<Vec<(String, Value, bool)>> {
        if !self.is_ready.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(Vec::new());
        }
        self.ensure_file_open().await?;

        let mut entries = Vec::new();
        let end_bound = crate::utils::exclusive_range_end(end);

        if let Some(end_bound) = end_bound {
            for (key, (offset, _)) in self.index.range(start.to_string()..end_bound) {
                if let Ok(entry) = self.try_read_entry(*offset).await {
                    entries.push((key.clone(), entry.value, entry.deleted));
                }
            }
        } else {
            for (key, (offset, _)) in self.index.range(start.to_string()..) {
                if let Ok(entry) = self.try_read_entry(*offset).await {
                    entries.push((key.clone(), entry.value, entry.deleted));
                }
            }
        }
        Ok(entries)
    }

    /// Scan all entries in the SSTable
    pub async fn scan_all(&self) -> Result<Vec<(String, Value, bool)>> {
        let mut entries = Vec::new();

        for (key, (offset, _)) in &self.index {
            if let Ok(entry) = self.try_read_entry(*offset).await {
                entries.push((key.clone(), entry.value, entry.deleted));
            }
        }

        Ok(entries)
    }

    /// Get all entries from the SSTable with full metadata (including timestamps)
    /// This is useful for compaction operations that need complete entry information
    pub async fn get_all_entries(&self) -> Result<Vec<SSTableEntry>> {
        // CRITICAL: Check if SSTable is ready before reading all entries
        if !self.is_ready.load(std::sync::atomic::Ordering::Acquire) {
            return Err(LsmError::Internal(format!(
                "Cannot read all entries from SSTable that is not ready: {:?}",
                self.path
            )));
        }

        // Check if SSTable is marked for deletion before reading all entries
        if self.is_marked_for_deletion() {
            return Err(LsmError::Internal(format!(
                "Cannot read all entries from SSTable marked for deletion: {:?}",
                self.path
            )));
        }

        let mut entries = Vec::with_capacity(self.index.len());

        // Iterate over index - this is safe as the index is only modified during SSTable creation
        for (key, (offset, size)) in &self.index {
            // Double-check deletion status before each read
            if self.is_marked_for_deletion() {
                warn!(
                    "SSTable {:?} was marked for deletion during get_all_entries, stopping read",
                    self.path
                );
                // Return partial results rather than error - compaction can use what it has
                break;
            }

            // Validate offset and size before reading
            if *offset > self.metadata.file_size {
                warn!(
                    "Invalid offset {} for key '{}' (exceeds file size {}), skipping",
                    offset, key, self.metadata.file_size
                );
                continue;
            }

            if *size == 0 || *size > 100 * 1024 * 1024 {
                warn!("Invalid entry size {} for key '{}', skipping", size, key);
                continue;
            }

            match self.try_read_entry(*offset).await {
                Ok(entry) => {
                    // Verify the key matches (sanity check)
                    if entry.key == *key {
                        entries.push(entry);
                    } else {
                        warn!(
                            "Key mismatch in SSTable entry: index key '{}' != entry key '{}'",
                            key, entry.key
                        );
                        // Still add the entry, but log the mismatch
                        entries.push(entry);
                    }
                }
                Err(e) => {
                    // Check if this is a deletion-related error - if so, stop reading
                    if self.is_marked_for_deletion() {
                        warn!(
                            "SSTable {:?} marked for deletion during get_all_entries, stopping read",
                            self.path
                        );
                        break;
                    }

                    warn!(
                        "Failed to read entry for key '{}' at offset {}: {}. Skipping entry.",
                        key, offset, e
                    );
                    // Continue with other entries rather than failing completely
                }
            }
        }

        Ok(entries)
    }

    /// Get metadata
    pub fn metadata(&self) -> &SSTableMetadata {
        &self.metadata
    }

    /// Get file path
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Check if SSTable is open
    pub fn is_open(&self) -> bool {
        // Use try_read to avoid blocking - if we can't get the lock, assume it's open
        match self.file.try_read() {
            Ok(guard) => guard.is_some(),
            Err(_) => true, // Lock held means file is being accessed, so it's open
        }
    }

    /// Check if SSTable is ready for reads
    /// A ready SSTable has been fully written, synced, and has its metadata/index loaded
    pub fn is_ready(&self) -> bool {
        self.is_ready.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Get the number of entries in the index
    /// This is useful for validation that the index was loaded correctly
    pub fn index_size(&self) -> usize {
        self.index.len()
    }

    /// Close SSTable file handle
    pub async fn close(&mut self) -> Result<()> {
        let mut file_guard = self.file.write().await;
        *file_guard = None;
        Ok(())
    }

    /// Close file handle if open (non-mutable version for LRU eviction)
    pub fn close_file(&self) {
        // This is a bit of a hack - we need mutable access to close
        // In practice, this will be called from the engine which has mutable access
        // For now, we'll rely on the engine managing this properly
    }

    /// Get the size of the SSTable
    pub fn size(&self) -> u64 {
        self.metadata.file_size
    }

    /// Clone the SSTable (for testing purposes)
    pub fn clone_for_testing(&self) -> Self {
        Self {
            path: self.path.clone(),
            config: self.config.clone(),
            metadata: self.metadata.clone(),
            index: self.index.clone(),
            bloom_filter: self.bloom_filter.clone(),
            file: tokio::sync::RwLock::new(None), // Don't clone file handle
            last_access: std::sync::atomic::AtomicU64::new(
                self.last_access.load(std::sync::atomic::Ordering::Relaxed),
            ),
            reader_count: std::sync::atomic::AtomicUsize::new(0),
            marked_for_deletion: std::sync::atomic::AtomicBool::new(false),
            is_ready: std::sync::atomic::AtomicBool::new(
                self.is_ready.load(std::sync::atomic::Ordering::Relaxed),
            ),
        }
    }
}

impl Clone for SSTable {
    fn clone(&self) -> Self {
        self.clone_for_testing()
    }
}
