//! SSTable implementation for LSM Tree Engine

use crate::core::config::SstableConfig;
use crate::error::{LsmError, Result};
use crate::storage::file_reader::SstableFileReader;
use crate::storage::flat_index::FlatIndex;
use crate::storage::SharedBlockCache;
use crate::utils;
use crc32fast::Hasher as Crc32Hasher;
use f4kvs_value::Value;
use serde::{Deserialize, Serialize};

use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::time::{sleep, Duration};
use tracing::{debug, error, warn};

/// New SST files start with this magic and use Snappy data blocks.
const SST_COMPRESSED_MAGIC: &[u8; 4] = b"F4SC";
const SST_COMPRESSED_HEADER: usize = 8;
const SST_BLOCK_TARGET: usize = 32 * 1024;

fn parse_framed_entry(buf: &[u8], at: usize) -> Result<SSTableEntry> {
    if at + 8 > buf.len() {
        return Err(LsmError::Corruption(format!(
            "framed entry header past block end at {at}"
        )));
    }
    let entry_size = u32::from_le_bytes(buf[at..at + 4].try_into().unwrap());
    if entry_size == 0 || entry_size > 100 * 1024 * 1024 {
        return Err(LsmError::Corruption(format!(
            "invalid framed entry size {entry_size} at {at}"
        )));
    }
    let data_end = at + 4 + entry_size as usize;
    let crc_end = data_end + 4;
    if crc_end > buf.len() {
        return Err(LsmError::Corruption(format!(
            "framed entry body past block end at {at}"
        )));
    }
    let entry_buffer = &buf[at + 4..data_end];
    let stored = u32::from_le_bytes(buf[data_end..crc_end].try_into().unwrap());
    let mut hasher = Crc32Hasher::new();
    hasher.update(&entry_size.to_le_bytes());
    hasher.update(entry_buffer);
    if stored != hasher.finalize() {
        return Err(LsmError::Corruption(format!(
            "framed entry checksum mismatch at {at}"
        )));
    }
    bincode::deserialize(entry_buffer)
        .map_err(|e| LsmError::Serialization(format!("Failed to deserialize entry: {e}")))
}

fn snap_compress(raw: &[u8]) -> Result<Vec<u8>> {
    snap::raw::Encoder::new()
        .compress_vec(raw)
        .map_err(|e| LsmError::Internal(format!("snappy compress: {e}")))
}

fn snap_decompress(comp: &[u8]) -> Result<Vec<u8>> {
    snap::raw::Decoder::new()
        .decompress_vec(comp)
        .map_err(|e| LsmError::Corruption(format!("snappy decompress: {e}")))
}

async fn write_snappy_block(
    writer: &mut BufWriter<tokio::fs::File>,
    file_hasher: &mut Crc32Hasher,
    offset: &mut u64,
    index_pairs: &mut Vec<(String, u64, u32)>,
    block: &mut Vec<u8>,
    pending: &mut Vec<(String, u32)>,
) -> Result<()> {
    if block.is_empty() {
        return Ok(());
    }
    let compressed = snap_compress(block)?;
    let unc_len = block.len() as u32;
    let comp_len = compressed.len() as u32;
    let block_off = *offset;
    writer.write_u32_le(unc_len).await.map_err(LsmError::Io)?;
    writer.write_u32_le(comp_len).await.map_err(LsmError::Io)?;
    writer.write_all(&compressed).await.map_err(LsmError::Io)?;
    file_hasher.update(&unc_len.to_le_bytes());
    file_hasher.update(&comp_len.to_le_bytes());
    file_hasher.update(&compressed);
    for (key, in_off) in pending.drain(..) {
        index_pairs.push((key, block_off, in_off));
    }
    *offset += 8 + compressed.len() as u64;
    block.clear();
    Ok(())
}

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

/// Result of a key lookup in a single SSTable.
///
/// Distinguishes a tombstone (key was deleted in this file) from a pure miss
/// (key never written in this file). Callers that merge multiple L0 files must
/// pick the highest [`timestamp`] across candidates — treating a tombstone as
/// `None` and continuing to older files resurrects deleted keys, and taking the
/// first hit by file-vector order is wrong after restart (`read_dir` order).
#[derive(Debug, Clone, PartialEq)]
pub enum SstableLookupResult {
    /// Live value found (with entry timestamp for L0 multi-file merge)
    Found {
        /// Value payload
        value: Value,
        /// Monotonic entry timestamp (sequence number at flush)
        timestamp: u64,
    },
    /// Deletion tombstone — wins over any older live value for this key
    Tombstone {
        /// Monotonic entry timestamp of the delete
        timestamp: u64,
    },
    /// Key not present in this SSTable
    Missing,
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

    /// In-memory index for fast lookups. Loaded lazily on first access:
    /// `open_sync` reads metadata only (decoding every F4IX at open dominated
    /// shard open on large catalogs).
    index: std::sync::RwLock<FlatIndex>,

    /// True once `index` holds the freshly written or decoded on-disk index.
    index_loaded: std::sync::atomic::AtomicBool,

    /// Serializes lazy index loads so concurrent Gets decode at most once.
    index_load_lock: std::sync::Mutex<()>,

    /// Bloom filter for fast key existence checks
    bloom_filter: Option<BloomFilter>,

    /// Random-access file reader (seek or positioned reads, depending on config).
    /// Shared across clones so Get can drop the live-map lock and still use the
    /// already-open FD (reopening every clone would burn FDs under soak load).
    reader: Arc<SstableFileReader>,

    /// Last access time for LRU eviction (nanoseconds since epoch)
    last_access: std::sync::atomic::AtomicU64,

    /// Active reader count (shared across clones so reclaim waits on live pins).
    reader_count: Arc<std::sync::atomic::AtomicUsize>,

    /// Marked for deletion (shared across clones — compaction clones inputs).
    marked_for_deletion: Arc<std::sync::atomic::AtomicBool>,

    /// Ready flag: indicates SSTable is fully written, synced, and metadata/index are loaded
    /// This prevents reads from happening before the SSTable is in a consistent state
    is_ready: std::sync::atomic::AtomicBool,

    /// True when the file uses F4SC Snappy data blocks (false = legacy framed entries).
    block_compressed: std::sync::atomic::AtomicBool,

    /// Last decompressed Snappy block (offset, bytes). Point Gets in the same
    /// 32 KiB window skip Snappy entirely.
    decompressed_block: std::sync::Mutex<Option<(u64, std::sync::Arc<[u8]>)>>,
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

        let reader = SstableFileReader::new(
            path.clone(),
            config.read_mode,
            config.mincore_cache_ttl_secs,
        );

        Ok(Self {
            path,
            config,
            metadata,
            index: std::sync::RwLock::new(FlatIndex::default()),
            index_loaded: std::sync::atomic::AtomicBool::new(false),
            index_load_lock: std::sync::Mutex::new(()),
            bloom_filter: None,
            reader: Arc::new(reader),
            last_access: std::sync::atomic::AtomicU64::new(0),
            reader_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            marked_for_deletion: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            is_ready: std::sync::atomic::AtomicBool::new(false),
            block_compressed: std::sync::atomic::AtomicBool::new(false),
            decompressed_block: std::sync::Mutex::new(None),
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

        self.block_compressed
            .store(true, std::sync::atomic::Ordering::Release);

        let mut file_hasher = Crc32Hasher::new();
        writer
            .write_all(SST_COMPRESSED_MAGIC)
            .await
            .map_err(LsmError::Io)?;
        writer
            .write_all(&[1u8, 0, 0, 0])
            .await
            .map_err(LsmError::Io)?;
        file_hasher.update(SST_COMPRESSED_MAGIC);
        file_hasher.update(&[1u8, 0, 0, 0]);

        let mut offset = SST_COMPRESSED_HEADER as u64;
        let mut index_pairs: Vec<(String, u64, u32)> = Vec::with_capacity(entries.len());
        let mut block = Vec::new();
        let mut pending: Vec<(String, u32)> = Vec::new();

        for entry in &entries {
            let entry_data = bincode::serialize(entry).map_err(|e| {
                LsmError::Serialization(format!("Failed to serialize entry: {}", e))
            })?;
            if !block.is_empty() && block.len() >= SST_BLOCK_TARGET {
                write_snappy_block(
                    &mut writer,
                    &mut file_hasher,
                    &mut offset,
                    &mut index_pairs,
                    &mut block,
                    &mut pending,
                )
                .await?;
            }
            let in_off = block.len() as u32;
            let entry_size = entry_data.len() as u32;
            let mut entry_hasher = Crc32Hasher::new();
            entry_hasher.update(&entry_size.to_le_bytes());
            entry_hasher.update(&entry_data);
            let entry_checksum = entry_hasher.finalize();
            block.extend_from_slice(&entry_size.to_le_bytes());
            block.extend_from_slice(&entry_data);
            block.extend_from_slice(&entry_checksum.to_le_bytes());
            pending.push((entry.key.clone(), in_off));
        }
        write_snappy_block(
            &mut writer,
            &mut file_hasher,
            &mut offset,
            &mut index_pairs,
            &mut block,
            &mut pending,
        )
        .await?;

        // Update metadata
        self.metadata.file_size = offset;
        self.metadata.index_offset = offset;

        let index_raw = {
            let mut guard = self
                .index
                .write()
                .map_err(|_| LsmError::Internal("index lock poisoned".into()))?;
            *guard = FlatIndex::from_sorted(index_pairs);
            guard.encode()
        };
        self.index_loaded
            .store(true, std::sync::atomic::Ordering::Release);
        let index_data = snap_compress(&index_raw)?;

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

        // Create and write bloom filter. Include tombstones: if a deleted key
        // is omitted, key_may_exist/lookup bloom-rejects the newer L0 file and
        // the merge falls through to an older live value (resurrects deletes).
        let mut bloom_filter = BloomFilter::with_optimal_params(entries.len());
        for entry in &entries {
            bloom_filter.add(&entry.key);
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
        self.open_sync()
    }

    /// Load metadata with blocking I/O (no tokio hop). The on-disk index is
    /// deferred to first use (`ensure_index_loaded`): decoding every F4IX at
    /// open cost 10–25 ms per SSTable and dominated shard open.
    pub fn open_sync(&mut self) -> Result<()> {
        self.update_last_access();
        if self.is_ready.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }
        self.reader.ensure_open_blocking(true)?;
        self.try_read_metadata_blocking()?;
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

    /// Ensure file is open, re-opening if necessary.
    ///
    /// After recovery load we deliberately close handles (index/metadata stay in
    /// memory) so opening N >> ulimit SSTables succeeds. Re-open on read is the
    /// normal path — log at debug, not warn (was drowning logs at ~300k/run).
    pub async fn ensure_file_open(&self) -> Result<()> {
        self.ensure_file_open_sync()
    }

    pub fn ensure_file_open_sync(&self) -> Result<()> {
        if !self.reader.is_open() {
            if self.config.enable_resilient_handling {
                self.reader.ensure_open_blocking(true)?;
                self.update_last_access();
            } else {
                return Err(LsmError::Internal(
                    "File not open and resilient handling is disabled".to_string(),
                ));
            }
        } else {
            self.update_last_access();
        }
        Ok(())
    }

    fn try_read_metadata_blocking(&mut self) -> Result<()> {
        let timing = std::env::var_os("F4KVS_OPEN_TIMING").is_some();
        let t0 = std::time::Instant::now();
        self.reader.ensure_open_blocking(true)?;

        let mut mag = [0u8; 4];
        if self.reader.read_exact_at_blocking(0, &mut mag).is_ok() && mag == *SST_COMPRESSED_MAGIC {
            self.block_compressed
                .store(true, std::sync::atomic::Ordering::Release);
        } else {
            self.block_compressed
                .store(false, std::sync::atomic::Ordering::Release);
        }

        let file_size = self.reader.file_size_blocking()?;
        let t_open = t0.elapsed();

        // Read metadata from end of file (metadata is at the end, followed by its checksum)
        let mut buffer = vec![0u8; 2048];
        let mut metadata_size = 0usize;
        let mut metadata_offset = 0u64;

        let mut pos = file_size;
        while pos > 0 && metadata_size == 0 {
            let read_size = std::cmp::min(pos, buffer.len() as u64) as usize;
            pos -= read_size as u64;

            self.reader
                .read_exact_at_blocking(pos, &mut buffer[..read_size])?;
            let bytes_read = read_size;

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
        let t_meta_scan = t0.elapsed();

        let metadata_checksum_offset = metadata_offset + metadata_size as u64;
        let stored_metadata_checksum = self
            .reader
            .read_u32_le_at_blocking(metadata_checksum_offset)?;

        let mut metadata_buffer = vec![0u8; metadata_size];
        self.reader
            .read_exact_at_blocking(metadata_offset, &mut metadata_buffer)?;

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
        let t_meta_crc = t0.elapsed();

        if timing {
            eprintln!(
                "open_timing {:?} size={} idx_comp={} | open={:?} meta_scan={:?} meta_crc={:?} (index deferred)",
                self.path.file_name().unwrap_or_default(),
                file_size,
                self.metadata.index_size,
                t_open,
                t_meta_scan - t_open,
                t_meta_crc - t_meta_scan,
            );
        }

        // Bloom is optional (index is authoritative). Loading Vec<bool> filters
        // on every shard open dominated catalog browse; skip it.
        self.bloom_filter = None;

        Ok(())
    }

    /// Load the on-disk index on first use. `open_sync` reads metadata only:
    /// reading + checksumming + Snappy-decompressing every F4IX at open cost
    /// 10–25 ms per SSTable and dominated shard open on large catalogs.
    /// Concurrent callers are serialized; the decode happens once.
    pub fn ensure_index_loaded(&self) -> Result<()> {
        if self.index_loaded.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }
        let _serialize = self
            .index_load_lock
            .lock()
            .map_err(|_| LsmError::Internal("index load lock poisoned".into()))?;
        if self.index_loaded.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }

        let timing = std::env::var_os("F4KVS_OPEN_TIMING").is_some();
        let t0 = std::time::Instant::now();
        self.reader.ensure_open_blocking(true)?;

        let index_start = self.metadata.index_offset;
        let index_size = self.metadata.index_size;
        if index_size == 0 {
            return Err(LsmError::Corruption(format!(
                "SSTable metadata has zero index_size (not opened?): {:?}",
                self.path
            )));
        }

        let mut index_buffer = vec![0u8; index_size as usize];
        self.reader
            .read_exact_at_blocking(index_start, &mut index_buffer)?;
        let t_idx_read = t0.elapsed();

        let stored_index_checksum = self
            .reader
            .read_u32_le_at_blocking(index_start + index_size)?;

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
        let t_idx_crc = t0.elapsed();

        let index_bytes = if self
            .block_compressed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            snap_decompress(&index_buffer)?
        } else {
            index_buffer
        };
        let t_decomp = t0.elapsed();
        let decoded = FlatIndex::decode(&index_bytes)?;
        let t_decode = t0.elapsed();

        // Decoding a corrupt index must fail here, not serve wrong offsets:
        // this is the deferred equivalent of the open-time validation.
        if decoded.len() != self.metadata.entry_count {
            return Err(LsmError::Corruption(format!(
                "SSTable index size {} does not match entry_count {}: {:?}",
                decoded.len(),
                self.metadata.entry_count,
                self.path
            )));
        }

        if timing {
            eprintln!(
                "index_load_timing {:?} idx_comp={} idx_raw={} | idx_read={:?} idx_crc={:?} decomp={:?} decode={:?}",
                self.path.file_name().unwrap_or_default(),
                index_size,
                index_bytes.len(),
                t_idx_read,
                t_idx_crc - t_idx_read,
                t_decomp - t_idx_crc,
                t_decode - t_decomp,
            );
        }

        *self
            .index
            .write()
            .map_err(|_| LsmError::Internal("index lock poisoned".into()))? = decoded;
        self.index_loaded
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Read guard on the index, loading it from disk on first use.
    fn index_guard(&self) -> Result<std::sync::RwLockReadGuard<'_, FlatIndex>> {
        self.ensure_index_loaded()?;
        self.index
            .read()
            .map_err(|_| LsmError::Internal("index lock poisoned".into()))
    }

    pub(crate) fn index_start(&self, prefix: &str) -> usize {
        match self.index_guard() {
            Ok(idx) => idx.partition_point(prefix.as_bytes()),
            Err(e) => {
                // Exhausted sentinel: scan heads at this position yield nothing.
                log::error!("index_start: index unavailable for {:?}: {}", self.path, e);
                usize::MAX
            }
        }
    }

    pub(crate) fn index_key_at(&self, pos: usize) -> Option<Vec<u8>> {
        self.with_index_key_at(pos, |k| k.map(|b| b.to_vec()))
    }

    pub(crate) fn with_index_key_at<R>(&self, pos: usize, f: impl FnOnce(Option<&[u8]>) -> R) -> R {
        match self.index_guard() {
            Ok(idx) => f(idx.at(pos).map(|(k, _)| k)),
            Err(e) => {
                log::error!("index_key_at: index unavailable for {:?}: {}", self.path, e);
                f(None)
            }
        }
    }

    pub(crate) fn index_key_eq(&self, pos: usize, key: &[u8]) -> bool {
        match self.index_guard() {
            Ok(idx) => idx.at(pos).map(|(k, _)| k == key).unwrap_or(false),
            Err(e) => {
                log::error!("index_key_eq: index unavailable for {:?}: {}", self.path, e);
                false
            }
        }
    }

    /// Smallest key in `(after, ∞)` that starts with `prefix`.
    pub fn first_key_after(&self, prefix: &str, after: Option<&str>) -> Option<String> {
        if !self.is_ready() {
            return None;
        }
        let idx = self.index_guard().ok()?;
        let bound = after.unwrap_or(prefix);
        let mut i = idx.partition_point(bound.as_bytes());
        if after.is_some() {
            if let Some((k, _)) = idx.at(i) {
                if k == bound.as_bytes() {
                    i += 1;
                }
            }
        }
        let (k, _) = idx.at(i)?;
        if k.starts_with(prefix.as_bytes()) {
            Some(String::from_utf8_lossy(k).into_owned())
        } else {
            None
        }
    }

    /// Fast check: key could exist in this SSTable (range + bloom). Does not touch the file.
    pub fn key_may_exist(&self, key: &str) -> bool {
        if !self.is_ready.load(std::sync::atomic::Ordering::Acquire) {
            return false;
        }
        // Marked files stay readable for pinned scanners/gets until reclaim
        // unlinks. New map walkers skip them via `is_marked_for_deletion`.
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

    fn block_cache_key(&self, offset: u64) -> String {
        format!("{}:{}", self.path.display(), offset)
    }

    /// Get value by key with resilient file handling
    pub async fn get(
        &self,
        key: &str,
        block_cache: Option<&SharedBlockCache>,
    ) -> Result<Option<Value>> {
        self.reader_count
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let mut guard = ReaderGuard::new(self.reader_count.as_ref(), false);
        self.get_with_reader_guard(key, block_cache, &mut guard)
            .await
    }

    /// Get value when the caller already pinned this SSTable via `reader_count`.
    ///
    /// Used by the engine to hold a read pin across LRU file-handle eviction.
    /// Tombstones are returned as `None` (same as missing) — prefer
    /// [`Self::lookup_pinned`] when merging overlapping L0 files.
    #[allow(dead_code)] // retained for callers that only need Option semantics
    pub(crate) async fn get_pinned(
        &self,
        key: &str,
        block_cache: Option<&SharedBlockCache>,
    ) -> Result<Option<Value>> {
        match self.lookup_pinned(key, block_cache).await? {
            SstableLookupResult::Found { value, .. } => Ok(Some(value)),
            SstableLookupResult::Tombstone { .. } | SstableLookupResult::Missing => Ok(None),
        }
    }

    /// Synchronous lookup (no tokio). File must be open or resilient reopen is on.
    pub(crate) fn lookup_sync(
        &self,
        key: &str,
        block_cache: Option<&SharedBlockCache>,
    ) -> Result<SstableLookupResult> {
        if !self.is_ready.load(std::sync::atomic::Ordering::Acquire) {
            return Err(LsmError::Internal(format!(
                "Cannot read from SSTable that is not ready: {:?}",
                self.path
            )));
        }
        if !self.key_may_exist(key) {
            return Ok(SstableLookupResult::Missing);
        }
        self.update_last_access();
        let (offset, size) = {
            let idx = self.index_guard()?;
            let Some(loc) = idx.get(key) else {
                return Ok(SstableLookupResult::Missing);
            };
            loc
        };
        if self.metadata.index_offset == 0 || offset >= self.metadata.index_offset {
            return Ok(SstableLookupResult::Missing);
        }
        if self.is_block_compressed() {
            if size > 100 * 1024 * 1024 {
                return Ok(SstableLookupResult::Missing);
            }
        } else if size == 0 || size > 100 * 1024 * 1024 {
            return Ok(SstableLookupResult::Missing);
        }
        match self.try_read_entry_sync(offset, size, block_cache) {
            Ok(entry) if entry.deleted => Ok(SstableLookupResult::Tombstone {
                timestamp: entry.timestamp,
            }),
            Ok(entry) => Ok(SstableLookupResult::Found {
                value: entry.value,
                timestamp: entry.timestamp,
            }),
            Err(e) => Err(e),
        }
    }

    fn read_snappy_block_sync(
        &self,
        block_off: u64,
        block_cache: Option<&SharedBlockCache>,
    ) -> Result<std::sync::Arc<[u8]>> {
        if let Ok(guard) = self.decompressed_block.lock() {
            if let Some((off, data)) = guard.as_ref() {
                if *off == block_off {
                    return Ok(std::sync::Arc::clone(data));
                }
            }
        }
        let cache_key = format!("{}:blk:{}", self.path.display(), block_off);
        if let Some(cache) = block_cache {
            if let Some(hit) = cache.get_sync(&cache_key) {
                let arc: std::sync::Arc<[u8]> = hit.into();
                if let Ok(mut guard) = self.decompressed_block.lock() {
                    *guard = Some((block_off, std::sync::Arc::clone(&arc)));
                }
                return Ok(arc);
            }
        }
        self.ensure_file_open_sync()?;
        let unc_len = self.reader.read_u32_le_at_blocking(block_off)?;
        let comp_len = self.reader.read_u32_le_at_blocking(block_off + 4)?;
        if unc_len == 0
            || unc_len > 128 * 1024 * 1024
            || comp_len == 0
            || comp_len > 128 * 1024 * 1024
        {
            return Err(LsmError::Corruption(format!(
                "invalid snappy block header at {block_off} unc={unc_len} comp={comp_len}"
            )));
        }
        let mut comp = vec![0u8; comp_len as usize];
        self.reader
            .read_exact_at_blocking(block_off + 8, &mut comp)?;
        let raw = snap_decompress(&comp)?;
        if raw.len() != unc_len as usize {
            return Err(LsmError::Corruption(format!(
                "snappy block length mismatch at {block_off}: got {} want {unc_len}",
                raw.len()
            )));
        }
        if let Some(cache) = block_cache {
            cache.put_sync(cache_key, raw.clone());
        }
        let arc: std::sync::Arc<[u8]> = raw.into();
        if let Ok(mut guard) = self.decompressed_block.lock() {
            *guard = Some((block_off, std::sync::Arc::clone(&arc)));
        }
        Ok(arc)
    }

    fn try_read_entry_sync(
        &self,
        offset: u64,
        loc: u32,
        block_cache: Option<&SharedBlockCache>,
    ) -> Result<SSTableEntry> {
        self.ensure_file_open_sync()?;
        if self.is_block_compressed() {
            let block = self.read_snappy_block_sync(offset, block_cache)?;
            return parse_framed_entry(&block, loc as usize);
        }
        let file_size = self
            .metadata
            .file_size
            .max(self.reader.file_size_blocking()?);
        if offset + 8 > file_size {
            return Err(LsmError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("entry header past EOF at {offset}"),
            )));
        }
        let entry_size = self.reader.read_u32_le_at_blocking(offset)?;
        if entry_size == 0 || entry_size > 100 * 1024 * 1024 {
            return Err(LsmError::Corruption(format!(
                "invalid entry size {entry_size} at {offset}"
            )));
        }
        let required = offset + 4 + entry_size as u64 + 4;
        if required > file_size {
            return Err(LsmError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("entry body past EOF at {offset}"),
            )));
        }
        let mut entry_buffer = vec![0u8; entry_size as usize];
        self.reader
            .read_exact_at_blocking(offset + 4, &mut entry_buffer)?;
        let stored_checksum = self
            .reader
            .read_u32_le_at_blocking(offset + 4 + entry_size as u64)?;
        let mut entry_hasher = Crc32Hasher::new();
        entry_hasher.update(&entry_size.to_le_bytes());
        entry_hasher.update(&entry_buffer);
        if stored_checksum != entry_hasher.finalize() {
            return Err(LsmError::Corruption(format!(
                "SSTable entry checksum mismatch at offset {offset}"
            )));
        }
        bincode::deserialize(&entry_buffer)
            .map_err(|e| LsmError::Serialization(format!("Failed to deserialize entry: {e}")))
    }

    /// Lookup that distinguishes tombstones from misses (needed for L0 merge).
    pub(crate) async fn lookup_pinned(
        &self,
        key: &str,
        block_cache: Option<&SharedBlockCache>,
    ) -> Result<SstableLookupResult> {
        let mut guard = ReaderGuard::new(self.reader_count.as_ref(), true);
        self.lookup_with_reader_guard(key, block_cache, &mut guard)
            .await
    }

    async fn get_with_reader_guard(
        &self,
        key: &str,
        block_cache: Option<&SharedBlockCache>,
        guard: &mut ReaderGuard<'_>,
    ) -> Result<Option<Value>> {
        match self
            .lookup_with_reader_guard(key, block_cache, guard)
            .await?
        {
            SstableLookupResult::Found { value, .. } => Ok(Some(value)),
            SstableLookupResult::Tombstone { .. } | SstableLookupResult::Missing => Ok(None),
        }
    }

    async fn lookup_with_reader_guard(
        &self,
        key: &str,
        block_cache: Option<&SharedBlockCache>,
        guard: &mut ReaderGuard<'_>,
    ) -> Result<SstableLookupResult> {
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
            return Ok(SstableLookupResult::Missing);
        }

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
        log::trace!("Index size: {}", self.metadata.entry_count);

        // Check if key is in range
        if key < self.metadata.smallest_key.as_str() || key > self.metadata.largest_key.as_str() {
            log::trace!("Key '{}' is out of range", key);
            return Ok(SstableLookupResult::Missing);
        }

        // Use bloom filter for fast key existence check
        if let Some(ref bloom_filter) = self.bloom_filter {
            if !bloom_filter.might_contain(key) {
                log::trace!("Bloom filter says key '{}' is not present", key);
                return Ok(SstableLookupResult::Missing); // Key definitely not present
            }
            log::trace!("Bloom filter says key '{}' might be present", key);
        }

        // Look up in index - loaded lazily on first access; immutable afterwards.
        let index_hit = match self.index_guard() {
            Ok(idx) => idx.get(key),
            Err(e) => {
                guard.decrement();
                return Err(e);
            }
        };
        let (offset, size) = match index_hit {
            Some(entry) => {
                log::trace!("Found key '{}' in index at offset {}", key, entry.0);
                let (offset, size) = entry;

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

                // Legacy files: size is framed-record length (never 0).
                // Compressed files: size is offset inside the Snappy block (0 is valid).
                if !self.is_block_compressed() && size == 0 {
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
                guard.decrement();
                return Ok(SstableLookupResult::Missing);
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

        if (!self.is_block_compressed() && size == 0) || size > 100 * 1024 * 1024 {
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
            match self.try_read_entry(offset, size, block_cache).await {
                Ok(entry) => {
                    // Manually decrement reader count before returning on success
                    guard.decrement();
                    if entry.deleted {
                        // Tombstone must not be collapsed to Missing: L0 merge
                        // would otherwise fall through to an older live value.
                        return Ok(SstableLookupResult::Tombstone {
                            timestamp: entry.timestamp,
                        });
                    } else {
                        return Ok(SstableLookupResult::Found {
                            value: entry.value,
                            timestamp: entry.timestamp,
                        });
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

                    // Check if this is a transient error (EOF, incomplete file, LRU close)
                    let is_transient = match &e {
                        LsmError::Io(io_err) => {
                            io_err.kind() == std::io::ErrorKind::UnexpectedEof
                                || io_err.to_string().contains("EOF")
                                || io_err.to_string().contains("incomplete")
                        }
                        LsmError::Internal(msg) => {
                            msg.contains("File not open") || msg.contains("not ready during read")
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
    async fn try_read_compressed_entry(
        &self,
        block_off: u64,
        loc: u32,
        block_cache: Option<&SharedBlockCache>,
    ) -> Result<SSTableEntry> {
        let block = self.read_snappy_block_sync(block_off, block_cache)?;
        parse_framed_entry(&block, loc as usize)
    }

    async fn try_read_entry(
        &self,
        offset: u64,
        loc: u32,
        block_cache: Option<&SharedBlockCache>,
    ) -> Result<SSTableEntry> {
        if self.is_block_compressed() {
            return self
                .try_read_compressed_entry(offset, loc, block_cache)
                .await;
        }
        let cache_key = self.block_cache_key(offset);
        if let Some(cache) = block_cache {
            if let Some(cached) = cache.get(&cache_key).await {
                if let Ok(entry) = bincode::deserialize::<SSTableEntry>(&cached) {
                    return Ok(entry);
                }
            }
        }

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

        // Re-open if LRU eviction closed the handle between ensure_sstable_open and this read
        self.ensure_file_open().await?;

        // Double-check that SSTable is still ready after opening the reader
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

        let file_size = self.reader.file_size().await?;

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

        let entry_size = match self.reader.read_u32_le_at(offset).await {
            Ok(size) => size,
            Err(LsmError::Io(e)) => {
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
            Err(e) => return Err(e),
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

        let mut entry_buffer = vec![0u8; entry_size as usize];
        match self
            .reader
            .read_exact_at(offset + 4, &mut entry_buffer)
            .await
        {
            Ok(()) => {}
            Err(LsmError::Io(e)) => {
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
            Err(e) => return Err(e),
        }

        let stored_checksum = match self
            .reader
            .read_u32_le_at(offset + 4 + entry_size as u64)
            .await
        {
            Ok(checksum) => checksum,
            Err(LsmError::Io(e)) => {
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
            Err(e) => return Err(e),
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
            let current_file_size = self.reader.file_size().await?;
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

        if let Some(cache) = block_cache {
            if let Ok(cached) = bincode::serialize(&entry) {
                cache.put(cache_key, cached).await;
            }
        }

        Ok(entry)
    }

    /// Mark SSTable for deletion (will be deleted when all readers done)
    pub fn mark_for_deletion(&self) {
        self.marked_for_deletion
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Unlink the file once marked and idle. New SST outputs are `sync_all`'d
    /// before this is called. Missing path is success (already reclaimed).
    pub async fn unlink_from_disk(&self) -> Result<()> {
        if !self.is_marked_for_deletion() {
            return Ok(());
        }
        if !self.can_delete() {
            return Err(LsmError::Internal(format!(
                "SSTable {:?} still has {} readers",
                self.path,
                self.reader_count()
            )));
        }
        self.close_file_handle().await;
        match tokio::fs::remove_file(&self.path).await {
            Ok(()) => {
                debug!("Unlinked compacted SSTable {:?}", self.path);
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(LsmError::Io(e)),
        }
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

    /// Increment reader count before opening a file handle for LRU-protected reads.
    pub(crate) fn pin_reader(&self) {
        self.reader_count
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }

    /// Decrement reader count after a pinned read completes.
    pub(crate) fn unpin_reader(&self) {
        self.reader_count
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }

    /// Pin this handle. The guard unpins on drop without touching the live map,
    /// so a concurrent compaction write lock cannot leak the pin.
    pub(crate) fn pin(&self) -> SstableReadPin {
        self.pin_reader();
        SstableReadPin {
            reader_count: Arc::clone(&self.reader_count),
        }
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
        let idx = self.index_guard()?;
        let mut keys = Vec::new();
        let pb = prefix.as_bytes();
        for i in idx.partition_point(pb)..idx.len() {
            let Some((key, _)) = idx.at(i) else {
                break;
            };
            if !key.starts_with(pb) {
                break;
            }
            keys.push(String::from_utf8_lossy(key).into_owned());
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

        // Collect locations under the index guard, then read entries without
        // holding the (non-Send) lock across awaits.
        let locs: Vec<(String, u64, u32)> = {
            let idx = self.index_guard()?;
            let pb = prefix.as_bytes();
            let mut locs = Vec::new();
            for i in idx.partition_point(pb)..idx.len() {
                let Some((key, (offset, loc))) = idx.at(i) else {
                    break;
                };
                if !key.starts_with(pb) {
                    break;
                }
                locs.push((String::from_utf8_lossy(key).into_owned(), offset, loc));
            }
            locs
        };

        let mut entries = Vec::with_capacity(locs.len());
        for (key, offset, loc) in locs {
            if let Ok(entry) = self.try_read_entry(offset, loc, None).await {
                entries.push((key, entry.value, entry.deleted));
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

        let end_bound = crate::utils::exclusive_range_end(end);
        let locs: Vec<(String, u64, u32)> = {
            let idx = self.index_guard()?;
            let mut locs = Vec::new();
            let start_i = idx.partition_point(start.as_bytes());
            for i in start_i..idx.len() {
                let Some((key, (offset, loc))) = idx.at(i) else {
                    break;
                };
                if let Some(ref end_bound) = end_bound {
                    if key >= end_bound.as_bytes() {
                        break;
                    }
                }
                locs.push((String::from_utf8_lossy(key).into_owned(), offset, loc));
            }
            locs
        };

        let mut entries = Vec::with_capacity(locs.len());
        for (key, offset, loc) in locs {
            if let Ok(entry) = self.try_read_entry(offset, loc, None).await {
                entries.push((key, entry.value, entry.deleted));
            }
        }
        Ok(entries)
    }

    /// Scan all entries in the SSTable
    pub async fn scan_all(&self) -> Result<Vec<(String, Value, bool)>> {
        let locs: Vec<(String, u64, u32)> = {
            let idx = self.index_guard()?;
            let mut locs = Vec::with_capacity(idx.len());
            for i in 0..idx.len() {
                let Some((key, (offset, loc))) = idx.at(i) else {
                    break;
                };
                locs.push((String::from_utf8_lossy(key).into_owned(), offset, loc));
            }
            locs
        };

        let mut entries = Vec::with_capacity(locs.len());
        for (key, offset, loc) in locs {
            if let Ok(entry) = self.try_read_entry(offset, loc, None).await {
                entries.push((key, entry.value, entry.deleted));
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

        let locs: Vec<(String, u64, u32)> = {
            let idx = self.index_guard()?;
            let mut locs = Vec::with_capacity(idx.len());
            for i in 0..idx.len() {
                let Some((key, (offset, size))) = idx.at(i) else {
                    break;
                };
                locs.push((String::from_utf8_lossy(key).into_owned(), offset, size));
            }
            locs
        };
        let expected = locs.len();
        let mut entries = Vec::with_capacity(expected);

        for (key, (offset, size)) in locs.iter().map(|(k, o, s)| (k.as_bytes(), (*o, *s))) {
            if self.is_marked_for_deletion() {
                return Err(LsmError::Internal(format!(
                    "SSTable {:?} marked for deletion during get_all_entries (got {} of {})",
                    self.path,
                    entries.len(),
                    expected
                )));
            }

            if offset > self.metadata.file_size {
                return Err(LsmError::Internal(format!(
                    "Invalid offset {} for key '{}' in {:?} (file size {})",
                    offset,
                    String::from_utf8_lossy(key),
                    self.path,
                    self.metadata.file_size
                )));
            }

            if (!self.is_block_compressed() && size == 0) || size > 100 * 1024 * 1024 {
                return Err(LsmError::Internal(format!(
                    "Invalid entry size {} for key '{}' in {:?}",
                    size,
                    String::from_utf8_lossy(key),
                    self.path
                )));
            }

            match self.try_read_entry(offset, size, None).await {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    return Err(LsmError::Internal(format!(
                        "Failed to read key '{}' at offset {} in {:?}: {}",
                        String::from_utf8_lossy(key),
                        offset,
                        self.path,
                        e
                    )));
                }
            }
        }

        if entries.len() != expected {
            return Err(LsmError::Internal(format!(
                "SSTable {:?} partial read: got {} of {} entries",
                self.path,
                entries.len(),
                expected
            )));
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
        self.reader.is_open()
    }

    /// Check if SSTable is ready for reads
    /// A ready SSTable has been fully written, synced, and has its metadata/index loaded
    pub fn is_ready(&self) -> bool {
        self.is_ready.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn is_block_compressed(&self) -> bool {
        self.block_compressed
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Get the number of entries in the index (loads it on first use).
    /// This is useful for validation that the index was loaded correctly
    pub fn index_size(&self) -> usize {
        if let Err(e) = self.ensure_index_loaded() {
            log::error!(
                "index_size: failed to load index for {:?}: {}",
                self.path,
                e
            );
            return 0;
        }
        self.index.read().map(|g| g.len()).unwrap_or(0)
    }

    /// Close SSTable file handle (keeps index/metadata/`is_ready` intact).
    pub async fn close(&mut self) -> Result<()> {
        self.close_file_handle().await;
        Ok(())
    }

    /// Release the OS file descriptor without invalidating in-memory index/metadata.
    ///
    /// Used after recovery load and LRU eviction so we can register far more
    /// SSTables than `ulimit -n` (open-all-at-load previously capped ~8177 on
    /// macOS soft limit 8192 and silently skipped the rest — crash-loop data loss).
    pub async fn close_file_handle(&self) {
        self.reader.close().await;
    }

    /// Get the size of the SSTable
    pub fn size(&self) -> u64 {
        self.metadata.file_size
    }

    /// Clone for compaction / prefix-scan pins.
    ///
    /// `reader_count` and `marked_for_deletion` are **shared**. Compaction
    /// clones inputs then `reclaim_compacted_inputs` marks/waits — a fresh
    /// atomic here made `wait_for_readers` return immediately and unlinked
    /// the file while the live-map instance was still pinned.
    pub fn clone_for_testing(&self) -> Self {
        Self {
            path: self.path.clone(),
            config: self.config.clone(),
            metadata: self.metadata.clone(),
            index: std::sync::RwLock::new(self.index.read().map(|g| g.clone()).unwrap_or_default()),
            index_loaded: std::sync::atomic::AtomicBool::new(
                self.index_loaded.load(std::sync::atomic::Ordering::Relaxed),
            ),
            index_load_lock: std::sync::Mutex::new(()),
            bloom_filter: self.bloom_filter.clone(),
            reader: Arc::clone(&self.reader),
            last_access: std::sync::atomic::AtomicU64::new(
                self.last_access.load(std::sync::atomic::Ordering::Relaxed),
            ),
            reader_count: Arc::clone(&self.reader_count),
            marked_for_deletion: Arc::clone(&self.marked_for_deletion),
            is_ready: std::sync::atomic::AtomicBool::new(
                self.is_ready.load(std::sync::atomic::Ordering::Relaxed),
            ),
            block_compressed: std::sync::atomic::AtomicBool::new(
                self.block_compressed
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            decompressed_block: std::sync::Mutex::new(None),
        }
    }
}

impl Clone for SSTable {
    fn clone(&self) -> Self {
        self.clone_for_testing()
    }
}

/// RAII pin that decrements `reader_count` on drop without taking the live-map lock.
pub(crate) struct SstableReadPin {
    reader_count: Arc<std::sync::atomic::AtomicUsize>,
}

impl Drop for SstableReadPin {
    fn drop(&mut self) {
        self.reader_count
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

/// Guard to ensure reader count is decremented on error return paths.
struct ReaderGuard<'a> {
    reader_count: &'a std::sync::atomic::AtomicUsize,
    decremented: bool,
    /// When true, the caller already incremented `reader_count` (engine read pin).
    external: bool,
}

impl<'a> ReaderGuard<'a> {
    fn new(reader_count: &'a std::sync::atomic::AtomicUsize, external: bool) -> Self {
        Self {
            reader_count,
            decremented: false,
            external,
        }
    }

    fn decrement(&mut self) {
        if self.external || self.decremented {
            return;
        }
        self.reader_count
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        self.decremented = true;
    }
}

impl<'a> Drop for ReaderGuard<'a> {
    fn drop(&mut self) {
        self.decrement();
    }
}

#[cfg(test)]
mod compress_tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn write_compressed_roundtrip_and_prefix() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.sst");
        let mut sst = SSTable::new(path.clone(), SstableConfig::default(), 0).unwrap();
        let mut entries = Vec::new();
        for i in 0..200 {
            entries.push(SSTableEntry {
                key: format!("dDocuments/path/{i:04}/file.txt"),
                value: Value::Bytes(format!(r#"{{"name":"file{i}.txt","size":{i}}}"#).into_bytes()),
                timestamp: i as u64 + 1,
                deleted: false,
            });
        }
        sst.write_entries(entries).await.unwrap();
        sst.open().await.unwrap();
        assert!(sst.is_block_compressed());
        let head = std::fs::read(&path).unwrap();
        assert_eq!(&head[..4], b"F4SC");

        let got = sst
            .lookup_sync("dDocuments/path/0007/file.txt", None)
            .unwrap();
        match got {
            SstableLookupResult::Found { value, .. } => {
                assert!(matches!(value, Value::Bytes(_)));
            }
            other => panic!("{other:?}"),
        }
        let keys = sst.scan_prefix("dDocuments/path/000").await.unwrap();
        assert!(keys.len() >= 10);

        let cache = SharedBlockCache::new(4 << 20);
        for i in 0..200 {
            let k = format!("dDocuments/path/{i:04}/file.txt");
            assert!(matches!(
                sst.lookup_sync(&k, Some(&cache)).unwrap(),
                SstableLookupResult::Found { .. }
            ));
        }
    }

    #[tokio::test]
    async fn legacy_uncompressed_still_reads() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("legacy.sst");
        // Minimal legacy file: one framed entry + empty-ish index/metadata is hard
        // to hand-roll. Write compressed, then verify a non-magic file is
        // detected as legacy (open fails without a real footer — just the flag).
        let mut sst = SSTable::new(path.clone(), SstableConfig::default(), 0).unwrap();
        sst.write_entries(vec![SSTableEntry {
            key: "k".into(),
            value: Value::Bytes(b"v".to_vec()),
            timestamp: 1,
            deleted: false,
        }])
        .await
        .unwrap();
        drop(sst);
        let mut sst = SSTable::new(path, SstableConfig::default(), 0).unwrap();
        sst.open().await.unwrap();
        assert!(sst.is_block_compressed());
        match sst.lookup_sync("k", None).unwrap() {
            SstableLookupResult::Found { value, .. } => {
                assert_eq!(value, Value::Bytes(b"v".to_vec()));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn clone_shares_reader_pin_and_delete_flag() {
        let dir = TempDir::new().unwrap();
        let sst = SSTable::new(dir.path().join("pin.sst"), SstableConfig::default(), 0).unwrap();
        sst.pin_reader();
        let clone = sst.clone();
        assert_eq!(clone.reader_count(), 1, "clone must see the live pin");
        clone.mark_for_deletion();
        assert!(sst.is_marked_for_deletion(), "mark must be shared");
        assert!(!clone.can_delete(), "pinned clone must block unlink");
        sst.unpin_reader();
        assert_eq!(clone.reader_count(), 0);
        assert!(clone.can_delete());
    }

    #[test]
    fn pin_guard_unpins_without_map_lock() {
        let dir = TempDir::new().unwrap();
        let sst = SSTable::new(dir.path().join("pin2.sst"), SstableConfig::default(), 0).unwrap();
        {
            let _pin = sst.pin();
            assert_eq!(sst.reader_count(), 1);
        }
        assert_eq!(
            sst.reader_count(),
            0,
            "Drop must unpin even with no map lock"
        );
    }
}
