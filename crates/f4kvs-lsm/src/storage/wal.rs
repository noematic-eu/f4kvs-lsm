//! Write-Ahead Log (WAL) implementation for LSM Tree Engine

use crate::core::config::{WalConfig, WalSyncMode};
use crate::error::{LsmError, Result};
use crate::utils;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Mutex, Notify, RwLock};
use tracing::{debug, warn};

/// WAL entry types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WALEntry {
    /// Put operation entry
    Put {
        /// Key for the put operation
        key: String,
        /// Value for the put operation
        value: f4kvs_value::Value,
        /// Timestamp of the operation
        timestamp: u64,
    },
    /// Delete operation entry
    Delete {
        /// Key for the delete operation
        key: String,
        /// Timestamp of the operation
        timestamp: u64,
    },
    /// Flush operation entry
    Flush {
        /// ID of the memtable being flushed
        memtable_id: u64,
        /// Timestamp of the flush
        timestamp: u64,
    },
    /// Checkpoint entry
    Checkpoint {
        /// Timestamp of the checkpoint
        timestamp: u64,
    },
}

/// WAL segment header
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WALSegmentHeader {
    magic: [u8; 4], // "WAL1"
    version: u8,
    created_at: u64,
    entry_count: u32,
}

/// WAL segment
pub struct WALSegment {
    path: PathBuf,
    file: File,
    header: WALSegmentHeader,
    entry_count: u32,
    max_size: u64,
    sync_mode: WalSyncMode,
}

impl WALSegment {
    const MAGIC: [u8; 4] = [b'W', b'A', b'L', b'1'];
    const VERSION: u8 = 1;

    /// Create a new WAL segment
    pub async fn new(path: PathBuf, max_size: u64, sync_mode: WalSyncMode) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(false)
            .open(&path)
            .await
            .map_err(LsmError::Io)?;

        let header = WALSegmentHeader {
            magic: Self::MAGIC,
            version: Self::VERSION,
            created_at: utils::timestamp_secs(),
            entry_count: 0,
        };

        let mut segment = Self {
            path,
            file,
            header: header.clone(),
            entry_count: 0,
            max_size,
            sync_mode,
        };

        // Write header
        segment.write_header().await?;

        Ok(segment)
    }

    /// Open an existing WAL segment for reading
    pub async fn open_for_reading(
        path: PathBuf,
        max_size: u64,
        sync_mode: WalSyncMode,
    ) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .open(&path)
            .await
            .map_err(LsmError::Io)?;

        // Read header directly (it's written without size prefix)
        let header_size = bincode::serialized_size(&WALSegmentHeader {
            magic: Self::MAGIC,
            version: Self::VERSION,
            created_at: 0,
            entry_count: 0,
        })
        .map_err(|e| LsmError::Serialization(format!("Failed to get header size: {}", e)))?
            as usize;

        let mut header_buffer = vec![0u8; header_size];
        file.read_exact(&mut header_buffer)
            .await
            .map_err(LsmError::Io)?;

        let header: WALSegmentHeader = bincode::deserialize(&header_buffer)
            .map_err(|e| LsmError::Serialization(format!("Failed to deserialize header: {}", e)))?;

        // Verify magic
        if header.magic != Self::MAGIC {
            return Err(LsmError::Corruption("Invalid WAL magic number".to_string()));
        }

        Ok(Self {
            path,
            file,
            header: header.clone(),
            entry_count: header.entry_count,
            max_size,
            sync_mode,
        })
    }

    /// Write segment header
    async fn write_header(&mut self) -> Result<()> {
        let header_data = bincode::serialize(&self.header)
            .map_err(|e| LsmError::Serialization(format!("Failed to serialize header: {}", e)))?;

        self.file
            .seek(tokio::io::SeekFrom::Start(0))
            .await
            .map_err(LsmError::Io)?;

        self.file
            .write_all(&header_data)
            .await
            .map_err(LsmError::Io)?;

        Ok(())
    }

    /// Write an entry to the segment
    pub async fn write_entry(&mut self, entry: &WALEntry) -> Result<bool> {
        // Seek to end of file
        self.file
            .seek(tokio::io::SeekFrom::End(0))
            .await
            .map_err(LsmError::Io)?;

        // Serialize entry
        let entry_data = bincode::serialize(entry)
            .map_err(|e| LsmError::Serialization(format!("Failed to serialize entry: {}", e)))?;

        // Check if writing this entry would exceed the size limit
        let current_size = self.file.metadata().await.map_err(LsmError::Io)?.len();
        let entry_size = entry_data.len() as u64 + 4; // +4 for the size header
        if current_size + entry_size > self.max_size {
            return Ok(false); // Need to rotate
        }

        // Write entry size and data
        let size = entry_data.len() as u32;
        self.file.write_u32_le(size).await.map_err(LsmError::Io)?;
        self.file
            .write_all(&entry_data)
            .await
            .map_err(LsmError::Io)?;

        // Update counts (header persisted on flush/close/rotate, not per entry)
        self.entry_count += 1;
        self.header.entry_count = self.entry_count;

        // Flush and sync based on sync_mode
        self.sync_after_flush().await?;

        Ok(true)
    }

    /// Persist the segment header and flush/sync to disk.
    async fn sync_header_and_flush(&mut self) -> Result<()> {
        self.write_header().await?;
        self.sync_after_flush().await
    }

    /// Sync file to disk based on sync_mode
    async fn sync_after_flush(&mut self) -> Result<()> {
        // Always flush to OS buffer first
        self.file.flush().await.map_err(LsmError::Io)?;

        match self.sync_mode {
            WalSyncMode::None => {
                // No additional sync - fastest but may lose data
            }
            WalSyncMode::Flush => {
                // Already flushed above - no additional action needed
            }
            WalSyncMode::Fsync => {
                // Full fsync for maximum durability
                // Use sync_all which syncs both data and metadata to disk
                let start_time = std::time::Instant::now();
                self.file.sync_all().await.map_err(LsmError::Io)?;
                let duration = start_time.elapsed();

                // Log fsync latency for strict mode
                debug!(
                    "WAL segment synced to disk (fsync) in {:?}ms",
                    duration.as_millis()
                );

                // TODO: Add metrics collection here for fsync latency
            }
            WalSyncMode::FsyncAsync => {
                // Detach fsync to an OS thread — avoids tokio::spawn deadlock when callers
                // use block_on on the same runtime (FFI / sync bench harness).
                let path = self.path.clone();
                std::thread::spawn(move || {
                    let start_time = std::time::Instant::now();
                    match std::fs::OpenOptions::new().write(true).open(&path) {
                        Ok(file) => {
                            if let Err(e) = file.sync_all() {
                                warn!(
                                    "Background fsync failed for {:?}: {} (latency: {:?}ms)",
                                    path,
                                    e,
                                    start_time.elapsed().as_millis()
                                );
                            } else {
                                debug!(
                                    "WAL segment synced to disk (async fsync) in {:?}ms",
                                    start_time.elapsed().as_millis()
                                );
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Background fsync could not open {:?}: {} (latency: {:?}ms)",
                                path,
                                e,
                                start_time.elapsed().as_millis()
                            );
                        }
                    }
                });
                debug!("WAL segment sync started in background (async fsync)");
            }
        }

        Ok(())
    }

    /// Check if segment should be rotated
    async fn should_rotate(&self) -> Result<bool> {
        let metadata = self.file.metadata().await.map_err(LsmError::Io)?;

        Ok(metadata.len() >= self.max_size)
    }

    /// Read all entries from segment
    pub async fn read_entries(&mut self) -> Result<Vec<WALEntry>> {
        let mut entries = Vec::new();

        // Seek to after header
        let header_size = bincode::serialized_size(&self.header)
            .map_err(|e| LsmError::Serialization(format!("Failed to get header size: {}", e)))?
            as u64;

        self.file
            .seek(tokio::io::SeekFrom::Start(header_size))
            .await
            .map_err(LsmError::Io)?;

        // Read entries
        while let Ok(size) = self.file.read_u32_le().await {
            if size == 0 {
                break; // End of entries
            }

            // Read entry data
            let mut entry_buffer = vec![0u8; size as usize];
            self.file
                .read_exact(&mut entry_buffer)
                .await
                .map_err(LsmError::Io)?;

            // Deserialize entry with better error handling
            let entry: WALEntry = match bincode::deserialize(&entry_buffer) {
                Ok(entry) => entry,
                Err(e) => {
                    warn!("Failed to deserialize WAL entry: {}, skipping entry", e);
                    continue; // Skip this corrupted entry
                }
            };

            entries.push(entry);
        }

        Ok(entries)
    }

    /// Get segment path
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Get entry count
    pub fn entry_count(&self) -> u32 {
        self.entry_count
    }

    /// Close segment
    /// Flush the segment to disk
    pub async fn flush(&mut self) -> Result<()> {
        self.sync_header_and_flush().await?;
        Ok(())
    }

    /// Close the WAL segment and flush any pending writes
    pub async fn close(&mut self) -> Result<()> {
        tracing::info!("WAL Segment: Closing segment: {:?}", self.path);

        self.write_header().await?;

        // Sync all data to disk
        self.file.sync_all().await.map_err(LsmError::Io)?;

        tracing::info!("WAL Segment: Segment closed successfully: {:?}", self.path);
        Ok(())
    }
}

/// Group commit batch for efficient WAL writes
#[derive(Debug)]
struct GroupCommitBatch {
    entries: Vec<WALEntry>,
    notify: Arc<Notify>,
}

/// WAL manager with group commit support
pub struct WALManager {
    config: WalConfig,
    current_segment: Arc<RwLock<Option<WALSegment>>>,
    segments: Arc<RwLock<HashMap<u64, PathBuf>>>,
    segment_counter: Arc<std::sync::atomic::AtomicU64>,
    wal_dir: PathBuf,

    // Group commit fields
    pending_batch: Arc<Mutex<Option<GroupCommitBatch>>>,
    commit_notify: Arc<Notify>,
    max_batch_size: usize,
    max_batch_wait: Duration,
    commit_task: Option<tokio::task::JoinHandle<()>>,
}

impl WALManager {
    /// Create a new WAL manager
    pub fn new(config: &WalConfig) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
            current_segment: Arc::new(RwLock::new(None)),
            segments: Arc::new(RwLock::new(HashMap::new())),
            segment_counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            wal_dir: PathBuf::from(&config.dir),
            pending_batch: Arc::new(Mutex::new(None)),
            commit_notify: Arc::new(Notify::new()),
            max_batch_size: 1000,                      // Default batch size
            max_batch_wait: Duration::from_millis(10), // 10ms max wait
            commit_task: None,
        })
    }

    /// Initialize WAL (create first segment)
    pub async fn initialize(&self) -> Result<()> {
        // Create WAL directory if it doesn't exist
        if !self.wal_dir.exists() {
            tokio::fs::create_dir_all(&self.wal_dir)
                .await
                .map_err(LsmError::Io)?;
        }

        // Check for existing segments and set counter appropriately
        self.scan_existing_segments().await?;
        self.rotate_segment().await?;
        Ok(())
    }

    /// Scan for existing WAL segments and set counter appropriately
    async fn scan_existing_segments(&self) -> Result<()> {
        if !self.wal_dir.exists() {
            return Ok(());
        }

        let mut max_segment_id = 0u64;
        if let Ok(entries) = std::fs::read_dir(&self.wal_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                    if file_name.starts_with("segment_") && file_name.ends_with(".wal") {
                        // Extract segment ID from filename
                        if let Some(id_str) = file_name
                            .strip_prefix("segment_")
                            .and_then(|s| s.strip_suffix(".wal"))
                        {
                            if let Ok(segment_id) = u64::from_str_radix(id_str, 16) {
                                max_segment_id = max_segment_id.max(segment_id);
                            }
                        }
                    }
                }
            }
        }

        // Set counter to next available segment ID
        self.segment_counter
            .store(max_segment_id + 1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    /// Rotate to a new segment
    async fn rotate_segment(&self) -> Result<()> {
        tracing::info!("WAL: Starting segment rotation");

        // Close current segment if exists
        let mut current_segment = self.current_segment.write().await;
        if let Some(mut segment) = current_segment.take() {
            tracing::info!("WAL: Closing current segment");
            segment.close().await?;

            // Add to segments list
            let segment_id = self
                .segment_counter
                .load(std::sync::atomic::Ordering::SeqCst);
            let mut segments = self.segments.write().await;
            segments.insert(segment_id, segment.path().clone());
            tracing::info!("WAL: Added segment {} to segments list", segment_id);
        } else {
            tracing::warn!("WAL: No current segment to close during rotation");
        }

        // Create new segment
        let segment_id = self
            .segment_counter
            .load(std::sync::atomic::Ordering::SeqCst);
        let segment_path = self
            .wal_dir
            .join(format!("segment_{:016x}.wal", segment_id));

        tracing::info!(
            "WAL: Creating new segment {} at {:?}",
            segment_id,
            segment_path
        );
        let segment = WALSegment::new(
            segment_path,
            self.config.segment_size as u64,
            self.config.sync_mode,
        )
        .await?;
        *current_segment = Some(segment);
        drop(current_segment); // Release the lock before incrementing counter

        // Increment segment counter
        self.segment_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        tracing::info!("WAL: Segment rotation completed successfully");
        Ok(())
    }

    /// Write an operation to WAL
    pub async fn write_operation(&self, key: &str, value: &f4kvs_value::Value) -> Result<()> {
        let entry = WALEntry::Put {
            key: key.to_string(),
            value: value.clone(),
            timestamp: utils::timestamp_secs(),
        };

        self.write_entry(&entry).await
    }

    /// Write a delete operation to WAL
    pub async fn write_delete(&self, key: &str) -> Result<()> {
        let timestamp = utils::timestamp_secs();

        let entry = WALEntry::Delete {
            key: key.to_string(),
            timestamp,
        };

        // Debug logging removed for performance
        self.write_entry(&entry).await
    }

    /// Write a WAL entry
    pub async fn write_entry(&self, entry: &WALEntry) -> Result<()> {
        // Try to write entry to current segment
        let needs_rotation = {
            // Get mutable reference to current segment (scoped to release lock before rotation)
            let mut current_segment_guard = self.current_segment.write().await;
            let current_segment = current_segment_guard.as_mut().ok_or_else(|| {
                tracing::error!("WAL: No current WAL segment available for write_entry");
                LsmError::Internal("No current WAL segment".to_string())
            })?;

            tracing::debug!("WAL: Writing entry to segment");

            // Try to write entry - returns false if segment is full
            let success = current_segment.write_entry(entry).await?;
            !success // needs_rotation = true if write failed due to size
                     // Guard is dropped here, releasing the lock
        };

        // If write failed due to size, rotate segment (lock is released now)
        if needs_rotation {
            tracing::info!("WAL: Segment full, rotating to new segment");
            // Rotate to new segment - this acquires its own lock
            self.rotate_segment().await?;

            // Try writing again to new segment
            let mut current_segment_guard = self.current_segment.write().await;
            let current_segment = current_segment_guard.as_mut().ok_or_else(|| {
                tracing::error!("WAL: Failed to create new WAL segment after rotation");
                LsmError::Internal("Failed to create new WAL segment".to_string())
            })?;

            tracing::debug!("WAL: Writing entry to new segment after rotation");
            current_segment.write_entry(entry).await?;
        }

        Ok(())
    }

    /// Flush WAL to disk
    pub async fn flush(&self) -> Result<()> {
        let mut current_segment = self.current_segment.write().await;
        if let Some(segment) = current_segment.as_mut() {
            // Flush the current segment to disk
            segment.flush().await?;
        }
        Ok(())
    }

    /// Truncate WAL after successful flush to LSM-Tree
    /// This clears all WAL entries that have been successfully flushed to SSTables
    pub async fn truncate_after_flush(&self) -> Result<()> {
        tracing::info!("WAL: Starting truncate_after_flush");

        // Count WAL files before truncation
        let wal_dir = std::path::Path::new(&self.wal_dir);
        let initial_file_count = if wal_dir.exists() {
            std::fs::read_dir(wal_dir)
                .map(|entries| entries.count())
                .unwrap_or(0)
        } else {
            0
        };
        tracing::info!("WAL: Found {} files before truncation", initial_file_count);

        // Close and remove current segment
        let mut current_segment = self.current_segment.write().await;
        if let Some(mut segment) = current_segment.take() {
            let path = segment.path().to_path_buf();
            tracing::info!("WAL: Closing current segment: {:?}", path);

            // Close the segment first and ensure file handle is released
            if let Err(e) = segment.close().await {
                tracing::warn!("WAL: Error closing segment {:?}: {}", path, e);
            }
            drop(segment); // Explicitly drop to release file handle

            // Small delay to ensure file handle is released
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

            // Verify file exists before removal
            if path.exists() {
                tracing::info!("WAL: Removing current segment file: {:?}", path);

                // Retry logic for file removal (handle OS file locking)
                let mut retry_count = 0;
                let max_retries = 3;
                while retry_count < max_retries {
                    match tokio::fs::remove_file(&path).await {
                        Ok(_) => {
                            tracing::info!("WAL: Successfully removed current segment: {:?}", path);
                            break;
                        }
                        Err(e) => {
                            retry_count += 1;
                            if retry_count >= max_retries {
                                return Err(LsmError::Io(e));
                            }
                            tracing::warn!(
                                "WAL: Failed to remove {:?} (attempt {}): {}, retrying...",
                                path,
                                retry_count,
                                e
                            );
                            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                        }
                    }
                }

                // Verify removal
                if path.exists() {
                    return Err(LsmError::Io(std::io::Error::other(format!(
                        "Failed to remove WAL file after {} retries: {:?}",
                        max_retries, path
                    ))));
                }
            } else {
                tracing::info!("WAL: Current segment file already removed: {:?}", path);
            }
        } else {
            tracing::info!("WAL: No current segment to remove");
        }

        // Clear all completed segments
        let mut segments = self.segments.write().await;
        tracing::info!("WAL: Removing {} completed segments", segments.len());

        let mut failed_removals = Vec::new();
        for (id, path) in segments.iter() {
            if path.exists() {
                tracing::info!("WAL: Removing segment {}: {:?}", id, path);

                // Retry logic for segment removal
                let mut retry_count = 0;
                let max_retries = 3;
                let mut removal_success = false;

                while retry_count < max_retries {
                    match tokio::fs::remove_file(path).await {
                        Ok(_) => {
                            tracing::info!("WAL: Successfully removed segment {}: {:?}", id, path);
                            removal_success = true;
                            break;
                        }
                        Err(e) => {
                            retry_count += 1;
                            if retry_count >= max_retries {
                                tracing::error!(
                                    "WAL: Failed to remove segment {} after {} retries: {}",
                                    id,
                                    max_retries,
                                    e
                                );
                                failed_removals.push((*id, path.clone(), e));
                            } else {
                                tracing::warn!("WAL: Failed to remove segment {} (attempt {}): {}, retrying...",
                                    id, retry_count, e);
                                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                            }
                        }
                    }
                }

                if !removal_success && path.exists() {
                    return Err(LsmError::Io(std::io::Error::other(format!(
                        "Failed to remove WAL segment {}: {:?}",
                        id, path
                    ))));
                }
            } else {
                tracing::info!("WAL: Segment {} already removed: {:?}", id, path);
            }
        }
        segments.clear();

        // Count remaining WAL files after truncation
        let final_file_count = if wal_dir.exists() {
            std::fs::read_dir(wal_dir)
                .map(|entries| entries.count())
                .unwrap_or(0)
        } else {
            0
        };

        if final_file_count > 0 {
            tracing::warn!(
                "WAL: {} files remain after truncation (started with {})",
                final_file_count,
                initial_file_count
            );
        } else {
            tracing::info!(
                "WAL: All files successfully removed (started with {})",
                initial_file_count
            );
        }

        if !failed_removals.is_empty() {
            tracing::error!("WAL: Failed to remove {} segments", failed_removals.len());
            for (id, path, error) in failed_removals {
                tracing::error!("WAL: Failed segment {}: {:?} - {}", id, path, error);
            }
        }

        // CRITICAL FIX: Create a new fresh segment after truncation
        // This ensures continuous operation by immediately providing a fresh WAL segment for subsequent writes
        drop(current_segment); // Release write lock
        self.rotate_segment().await?;
        tracing::info!("WAL: Created new segment after truncation");

        tracing::info!("WAL: truncate_after_flush completed successfully");
        Ok(())
    }

    /// Verify that all WAL files have been removed
    pub async fn verify_truncated(&self) -> Result<bool> {
        let current_segment = self.current_segment.read().await;
        // After truncation, we should have a fresh new segment (not None)
        if current_segment.is_none() {
            tracing::warn!("WAL: No current segment exists during verification - this indicates truncation failed");
            return Ok(false);
        }

        let segments = self.segments.read().await;
        if !segments.is_empty() {
            tracing::warn!(
                "WAL: {} segments still exist during verification",
                segments.len()
            );
            return Ok(false);
        }

        // Additional verification: check filesystem for any remaining WAL files
        let wal_dir = std::path::Path::new(&self.wal_dir);
        if wal_dir.exists() {
            let remaining_files = std::fs::read_dir(wal_dir)
                .map(|entries| {
                    entries
                        .filter_map(|entry| entry.ok())
                        .filter(|entry| {
                            entry
                                .path()
                                .extension()
                                .and_then(|ext| ext.to_str())
                                .map(|ext| ext == "log")
                                .unwrap_or(false)
                        })
                        .count()
                })
                .unwrap_or(0);

            if remaining_files > 0 {
                tracing::warn!(
                    "WAL: {} files still exist in filesystem during verification",
                    remaining_files
                );
                return Ok(false);
            }
        }

        tracing::info!("WAL: Verification successful - all WAL files removed");
        Ok(true)
    }

    /// Read WAL entries from all segments
    pub async fn read_entries(&self) -> Result<Vec<WALEntry>> {
        let mut all_entries = Vec::new();

        // Read from current segment
        let current_segment = self.current_segment.read().await;
        if let Some(segment) = current_segment.as_ref() {
            // For reading, sync_mode doesn't matter, use default
            let mut segment_clone = WALSegment::open_for_reading(
                segment.path().clone(),
                self.config.segment_size as u64,
                self.config.sync_mode,
            )
            .await?;
            let entries = segment_clone.read_entries().await?;
            all_entries.extend(entries);
        }

        // Read from completed segments
        let segments = self.segments.read().await;
        for (_, path) in segments.iter() {
            if let Ok(mut segment) = WALSegment::new(
                path.clone(),
                self.config.segment_size as u64,
                self.config.sync_mode,
            )
            .await
            {
                if let Ok(entries) = segment.read_entries().await {
                    all_entries.extend(entries);
                }
            }
        }

        // Sort by timestamp
        all_entries.sort_by(|a, b| {
            let timestamp_a = match a {
                WALEntry::Put { timestamp, .. } => *timestamp,
                WALEntry::Delete { timestamp, .. } => *timestamp,
                WALEntry::Flush { timestamp, .. } => *timestamp,
                WALEntry::Checkpoint { timestamp, .. } => *timestamp,
            };
            let timestamp_b = match b {
                WALEntry::Put { timestamp, .. } => *timestamp,
                WALEntry::Delete { timestamp, .. } => *timestamp,
                WALEntry::Flush { timestamp, .. } => *timestamp,
                WALEntry::Checkpoint { timestamp, .. } => *timestamp,
            };
            timestamp_a.cmp(&timestamp_b)
        });

        Ok(all_entries)
    }

    /// Cleanup old segments
    pub async fn cleanup_old_segments(&self, retention_period: std::time::Duration) -> Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| {
                crate::F4KvsError::storage(format!(
                    "System time error: system time is before UNIX epoch: {}",
                    e
                ))
            })?
            .as_secs();
        let cutoff = now - retention_period.as_secs();

        let mut segments = self.segments.write().await;
        let mut to_remove = Vec::new();

        for (id, path) in segments.iter() {
            if let Ok(metadata) = tokio::fs::metadata(path).await {
                if let Ok(created) = metadata.created() {
                    if let Ok(created_secs) = created.duration_since(UNIX_EPOCH) {
                        if created_secs.as_secs() < cutoff {
                            to_remove.push(*id);
                        }
                    }
                }
            }
        }

        // Remove old segments
        for id in to_remove {
            if let Some(path) = segments.remove(&id) {
                if let Err(e) = tokio::fs::remove_file(&path).await {
                    tracing::warn!("Failed to remove old WAL segment {}: {}", path.display(), e);
                }
            }
        }

        Ok(())
    }

    /// Clean up segments that have been flushed to SSTables
    /// This removes segments that are older than the specified grace period
    pub async fn cleanup_flushed_segments(&self, grace_period: Duration) -> Result<()> {
        let cutoff = utils::timestamp_secs().saturating_sub(grace_period.as_secs());

        let mut segments = self.segments.write().await;
        let mut to_remove = Vec::new();

        // Find segments that are old enough to be considered flushed
        for (id, path) in segments.iter() {
            if let Ok(metadata) = tokio::fs::metadata(path).await {
                if let Ok(created) = metadata.created() {
                    if let Ok(created_secs) = created.duration_since(UNIX_EPOCH) {
                        if created_secs.as_secs() < cutoff {
                            tracing::info!(
                                "WAL: Marking flushed segment {} for removal (age: {}s)",
                                id,
                                created_secs.as_secs()
                            );
                            to_remove.push(*id);
                        }
                    }
                }
            }
        }

        // Remove flushed segments
        for id in to_remove {
            if let Some(path) = segments.remove(&id) {
                if let Err(e) = tokio::fs::remove_file(&path).await {
                    tracing::warn!(
                        "WAL: Failed to remove flushed segment {}: {}",
                        path.display(),
                        e
                    );
                } else {
                    tracing::info!("WAL: Removed flushed segment {}: {}", id, path.display());
                }
            }
        }

        Ok(())
    }

    /// Force aggressive cleanup of all WAL segments
    /// This is used when there are too many segments
    pub async fn force_cleanup(&self) -> Result<()> {
        tracing::warn!("WAL: Starting aggressive cleanup");

        // Close and remove current segment
        let mut current_segment = self.current_segment.write().await;
        if let Some(mut segment) = current_segment.take() {
            let path = segment.path().to_path_buf();
            tracing::info!("WAL: Force closing current segment: {:?}", path);

            if let Err(e) = segment.close().await {
                tracing::warn!("WAL: Error closing segment {:?}: {}", path, e);
            }
            drop(segment);

            // Remove current segment file
            if path.exists() {
                if let Err(e) = tokio::fs::remove_file(&path).await {
                    tracing::warn!("WAL: Failed to remove current segment {:?}: {}", path, e);
                } else {
                    tracing::info!("WAL: Removed current segment: {:?}", path);
                }
            }
        }

        // Remove all completed segments
        let mut segments = self.segments.write().await;
        tracing::info!("WAL: Force removing {} completed segments", segments.len());

        for (id, path) in segments.iter() {
            if path.exists() {
                if let Err(e) = tokio::fs::remove_file(path).await {
                    tracing::warn!("WAL: Failed to remove segment {}: {}", path.display(), e);
                } else {
                    tracing::info!("WAL: Force removed segment {}: {}", id, path.display());
                }
            }
        }
        segments.clear();

        // CRITICAL FIX: Create a new fresh segment after cleanup
        // This ensures continuous operation by immediately providing a fresh WAL segment for subsequent writes
        drop(current_segment); // Release write lock
        self.rotate_segment().await?;
        tracing::info!("WAL: Created new segment after force cleanup");

        tracing::info!("WAL: Aggressive cleanup completed");
        Ok(())
    }

    /// Force rotation of current segment
    pub async fn force_rotate(&self) -> Result<()> {
        self.rotate_segment().await
    }

    /// Batch write multiple put operations to WAL
    pub async fn batch_write_operations(
        &self,
        items: &[(String, f4kvs_value::Value)],
    ) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }

        let timestamp = utils::timestamp_secs();
        // Debug logging removed for performance

        let entries: Vec<WALEntry> = items
            .iter()
            .map(|(key, value)| WALEntry::Put {
                key: key.clone(),
                value: value.clone(),
                timestamp,
            })
            .collect();

        self.batch_write_entries(&entries).await
    }

    /// Internal method to write multiple entries in batch
    async fn batch_write_entries(&self, entries: &[WALEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        tracing::debug!("WAL: Starting batch write of {} entries", entries.len());

        // Check if we need to rotate first
        {
            let current_segment = self.current_segment.read().await;
            if let Some(segment) = current_segment.as_ref() {
                if segment.should_rotate().await? {
                    tracing::info!("WAL: Rotating segment before batch write");
                    // current_segment is automatically dropped here
                    self.rotate_segment().await?;
                }
            }
        } // current_segment is dropped here

        // Get current segment after potential rotation
        let mut current_segment = self.current_segment.write().await;
        let current_segment = current_segment.as_mut().ok_or_else(|| {
            tracing::error!("WAL: No current WAL segment available for batch write");
            LsmError::Internal("No current WAL segment".to_string())
        })?;

        tracing::debug!("WAL: Got current segment for batch write");

        // Seek to end of file
        current_segment
            .file
            .seek(tokio::io::SeekFrom::End(0))
            .await
            .map_err(LsmError::Io)?;

        // Prepare all data to write in one go
        let mut data_to_write = Vec::new();
        for entry in entries {
            let entry_data = bincode::serialize(entry).map_err(|e| {
                LsmError::Serialization(format!("Failed to serialize entry: {}", e))
            })?;
            let size = entry_data.len() as u32;
            data_to_write.extend_from_slice(&size.to_le_bytes());
            data_to_write.extend_from_slice(&entry_data);
        }

        tracing::debug!(
            "WAL: Prepared {} bytes for batch write",
            data_to_write.len()
        );

        // CRITICAL FIX: Check if the batch write would exceed segment size
        // If so, we need to split the batch or rotate the segment
        let current_size = current_segment
            .file
            .metadata()
            .await
            .map_err(LsmError::Io)?
            .len();
        let max_size = current_segment.max_size;

        if current_size + data_to_write.len() as u64 > max_size {
            tracing::info!(
                "WAL: Batch write would exceed segment size ({} + {} > {}), rotating segment",
                current_size,
                data_to_write.len(),
                max_size
            );

            // Drop the current segment lock
            let _ = current_segment;

            // Rotate to new segment
            self.rotate_segment().await?;

            // Get the new segment and complete the write operation
            let mut current_segment = self.current_segment.write().await;
            let current_segment = current_segment.as_mut().ok_or_else(|| {
                tracing::error!("WAL: Failed to get new segment after rotation");
                LsmError::Internal("Failed to create new WAL segment".to_string())
            })?;

            // Seek to end of new segment
            current_segment
                .file
                .seek(tokio::io::SeekFrom::End(0))
                .await
                .map_err(LsmError::Io)?;

            // Write all data to new segment
            current_segment
                .file
                .write_all(&data_to_write)
                .await
                .map_err(LsmError::Io)?;
            tracing::debug!(
                "WAL: Successfully wrote {} bytes to new segment",
                data_to_write.len()
            );

            // Update counts
            current_segment.entry_count += entries.len() as u32;
            current_segment.header.entry_count = current_segment.entry_count;
            tracing::debug!(
                "WAL: Updated entry count to {}",
                current_segment.entry_count
            );

            // Update header and sync once for the whole batch
            current_segment.sync_header_and_flush().await?;
        } else {
            // Write all data to current segment
            current_segment
                .file
                .write_all(&data_to_write)
                .await
                .map_err(LsmError::Io)?;
            tracing::debug!(
                "WAL: Successfully wrote {} bytes to segment",
                data_to_write.len()
            );

            // Update counts
            current_segment.entry_count += entries.len() as u32;
            current_segment.header.entry_count = current_segment.entry_count;
            tracing::debug!(
                "WAL: Updated entry count to {}",
                current_segment.entry_count
            );

            // Update header and sync once for the whole batch
            current_segment.sync_header_and_flush().await?;
        }

        Ok(())
    }

    /// Start the group commit background task
    pub async fn start_group_commit(&mut self) -> Result<()> {
        if self.commit_task.is_some() {
            return Ok(()); // Already started
        }

        let pending_batch = self.pending_batch.clone();
        let commit_notify = self.commit_notify.clone();
        let current_segment = self.current_segment.clone();
        let _max_batch_size = self.max_batch_size;
        let max_batch_wait = self.max_batch_wait;

        let commit_task = tokio::spawn(async move {
            let mut commit_interval = tokio::time::interval(max_batch_wait);

            loop {
                tokio::select! {
                    _ = commit_interval.tick() => {
                        // Time-based commit
                        Self::process_pending_batch(&pending_batch, &current_segment).await;
                    }
                    _ = commit_notify.notified() => {
                        // Notify-based commit
                        Self::process_pending_batch(&pending_batch, &current_segment).await;
                    }
                }
            }
        });

        self.commit_task = Some(commit_task);
        Ok(())
    }

    /// Process pending batch for group commit
    async fn process_pending_batch(
        pending_batch: &Arc<Mutex<Option<GroupCommitBatch>>>,
        current_segment: &Arc<RwLock<Option<WALSegment>>>,
    ) {
        let batch = {
            let mut batch_guard = pending_batch.lock().await;
            batch_guard.take()
        };

        if let Some(batch) = batch {
            // Write all entries in the batch
            if let Err(e) = Self::write_batch_to_segment(current_segment, &batch.entries).await {
                tracing::error!("Failed to write batch to WAL: {}", e);
            }

            // Notify all waiting writers
            batch.notify.notify_waiters();
        }
    }

    /// Write a batch of entries to the current segment
    async fn write_batch_to_segment(
        current_segment: &Arc<RwLock<Option<WALSegment>>>,
        entries: &[WALEntry],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut segment_guard = current_segment.write().await;
        let segment = segment_guard
            .as_mut()
            .ok_or_else(|| LsmError::Internal("No current WAL segment".to_string()))?;

        // Serialize all entries
        let mut data_to_write = Vec::new();
        for entry in entries {
            let entry_data = bincode::serialize(entry).map_err(|e| {
                LsmError::Serialization(format!("Failed to serialize entry: {}", e))
            })?;
            let size = entry_data.len() as u32;
            data_to_write.extend_from_slice(&size.to_le_bytes());
            data_to_write.extend_from_slice(&entry_data);
        }

        // Seek to end of file
        segment
            .file
            .seek(tokio::io::SeekFrom::End(0))
            .await
            .map_err(LsmError::Io)?;

        // Write all data in one go
        segment
            .file
            .write_all(&data_to_write)
            .await
            .map_err(LsmError::Io)?;

        // Update entry count
        segment.entry_count += entries.len() as u32;
        segment.header.entry_count = segment.entry_count;

        // Update header
        segment.write_header().await?;

        // Single flush for the entire batch
        segment.file.flush().await.map_err(LsmError::Io)?;

        Ok(())
    }

    /// Add entry to group commit batch
    pub async fn write_entry_group_commit(&self, entry: WALEntry) -> Result<()> {
        let notify = Arc::new(Notify::new());
        let notify_clone = notify.clone();

        // Try to add to existing batch or create new one
        let should_commit = {
            let mut batch_guard = self.pending_batch.lock().await;

            match batch_guard.as_mut() {
                Some(batch) => {
                    batch.entries.push(entry);
                    batch.entries.len() >= self.max_batch_size
                }
                None => {
                    let batch = GroupCommitBatch {
                        entries: vec![entry],
                        notify: notify_clone,
                    };
                    *batch_guard = Some(batch);
                    false
                }
            }
        };

        if should_commit {
            // Trigger immediate commit
            self.commit_notify.notify_one();
        }

        // Wait for commit to complete
        notify.notified().await;
        Ok(())
    }

    /// Mark clean shutdown by writing a checkpoint and truncating
    pub async fn mark_clean_shutdown(&self) -> Result<()> {
        tracing::info!("WAL: Marking clean shutdown");

        // Check if there's a current segment before trying to write
        let has_current_segment = {
            let current_segment = self.current_segment.read().await;
            current_segment.is_some()
        };

        if has_current_segment {
            // Write a checkpoint entry if there's an active segment
            let checkpoint_entry = WALEntry::Checkpoint {
                timestamp: utils::timestamp_secs(),
            };

            self.write_entry(&checkpoint_entry).await?;

            // Flush to ensure checkpoint is written
            self.flush().await?;
        } else {
            tracing::info!("WAL: No current segment to write checkpoint, skipping");
        }

        // Truncate all WAL files since we're shutting down cleanly
        // This handles the case where there are no segments or only completed segments
        self.truncate_after_flush().await?;

        tracing::info!("WAL: Clean shutdown marked successfully");
        Ok(())
    }
}
