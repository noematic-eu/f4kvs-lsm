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

        // Write header and durable it — otherwise SIGKILL can leave a 0-byte
        // segment that previously blocked all recovery (early eof).
        segment.write_header().await?;
        segment.file.sync_data().await.map_err(LsmError::Io)?;

        Ok(segment)
    }

    /// Open an existing WAL segment for reading.
    ///
    /// Empty or header-incomplete files (SIGKILL mid-`WALSegment::new` / mid-truncate
    /// rotate) are treated as valid **empty** segments: post-flush truncate creates a
    /// fresh file, and a crash before the first durable put must not block recovery
    /// when SSTables already hold the data (crash-loop 2026-08-12).
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

        let file_len = file.metadata().await.map_err(LsmError::Io)?.len();

        // Read header directly (it's written without size prefix)
        let header_size = bincode::serialized_size(&WALSegmentHeader {
            magic: Self::MAGIC,
            version: Self::VERSION,
            created_at: 0,
            entry_count: 0,
        })
        .map_err(|e| LsmError::Serialization(format!("Failed to get header size: {}", e)))?
            as usize;

        if file_len == 0 || file_len < header_size as u64 {
            warn!(
                "WAL segment {:?} is empty/incomplete ({} bytes < header {}); treating as empty",
                path, file_len, header_size
            );
            let header = WALSegmentHeader {
                magic: Self::MAGIC,
                version: Self::VERSION,
                created_at: 0,
                entry_count: 0,
            };
            return Ok(Self {
                path,
                file,
                header,
                entry_count: 0,
                max_size,
                sync_mode,
            });
        }

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

        // Empty/incomplete file opened as zero-entry segment (see open_for_reading).
        let file_len = self.file.metadata().await.map_err(LsmError::Io)?.len();
        if file_len == 0 || self.header.entry_count == 0 && file_len < 32 {
            // Header may still be present with entry_count=0 — fall through to scan.
            if file_len == 0 {
                return Ok(entries);
            }
        }

        // Seek to after header
        let header_size = bincode::serialized_size(&self.header)
            .map_err(|e| LsmError::Serialization(format!("Failed to get header size: {}", e)))?
            as u64;

        if file_len <= header_size {
            return Ok(entries);
        }

        self.file
            .seek(tokio::io::SeekFrom::Start(header_size))
            .await
            .map_err(LsmError::Io)?;

        // Read entries; a torn last record (SIGKILL mid-write) stops cleanly —
        // prior complete entries remain recoverable.
        loop {
            let size = match self.file.read_u32_le().await {
                Ok(s) => s,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(LsmError::Io(e)),
            };
            if size == 0 {
                break; // End of entries
            }
            // Sanity: absurd sizes mean corruption / torn length prefix.
            if size as u64 > self.max_size {
                warn!(
                    "WAL entry size {} exceeds segment max {}; stopping read ({} entries recovered)",
                    size,
                    self.max_size,
                    entries.len()
                );
                break;
            }

            let mut entry_buffer = vec![0u8; size as usize];
            match self.file.read_exact(&mut entry_buffer).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    warn!(
                        "Torn WAL entry at end of {:?} (need {} bytes); recovered {} complete entries",
                        self.path,
                        size,
                        entries.len()
                    );
                    break;
                }
                Err(e) => return Err(LsmError::Io(e)),
            }

            let entry: WALEntry = match bincode::deserialize(&entry_buffer) {
                Ok(entry) => entry,
                Err(e) => {
                    warn!("Failed to deserialize WAL entry: {}, skipping entry", e);
                    continue;
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

/// Pending WAL entry waiting for group-commit flush.
struct PendingGroupCommitEntry {
    entry: WALEntry,
    ack: tokio::sync::oneshot::Sender<Result<()>>,
}

/// Buffered WAL entries for group commit.
struct GroupCommitQueue {
    pending: Vec<PendingGroupCommitEntry>,
    timing: crate::storage::wal_group_commit::GroupCommitTiming,
}

/// Background group-commit flusher (time/size triggered).
struct GroupCommitFlusher {
    queue: Arc<Mutex<GroupCommitQueue>>,
    current_segment: Arc<RwLock<Option<WALSegment>>>,
    segments: Arc<RwLock<HashMap<u64, PathBuf>>>,
    segment_counter: Arc<std::sync::atomic::AtomicU64>,
    wal_dir: PathBuf,
    config: WalConfig,
}

impl GroupCommitFlusher {
    async fn flush_pending(&self) -> Result<()> {
        let pending = {
            let mut guard = self.queue.lock().await;
            let taken = std::mem::take(&mut guard.pending);
            if !taken.is_empty() {
                guard.timing.clear();
            }
            taken
        };

        if pending.is_empty() {
            return Ok(());
        }

        let entries: Vec<WALEntry> = pending.iter().map(|p| p.entry.clone()).collect();
        let manager = WALManager {
            config: self.config.clone(),
            current_segment: self.current_segment.clone(),
            segments: self.segments.clone(),
            segment_counter: self.segment_counter.clone(),
            wal_dir: self.wal_dir.clone(),
            group_commit_queue: Arc::new(Mutex::new(GroupCommitQueue {
                pending: Vec::new(),
                timing: crate::storage::wal_group_commit::GroupCommitTiming::default(),
            })),
            commit_notify: Arc::new(Notify::new()),
            commit_task: Arc::new(Mutex::new(None)),
        };
        let flush_result = manager.batch_write_entries(&entries).await;

        for waiter in pending {
            let ack_result = flush_result
                .as_ref()
                .map(|_| ())
                .map_err(|e| LsmError::Internal(e.to_string()));
            let _ = waiter.ack.send(ack_result);
        }

        flush_result
    }
}

/// WAL manager with group commit support
pub struct WALManager {
    config: WalConfig,
    current_segment: Arc<RwLock<Option<WALSegment>>>,
    segments: Arc<RwLock<HashMap<u64, PathBuf>>>,
    segment_counter: Arc<std::sync::atomic::AtomicU64>,
    wal_dir: PathBuf,

    // Group commit fields
    group_commit_queue: Arc<Mutex<GroupCommitQueue>>,
    commit_notify: Arc<Notify>,
    commit_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
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
            group_commit_queue: Arc::new(Mutex::new(GroupCommitQueue {
                pending: Vec::new(),
                timing: crate::storage::wal_group_commit::GroupCommitTiming::default(),
            })),
            commit_notify: Arc::new(Notify::new()),
            commit_task: Arc::new(Mutex::new(None)),
        })
    }

    fn group_commit_enabled(&self) -> bool {
        self.config.group_commit_enabled
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

        if self.group_commit_enabled() {
            self.start_group_commit().await?;
        }

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

        if self.group_commit_enabled() {
            self.write_entry_group_commit(entry).await
        } else {
            self.write_entry(&entry).await
        }
    }

    /// Write a delete operation to WAL
    pub async fn write_delete(&self, key: &str) -> Result<()> {
        let timestamp = utils::timestamp_secs();

        let entry = WALEntry::Delete {
            key: key.to_string(),
            timestamp,
        };

        if self.group_commit_enabled() {
            self.write_entry_group_commit(entry).await
        } else {
            self.write_entry(&entry).await
        }
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
        if self.group_commit_enabled() {
            self.flush_pending_group_commit().await?;
        }

        let mut current_segment = self.current_segment.write().await;
        if let Some(segment) = current_segment.as_mut() {
            // Flush the current segment to disk
            segment.flush().await?;
        }
        Ok(())
    }

    /// List `segment_*.wal` paths on disk (any process lifetime — includes phantoms
    /// never tracked in `self.segments` after a prior session crash/restart).
    fn list_segment_files_on_disk(&self) -> Vec<std::path::PathBuf> {
        let mut paths = Vec::new();
        if !self.wal_dir.exists() {
            return paths;
        }
        if let Ok(entries) = std::fs::read_dir(&self.wal_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name.starts_with("segment_") && name.ends_with(".wal") {
                        paths.push(path);
                    }
                }
            }
        }
        paths
    }

    /// Parse hex segment id from `segment_{:016x}.wal`.
    fn parse_segment_id(file_name: &str) -> Option<u64> {
        file_name
            .strip_prefix("segment_")
            .and_then(|s| s.strip_suffix(".wal"))
            .and_then(|id| u64::from_str_radix(id, 16).ok())
    }

    async fn remove_file_with_retries(path: &std::path::Path) -> Result<()> {
        if !path.exists() {
            return Ok(());
        }
        let mut retry_count = 0;
        let max_retries = 3;
        loop {
            match tokio::fs::remove_file(path).await {
                Ok(_) => return Ok(()),
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
    }

    /// Truncate WAL after successful flush to LSM-Tree.
    ///
    /// Removes **all** `segment_*.wal` files on disk — not only the current
    /// in-memory handle / `self.segments` map. Residual segments from prior
    /// process sessions (never re-registered into `segments` on open) must be
    /// wiped here; leaving them causes recovery to re-apply stale puts into the
    /// memtable, which then re-flush with higher sequence numbers and permanently
    /// win L0 timestamp merges (crash-loop stale-value bug, 2026-08-12).
    pub async fn truncate_after_flush(&self) -> Result<()> {
        tracing::info!("WAL: Starting truncate_after_flush (filesystem-wide)");

        let initial_file_count = self.list_segment_files_on_disk().len();
        tracing::info!(
            "WAL: Found {} segment file(s) before truncation",
            initial_file_count
        );

        // Drop the live handle first so the OS releases the file.
        {
            let mut current_segment = self.current_segment.write().await;
            if let Some(mut segment) = current_segment.take() {
                let path = segment.path().to_path_buf();
                if let Err(e) = segment.close().await {
                    tracing::warn!("WAL: Error closing current segment {:?}: {}", path, e);
                }
                drop(segment);
            }
        }
        // Forget in-memory completed-segment bookkeeping; disk scan is authoritative.
        {
            let mut segments = self.segments.write().await;
            segments.clear();
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Wipe every residual segment file on disk (prior sessions + this session).
        let to_remove = self.list_segment_files_on_disk();
        tracing::info!(
            "WAL: Removing {} segment file(s) from disk after memtable flush",
            to_remove.len()
        );
        for path in &to_remove {
            Self::remove_file_with_retries(path).await?;
            if path.exists() {
                return Err(LsmError::Io(std::io::Error::other(format!(
                    "Failed to remove WAL segment after retries: {:?}",
                    path
                ))));
            }
        }

        let remaining = self.list_segment_files_on_disk().len();
        if remaining > 0 {
            return Err(LsmError::Io(std::io::Error::other(format!(
                "WAL truncation incomplete: {} segment file(s) still on disk after wipe",
                remaining
            ))));
        }
        tracing::info!(
            "WAL: All segment files removed (started with {})",
            initial_file_count
        );

        // Fresh empty segment for subsequent writes.
        self.rotate_segment().await?;
        tracing::info!("WAL: Created new segment after truncation");
        tracing::info!("WAL: truncate_after_flush completed successfully");
        Ok(())
    }

    /// Verify truncation: in-memory map empty, and at most one live segment file
    /// (the fresh post-truncate current segment).
    pub async fn verify_truncated(&self) -> Result<bool> {
        let current_segment = self.current_segment.read().await;
        if current_segment.is_none() {
            tracing::warn!(
                "WAL: No current segment exists during verification - truncation failed"
            );
            return Ok(false);
        }
        let current_path = current_segment.as_ref().map(|s| s.path().to_path_buf());

        let segments = self.segments.read().await;
        if !segments.is_empty() {
            tracing::warn!(
                "WAL: {} completed segments still tracked during verification",
                segments.len()
            );
            return Ok(false);
        }

        let on_disk = self.list_segment_files_on_disk();
        // Exactly one segment file expected: the fresh current after rotate.
        if on_disk.len() > 1 {
            tracing::warn!(
                "WAL: {} segment files on disk during verification (expected 1): {:?}",
                on_disk.len(),
                on_disk
            );
            return Ok(false);
        }
        if let (Some(cur), Some(disk)) = (current_path.as_ref(), on_disk.first()) {
            if cur != disk {
                tracing::warn!(
                    "WAL: current segment path {:?} != sole disk segment {:?}",
                    cur,
                    disk
                );
                return Ok(false);
            }
        }

        tracing::info!("WAL: Verification successful - only fresh current segment remains");
        Ok(true)
    }

    /// Read all entries from WAL files on disk (startup recovery).
    /// Segments are read in ascending segment-id order; within equal timestamps
    /// a stable sort preserves that order (second-granularity timestamps alone
    /// are not a total order across residual multi-session segments).
    pub async fn read_entries_from_disk(&self) -> Result<Vec<WALEntry>> {
        let mut all_entries = Vec::new();
        if !self.wal_dir.exists() {
            return Ok(all_entries);
        }

        let mut segment_files: Vec<(u64, std::path::PathBuf)> = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.wal_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let Some(file_name) = path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                if !file_name.ends_with(".wal") || !file_name.starts_with("segment_") {
                    continue;
                }
                let id = Self::parse_segment_id(file_name).unwrap_or(u64::MAX);
                segment_files.push((id, path));
            }
        }
        segment_files.sort_by_key(|(id, _)| *id);

        let segment_files_found = segment_files.len();
        let mut segments_read_ok = 0usize;
        let mut read_errors = Vec::new();

        for (_id, path) in segment_files {
            let file_name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("?")
                .to_string();
            match WALSegment::open_for_reading(
                path,
                self.config.segment_size as u64,
                self.config.sync_mode,
            )
            .await
            {
                Ok(mut segment) => match segment.read_entries().await {
                    Ok(entries) => {
                        segments_read_ok += 1;
                        all_entries.extend(entries);
                    }
                    Err(e) => {
                        read_errors.push(format!("{file_name}: {e}"));
                    }
                },
                Err(e) => {
                    read_errors.push(format!("{file_name}: {e}"));
                }
            }
        }

        if segment_files_found > 0 && segments_read_ok == 0 {
            return Err(LsmError::Wal(format!(
                "Failed to recover from {} WAL segment(s): {}",
                segment_files_found,
                read_errors.join("; ")
            )));
        }

        if !read_errors.is_empty() {
            warn!(
                "Skipped {} corrupted WAL segment(s) during recovery: {}",
                read_errors.len(),
                read_errors.join("; ")
            );
        }

        // Stable sort: equal timestamps keep segment-id then in-file order.
        all_entries.sort_by_key(|e| match e {
            WALEntry::Put { timestamp, .. }
            | WALEntry::Delete { timestamp, .. }
            | WALEntry::Flush { timestamp, .. }
            | WALEntry::Checkpoint { timestamp, .. } => *timestamp,
        });
        Ok(all_entries)
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

        if self.group_commit_enabled() {
            self.flush_pending_group_commit().await?;
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

    /// Internal method to write multiple entries in batch.
    ///
    /// **Locking:** never call [`Self::rotate_segment`] while holding
    /// `current_segment` write guard. A previous bug did
    /// `let segment = guard.as_mut()?; …; let _ = segment; rotate()` — that only
    /// dropped the `&mut` reborrow, not the `RwLockWriteGuard`, so rotation
    /// deadlocked on the same task (observed ~13–15k × 4 KiB batch puts when the
    /// default 64 MiB segment filled). Match frame WAL: `drop(guard)` then rotate.
    async fn batch_write_entries(&self, entries: &[WALEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        tracing::debug!("WAL: Starting batch write of {} entries", entries.len());

        // Serialize outside the segment lock.
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

        // Rotate if the open segment is already at capacity (read path).
        {
            let current_segment = self.current_segment.read().await;
            if let Some(segment) = current_segment.as_ref() {
                if segment.should_rotate().await? {
                    tracing::info!("WAL: Rotating segment before batch write");
                    drop(current_segment);
                    self.rotate_segment().await?;
                }
            }
        }

        // If this batch would overflow the current file, rotate first.
        // Must release the write guard before rotate_segment (it takes write again).
        let needs_rotate = {
            let guard = self.current_segment.write().await;
            let segment = guard.as_ref().ok_or_else(|| {
                tracing::error!("WAL: No current WAL segment available for batch write");
                LsmError::Internal("No current WAL segment".to_string())
            })?;
            let current_size = segment.file.metadata().await.map_err(LsmError::Io)?.len();
            let overflow = current_size + data_to_write.len() as u64 > segment.max_size;
            if overflow {
                tracing::info!(
                    "WAL: Batch write would exceed segment size ({} + {} > {}), rotating segment",
                    current_size,
                    data_to_write.len(),
                    segment.max_size
                );
            }
            overflow
        }; // write guard dropped here

        if needs_rotate {
            self.rotate_segment().await?;
        }

        // Append + single sync on the (possibly new) segment.
        {
            let mut guard = self.current_segment.write().await;
            let segment = guard.as_mut().ok_or_else(|| {
                tracing::error!("WAL: No current WAL segment available after rotation");
                LsmError::Internal("No current WAL segment".to_string())
            })?;

            segment
                .file
                .seek(tokio::io::SeekFrom::End(0))
                .await
                .map_err(LsmError::Io)?;
            segment
                .file
                .write_all(&data_to_write)
                .await
                .map_err(LsmError::Io)?;
            tracing::debug!(
                "WAL: Successfully wrote {} bytes to segment",
                data_to_write.len()
            );

            segment.entry_count += entries.len() as u32;
            segment.header.entry_count = segment.entry_count;
            segment.sync_header_and_flush().await?;
        }

        Ok(())
    }

    /// Start the group commit background task
    pub async fn start_group_commit(&self) -> Result<()> {
        let mut task_guard = self.commit_task.lock().await;
        if task_guard.is_some() {
            return Ok(());
        }

        let queue = self.group_commit_queue.clone();
        let commit_notify = self.commit_notify.clone();
        let current_segment = self.current_segment.clone();
        let segments = self.segments.clone();
        let segment_counter = self.segment_counter.clone();
        let wal_dir = self.wal_dir.clone();
        let config = self.config.clone();
        let max_batch_wait = self.config.group_commit_max_wait;
        let idle_flush = self.config.group_commit_idle_flush;

        let commit_task = tokio::spawn(async move {
            let manager = GroupCommitFlusher {
                queue: queue.clone(),
                current_segment,
                segments,
                segment_counter,
                wal_dir,
                config,
            };

            loop {
                let deadline = {
                    let guard = queue.lock().await;
                    crate::storage::wal_group_commit::next_flush_deadline(
                        &guard.timing,
                        guard.pending.len(),
                        max_batch_wait,
                        idle_flush,
                    )
                };

                let mut flush_now = false;
                if let Some(deadline) = deadline {
                    tokio::select! {
                        _ = tokio::time::sleep_until(deadline) => {
                            flush_now = true;
                        }
                        _ = commit_notify.notified() => {
                            let guard = queue.lock().await;
                            flush_now = guard.pending.len() >= manager.config.group_commit_max_batch_size;
                        }
                    }
                } else {
                    commit_notify.notified().await;
                }

                if flush_now {
                    if let Err(e) = manager.flush_pending().await {
                        tracing::error!("Group commit flush failed: {}", e);
                    }
                }
            }
        });

        *task_guard = Some(commit_task);
        Ok(())
    }

    /// Flush any buffered group-commit entries to WAL (one durable batch).
    pub async fn flush_pending_group_commit(&self) -> Result<()> {
        let pending = {
            let mut guard = self.group_commit_queue.lock().await;
            let taken = std::mem::take(&mut guard.pending);
            if !taken.is_empty() {
                guard.timing.clear();
            }
            taken
        };

        if pending.is_empty() {
            return Ok(());
        }

        let entries: Vec<WALEntry> = pending.iter().map(|p| p.entry.clone()).collect();
        let flush_result = self.batch_write_entries(&entries).await;

        for waiter in pending {
            let ack_result = flush_result
                .as_ref()
                .map(|_| ())
                .map_err(|e| LsmError::Internal(e.to_string()));
            let _ = waiter.ack.send(ack_result);
        }

        flush_result
    }

    /// Buffer a WAL entry for group commit; returns after the entry is durable.
    pub async fn write_entry_group_commit(&self, entry: WALEntry) -> Result<()> {
        let (rx, _batch_full) = {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let mut guard = self.group_commit_queue.lock().await;
            let was_empty = guard.pending.is_empty();
            guard.timing.record_enqueue(was_empty);
            guard.pending.push(PendingGroupCommitEntry { entry, ack: tx });
            let batch_full = guard.pending.len() >= self.config.group_commit_max_batch_size;
            (rx, batch_full)
        };

        self.commit_notify.notify_one();

        if !self.config.group_commit_wait_durable {
            return Ok(());
        }

        match rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(LsmError::Internal(
                "Group commit waiter dropped before flush".to_string(),
            )),
        }
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
            if self.group_commit_enabled() {
                self.flush_pending_group_commit().await?;
            }

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

#[cfg(test)]
mod batch_rotate_tests {
    use super::*;
    use crate::core::config::{WalConfig, WalSyncMode};
    use f4kvs_value::Value;
    use std::time::Duration;
    use tokio::time::timeout;

    /// Repro of meso hang: batch_write across a full segment must not deadlock.
    /// Pre-fix: `let _ = segment_ref` left the write guard held → rotate waited forever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_write_rotates_when_segment_full_no_deadlock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = WalConfig::default();
        cfg.dir = dir.path().join("wal");
        cfg.engine = crate::core::config::WalEngine::Segment;
        cfg.sync_mode = WalSyncMode::Fsync;
        // Small segment so a few 4KiB batches force rotation quickly.
        cfg.segment_size = 256 * 1024; // 256 KiB
        cfg.group_commit_enabled = false;

        let wal = WALManager::new(&cfg).expect("wal new");
        wal.initialize().await.expect("wal init");

        let payload = Value::Bytes(vec![b'x'; 4 * 1024]);
        let batch_size = 20;
        let batches = 40; // well past 256 KiB of payload

        let work = async {
            for b in 0..batches {
                let items: Vec<(String, Value)> = (0..batch_size)
                    .map(|i| {
                        let k = format!("chunk:{:05}:{:03}", b, i);
                        (k, payload.clone())
                    })
                    .collect();
                wal.batch_write_operations(&items)
                    .await
                    .unwrap_or_else(|e| panic!("batch {b} failed: {e}"));
            }
        };

        timeout(Duration::from_secs(30), work)
            .await
            .expect("batch_write across segment rotation hung (write-lock deadlock)");

        let entries = wal.read_entries_from_disk().await.expect("read");
        assert_eq!(entries.len(), batch_size * batches);

        // More than one segment file should exist after rotation.
        let seg_count = std::fs::read_dir(&cfg.dir)
            .expect("read wal dir")
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("segment_") && n.ends_with(".wal"))
                    .unwrap_or(false)
            })
            .count();
        assert!(
            seg_count >= 2,
            "expected multiple WAL segments after overflow, got {seg_count}"
        );
    }
}
