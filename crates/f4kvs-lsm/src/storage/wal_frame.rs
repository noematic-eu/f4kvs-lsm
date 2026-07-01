//! SQLite-style frame WAL — incremental `sync_data` per commit instead of `sync_all`.

use crate::core::config::{WalConfig, WalSyncMode};
use crate::error::{LsmError, Result};
use crate::storage::wal::WALEntry;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FrameWalHeader {
    magic: [u8; 4],
    version: u8,
    created_at: u64,
    entry_count: u32,
    page_size: u32,
}

pub struct FrameWalSegment {
    path: PathBuf,
    file: File,
    header: FrameWalHeader,
    entry_count: u32,
    max_size: u64,
    sync_mode: WalSyncMode,
    synced_offset: u64,
}

impl FrameWalSegment {
    const MAGIC: [u8; 4] = [b'W', b'A', b'L', b'F'];
    const VERSION: u8 = 1;

    pub async fn new(
        path: PathBuf,
        max_size: u64,
        sync_mode: WalSyncMode,
        page_size: u32,
    ) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(false)
            .open(&path)
            .await
            .map_err(LsmError::Io)?;

        let header = FrameWalHeader {
            magic: Self::MAGIC,
            version: Self::VERSION,
            created_at: utils::timestamp_secs(),
            entry_count: 0,
            page_size,
        };

        let mut segment = Self {
            path,
            file,
            header: header.clone(),
            entry_count: 0,
            max_size,
            sync_mode,
            synced_offset: 0,
        };
        segment.write_header().await?;
        segment.synced_offset = segment.file.metadata().await.map_err(LsmError::Io)?.len();
        Ok(segment)
    }

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

        let header_size = bincode::serialized_size(&FrameWalHeader {
            magic: Self::MAGIC,
            version: Self::VERSION,
            created_at: 0,
            entry_count: 0,
            page_size: 0,
        })
        .map_err(|e| LsmError::Serialization(format!("Failed to get header size: {}", e)))?
            as usize;

        let mut header_buffer = vec![0u8; header_size];
        file.read_exact(&mut header_buffer)
            .await
            .map_err(LsmError::Io)?;

        let header: FrameWalHeader = bincode::deserialize(&header_buffer)
            .map_err(|e| LsmError::Serialization(format!("Failed to deserialize header: {}", e)))?;

        if header.magic != Self::MAGIC {
            return Err(LsmError::Corruption("Invalid frame WAL magic".to_string()));
        }

        let len = file.metadata().await.map_err(LsmError::Io)?.len();

        Ok(Self {
            path,
            file,
            header: header.clone(),
            entry_count: header.entry_count,
            max_size,
            sync_mode,
            synced_offset: len,
        })
    }

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

    pub async fn write_entry(&mut self, entry: &WALEntry) -> Result<bool> {
        self.file
            .seek(tokio::io::SeekFrom::End(0))
            .await
            .map_err(LsmError::Io)?;

        let entry_data = bincode::serialize(entry)
            .map_err(|e| LsmError::Serialization(format!("Failed to serialize entry: {}", e)))?;

        let current_size = self.file.metadata().await.map_err(LsmError::Io)?.len();
        let entry_size = entry_data.len() as u64 + 4;
        if current_size + entry_size > self.max_size {
            return Ok(false);
        }

        let size = entry_data.len() as u32;
        self.file.write_u32_le(size).await.map_err(LsmError::Io)?;
        self.file
            .write_all(&entry_data)
            .await
            .map_err(LsmError::Io)?;

        self.entry_count += 1;
        self.header.entry_count = self.entry_count;
        self.sync_after_flush().await?;
        Ok(true)
    }

    pub(crate) async fn sync_header_and_flush(&mut self) -> Result<()> {
        self.write_header().await?;
        self.sync_after_flush().await
    }

    async fn sync_after_flush(&mut self) -> Result<()> {
        self.file.flush().await.map_err(LsmError::Io)?;

        match self.sync_mode {
            WalSyncMode::None | WalSyncMode::Flush => {}
            WalSyncMode::Fsync => {
                let start = std::time::Instant::now();
                self.file.sync_data().await.map_err(LsmError::Io)?;
                self.synced_offset = self.file.metadata().await.map_err(LsmError::Io)?.len();
                debug!(
                    "Frame WAL synced (sync_data) in {:?}ms, offset={}",
                    start.elapsed().as_millis(),
                    self.synced_offset
                );
            }
            WalSyncMode::FsyncAsync => {
                let path = self.path.clone();
                let synced_offset = self.synced_offset;
                std::thread::spawn(move || {
                    let start = std::time::Instant::now();
                    match std::fs::OpenOptions::new().write(true).open(&path) {
                        Ok(file) => {
                            if let Err(e) = file.sync_data() {
                                warn!("Background frame sync_data failed for {:?}: {}", path, e);
                            } else {
                                debug!(
                                    "Frame WAL async sync_data in {:?}ms (from offset {})",
                                    start.elapsed().as_millis(),
                                    synced_offset
                                );
                            }
                        }
                        Err(e) => warn!("Background frame sync could not open {:?}: {}", path, e),
                    }
                });
            }
        }
        Ok(())
    }

    pub async fn should_rotate(&self) -> Result<bool> {
        let metadata = self.file.metadata().await.map_err(LsmError::Io)?;
        Ok(metadata.len() >= self.max_size)
    }

    pub async fn read_entries(&mut self) -> Result<Vec<WALEntry>> {
        let mut entries = Vec::new();
        let header_size = bincode::serialized_size(&self.header)
            .map_err(|e| LsmError::Serialization(format!("Failed to get header size: {}", e)))?
            as u64;

        self.file
            .seek(tokio::io::SeekFrom::Start(header_size))
            .await
            .map_err(LsmError::Io)?;

        while let Ok(size) = self.file.read_u32_le().await {
            if size == 0 {
                break;
            }
            let mut entry_buffer = vec![0u8; size as usize];
            self.file
                .read_exact(&mut entry_buffer)
                .await
                .map_err(LsmError::Io)?;
            if let Ok(entry) = bincode::deserialize(&entry_buffer) {
                entries.push(entry);
            }
        }
        Ok(entries)
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.sync_header_and_flush().await
    }

    pub async fn close(&mut self) -> Result<()> {
        self.write_header().await?;
        self.file.sync_data().await.map_err(LsmError::Io)?;
        Ok(())
    }
}

struct PendingGroupCommitEntry {
    entry: WALEntry,
    ack: tokio::sync::oneshot::Sender<Result<()>>,
}

struct GroupCommitQueue {
    pending: Vec<PendingGroupCommitEntry>,
    timing: crate::storage::wal_group_commit::GroupCommitTiming,
}

struct FrameGroupCommitFlusher {
    queue: Arc<Mutex<GroupCommitQueue>>,
    commit_notify: Arc<Notify>,
    current_segment: Arc<RwLock<Option<FrameWalSegment>>>,
    segments: Arc<RwLock<HashMap<u64, PathBuf>>>,
    segment_counter: Arc<std::sync::atomic::AtomicU64>,
    wal_dir: PathBuf,
    config: WalConfig,
}

impl FrameGroupCommitFlusher {
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
        let manager = FrameWalManager {
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

/// Frame WAL manager — same surface as `WALManager`, different sync semantics.
pub struct FrameWalManager {
    config: WalConfig,
    current_segment: Arc<RwLock<Option<FrameWalSegment>>>,
    segments: Arc<RwLock<HashMap<u64, PathBuf>>>,
    segment_counter: Arc<std::sync::atomic::AtomicU64>,
    wal_dir: PathBuf,
    group_commit_queue: Arc<Mutex<GroupCommitQueue>>,
    commit_notify: Arc<Notify>,
    commit_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl FrameWalManager {
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

    fn page_size(&self) -> u32 {
        self.config.frame_page_size.max(512) as u32
    }

    pub async fn initialize(&self) -> Result<()> {
        if !self.wal_dir.exists() {
            tokio::fs::create_dir_all(&self.wal_dir)
                .await
                .map_err(LsmError::Io)?;
        }
        self.scan_existing_segments().await?;
        self.rotate_segment().await?;
        if self.group_commit_enabled() {
            self.start_group_commit().await?;
        }
        Ok(())
    }

    async fn scan_existing_segments(&self) -> Result<()> {
        if !self.wal_dir.exists() {
            return Ok(());
        }
        let mut max_segment_id = 0u64;
        if let Ok(entries) = std::fs::read_dir(&self.wal_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                    if file_name.starts_with("frame_") && file_name.ends_with(".wal") {
                        if let Some(id_str) = file_name
                            .strip_prefix("frame_")
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
        self.segment_counter
            .store(max_segment_id + 1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    async fn rotate_segment(&self) -> Result<()> {
        let mut current_segment = self.current_segment.write().await;
        if let Some(mut segment) = current_segment.take() {
            segment.close().await?;
            let segment_id = self
                .segment_counter
                .load(std::sync::atomic::Ordering::SeqCst);
            let mut segments = self.segments.write().await;
            segments.insert(segment_id, segment.path().clone());
        }

        let segment_id = self
            .segment_counter
            .load(std::sync::atomic::Ordering::SeqCst);
        let segment_path = self
            .wal_dir
            .join(format!("frame_{:016x}.wal", segment_id));
        let segment = FrameWalSegment::new(
            segment_path,
            self.config.segment_size as u64,
            self.config.sync_mode,
            self.page_size(),
        )
        .await?;
        *current_segment = Some(segment);
        drop(current_segment);
        self.segment_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

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

    pub async fn write_delete(&self, key: &str) -> Result<()> {
        let entry = WALEntry::Delete {
            key: key.to_string(),
            timestamp: utils::timestamp_secs(),
        };
        if self.group_commit_enabled() {
            self.write_entry_group_commit(entry).await
        } else {
            self.write_entry(&entry).await
        }
    }

    pub async fn write_entry(&self, entry: &WALEntry) -> Result<()> {
        let needs_rotation = {
            let mut guard = self.current_segment.write().await;
            let segment = guard.as_mut().ok_or_else(|| {
                LsmError::Internal("No current frame WAL segment".to_string())
            })?;
            let success = segment.write_entry(entry).await?;
            !success
        };
        if needs_rotation {
            self.rotate_segment().await?;
            let mut guard = self.current_segment.write().await;
            let segment = guard.as_mut().ok_or_else(|| {
                LsmError::Internal("Failed to create frame WAL segment".to_string())
            })?;
            segment.write_entry(entry).await?;
        }
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        if self.group_commit_enabled() {
            self.flush_pending_group_commit().await?;
        }
        let mut current_segment = self.current_segment.write().await;
        if let Some(segment) = current_segment.as_mut() {
            segment.flush().await?;
        }
        Ok(())
    }

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

    async fn batch_write_entries(&self, entries: &[WALEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        {
            let current_segment = self.current_segment.read().await;
            if let Some(segment) = current_segment.as_ref() {
                if segment.should_rotate().await? {
                    drop(current_segment);
                    self.rotate_segment().await?;
                }
            }
        }

        let mut current_segment = self.current_segment.write().await;
        let segment = current_segment.as_mut().ok_or_else(|| {
            LsmError::Internal("No current frame WAL segment for batch".to_string())
        })?;

        segment
            .file
            .seek(tokio::io::SeekFrom::End(0))
            .await
            .map_err(LsmError::Io)?;

        let mut data_to_write = Vec::new();
        for entry in entries {
            let entry_data = bincode::serialize(entry)
                .map_err(|e| LsmError::Serialization(format!("Failed to serialize: {}", e)))?;
            let size = entry_data.len() as u32;
            data_to_write.extend_from_slice(&size.to_le_bytes());
            data_to_write.extend_from_slice(&entry_data);
        }

        let current_size = segment.file.metadata().await.map_err(LsmError::Io)?.len();
        if current_size + data_to_write.len() as u64 > segment.max_size {
            drop(current_segment);
            self.rotate_segment().await?;
            let mut current_segment = self.current_segment.write().await;
            let segment = current_segment.as_mut().ok_or_else(|| {
                LsmError::Internal("Failed frame WAL segment after rotation".to_string())
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
            segment.entry_count += entries.len() as u32;
            segment.header.entry_count = segment.entry_count;
            segment.sync_header_and_flush().await?;
        } else {
            segment
                .file
                .write_all(&data_to_write)
                .await
                .map_err(LsmError::Io)?;
            segment.entry_count += entries.len() as u32;
            segment.header.entry_count = segment.entry_count;
            segment.sync_header_and_flush().await?;
        }
        Ok(())
    }

    pub async fn read_entries_from_disk(&self) -> Result<Vec<WALEntry>> {
        let mut all_entries = Vec::new();
        if !self.wal_dir.exists() {
            return Ok(all_entries);
        }
        if let Ok(entries) = std::fs::read_dir(&self.wal_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let Some(file_name) = path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                if !file_name.ends_with(".wal") || !file_name.starts_with("frame_") {
                    continue;
                }
                if let Ok(mut segment) = FrameWalSegment::open_for_reading(
                    path,
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
        }
        all_entries.sort_by_key(entry_timestamp);
        Ok(all_entries)
    }

    pub async fn read_entries(&self) -> Result<Vec<WALEntry>> {
        let mut all_entries = Vec::new();
        let current_segment = self.current_segment.read().await;
        if let Some(segment) = current_segment.as_ref() {
            let mut segment_clone = FrameWalSegment::open_for_reading(
                segment.path().clone(),
                self.config.segment_size as u64,
                self.config.sync_mode,
            )
            .await?;
            all_entries.extend(segment_clone.read_entries().await?);
        }
        let segments = self.segments.read().await;
        for (_, path) in segments.iter() {
            if let Ok(mut segment) = FrameWalSegment::open_for_reading(
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
        all_entries.sort_by_key(entry_timestamp);
        Ok(all_entries)
    }

    pub async fn truncate_after_flush(&self) -> Result<()> {
        let mut current_segment = self.current_segment.write().await;
        if let Some(mut segment) = current_segment.take() {
            let path = segment.path().to_path_buf();
            segment.close().await.ok();
            drop(segment);
            tokio::time::sleep(Duration::from_millis(10)).await;
            if path.exists() {
                tokio::fs::remove_file(&path).await.map_err(LsmError::Io)?;
            }
        }
        let mut segments = self.segments.write().await;
        for (_, path) in segments.iter() {
            if path.exists() {
                tokio::fs::remove_file(path).await.ok();
            }
        }
        segments.clear();
        drop(current_segment);
        self.rotate_segment().await
    }

    pub async fn verify_truncated(&self) -> Result<bool> {
        let current_segment = self.current_segment.read().await;
        if current_segment.is_none() {
            return Ok(false);
        }
        let segments = self.segments.read().await;
        Ok(segments.is_empty())
    }

    pub async fn mark_clean_shutdown(&self) -> Result<()> {
        let has_current = {
            let current_segment = self.current_segment.read().await;
            current_segment.is_some()
        };
        if has_current {
            if self.group_commit_enabled() {
                self.flush_pending_group_commit().await?;
            }
            let checkpoint = WALEntry::Checkpoint {
                timestamp: utils::timestamp_secs(),
            };
            self.write_entry(&checkpoint).await?;
            self.flush().await?;
        }
        self.truncate_after_flush().await
    }

    pub async fn cleanup_old_segments(&self, retention_period: Duration) -> Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| LsmError::Internal(e.to_string()))?
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
        for id in to_remove {
            if let Some(path) = segments.remove(&id) {
                tokio::fs::remove_file(&path).await.ok();
            }
        }
        Ok(())
    }

    pub async fn cleanup_flushed_segments(&self, grace_period: Duration) -> Result<()> {
        self.cleanup_old_segments(grace_period).await
    }

    pub async fn force_cleanup(&self) -> Result<()> {
        let mut segments = self.segments.write().await;
        for (_, path) in segments.drain() {
            tokio::fs::remove_file(&path).await.ok();
        }
        Ok(())
    }

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
            let manager = FrameGroupCommitFlusher {
                queue: queue.clone(),
                commit_notify: commit_notify.clone(),
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
                        tracing::error!("Frame group commit flush failed: {}", e);
                    }
                }
            }
        });
        *task_guard = Some(commit_task);
        Ok(())
    }

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

    async fn write_entry_group_commit(&self, entry: WALEntry) -> Result<()> {
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
                "Frame group commit waiter dropped".to_string(),
            )),
        }
    }
}

fn entry_timestamp(entry: &WALEntry) -> u64 {
    match entry {
        WALEntry::Put { timestamp, .. }
        | WALEntry::Delete { timestamp, .. }
        | WALEntry::Flush { timestamp, .. }
        | WALEntry::Checkpoint { timestamp, .. } => *timestamp,
    }
}