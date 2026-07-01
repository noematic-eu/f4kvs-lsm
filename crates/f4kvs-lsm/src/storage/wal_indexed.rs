//! WAL v2 — per-frame micro-files + `wal.idx` durable watermark.
//!
//! Each committed frame is a fixed-size file under `wal/frames/{segment_id}/`.
//! On macOS, `sync_data` on an 8 KiB file is cheap; syncing a pre-allocated
//! multi-MiB segment file falls back to whole-file sync and costs ~2× segment WAL.

use crate::core::config::{WalConfig, WalSyncMode};
use crate::error::{LsmError, Result};
use crate::storage::wal::WALEntry;
use crate::storage::wal_index::{WalIndexFile, WalIndexHeader};
use crate::storage::wal_sync;
use crate::utils;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;

/// Minimum frame payload to fit ~4 KiB values + key in one WAL record.
pub const INDEXED_MIN_FRAME_SIZE: usize = 8192;

fn frames_root(wal_dir: &Path) -> PathBuf {
    wal_dir.join("frames")
}

fn segment_frames_dir(wal_dir: &Path, segment_id: u32) -> PathBuf {
    frames_root(wal_dir).join(format!("{segment_id:016x}"))
}

fn frame_path(frames_dir: &Path, frame_id: u32) -> PathBuf {
    frames_dir.join(format!("frame_{frame_id:08}.wal"))
}

pub struct IndexedWalSegment {
    frames_dir: PathBuf,
    frame_size: usize,
    data_frame_count: u32,
    segment_id: u32,
    sync_mode: WalSyncMode,
    staging_frame: u32,
    staging: Vec<u8>,
    entries_in_staging: u32,
}

impl IndexedWalSegment {
    pub async fn create(
        frames_dir: PathBuf,
        segment_id: u32,
        frame_size: usize,
        data_frame_count: u32,
        sync_mode: WalSyncMode,
    ) -> Result<Self> {
        tokio::fs::create_dir_all(&frames_dir)
            .await
            .map_err(LsmError::Io)?;

        Ok(Self {
            frames_dir,
            frame_size,
            data_frame_count,
            segment_id,
            sync_mode,
            staging_frame: 1,
            staging: Vec::with_capacity(frame_size),
            entries_in_staging: 0,
        })
    }

    pub async fn open_existing(
        frames_dir: PathBuf,
        segment_id: u32,
        frame_size: usize,
        data_frame_count: u32,
        sync_mode: WalSyncMode,
        resume_frame: u32,
    ) -> Result<Self> {
        if !frames_dir.exists() {
            return Err(LsmError::Corruption(format!(
                "indexed wal frames dir missing: {:?}",
                frames_dir
            )));
        }

        Ok(Self {
            frames_dir,
            frame_size,
            data_frame_count,
            segment_id,
            sync_mode,
            staging_frame: resume_frame.max(1),
            staging: Vec::with_capacity(frame_size),
            entries_in_staging: 0,
        })
    }

    fn encode_entry(entry: &WALEntry) -> Result<Vec<u8>> {
        let payload = bincode::serialize(entry)
            .map_err(|e| LsmError::Serialization(format!("wal entry: {e}")))?;
        let mut out = Vec::with_capacity(4 + payload.len());
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&payload);
        Ok(out)
    }

    fn frame_file_size(&self, bytes_len: usize) -> usize {
        self.frame_size.max(bytes_len)
    }

    async fn write_frame_file(&self, frame_id: u32, bytes: &[u8]) -> Result<()> {
        let file_size = self.frame_file_size(bytes.len());
        let path = frame_path(&self.frames_dir, frame_id);
        let mut frame = vec![0u8; file_size];
        frame[..bytes.len()].copy_from_slice(bytes);

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)
            .await
            .map_err(LsmError::Io)?;
        let len = file.metadata().await.map_err(LsmError::Io)?.len();
        if len != file_size as u64 {
            file.set_len(file_size as u64).await.map_err(LsmError::Io)?;
        }
        file.write_all(&frame).await.map_err(LsmError::Io)?;
        file.flush().await.map_err(LsmError::Io)?;

        wal_sync::sync_path(&path, self.sync_mode).await?;
        Ok(())
    }

    fn list_committed_frame_ids(&self) -> Result<Vec<u32>> {
        let mut ids = Vec::new();
        if !self.frames_dir.exists() {
            return Ok(ids);
        }
        for entry in std::fs::read_dir(&self.frames_dir).map_err(LsmError::Io)? {
            let entry = entry.map_err(LsmError::Io)?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if let Some(id_str) = name
                .strip_prefix("frame_")
                .and_then(|s| s.strip_suffix(".wal"))
            {
                if let Ok(id) = id_str.parse::<u32>() {
                    ids.push(id);
                }
            }
        }
        ids.sort_unstable();
        Ok(ids)
    }

    async fn flush_staging(
        &mut self,
        index: &mut WalIndexFile,
        index_state: &mut WalIndexHeader,
    ) -> Result<()> {
        if self.staging.is_empty() {
            return Ok(());
        }
        if self.staging_frame > self.data_frame_count {
            return Err(LsmError::Internal("indexed wal segment full".into()));
        }

        let frame_id = self.staging_frame;
        let used = self.staging.len() as u32;
        let entries = self.entries_in_staging as u64;
        let staging = self.staging.clone();

        self.write_frame_file(frame_id, &staging).await?;

        index_state.committed_frame = frame_id;
        index_state.committed_offset = used;
        index_state.entry_count = index_state.entry_count.saturating_add(entries);
        index.commit_volatile(*index_state).await?;

        self.staging.clear();
        self.entries_in_staging = 0;
        self.staging_frame = self.staging_frame.saturating_add(1);
        Ok(())
    }

    async fn commit_frame(
        &mut self,
        frame_id: u32,
        bytes: &[u8],
        entries: u64,
        index: &mut WalIndexFile,
        index_state: &mut WalIndexHeader,
    ) -> Result<()> {
        self.write_frame_file(frame_id, bytes).await?;
        index_state.committed_frame = frame_id;
        index_state.committed_offset = bytes.len() as u32;
        index_state.entry_count = index_state.entry_count.saturating_add(entries);
        index.commit_volatile(*index_state).await?;
        self.staging_frame = self.staging_frame.saturating_add(1);
        Ok(())
    }

    pub async fn append_entry(
        &mut self,
        entry: &WALEntry,
        index: &mut WalIndexFile,
        index_state: &mut WalIndexHeader,
    ) -> Result<()> {
        let encoded = Self::encode_entry(entry)?;

        if encoded.len() > self.frame_size {
            if !self.staging.is_empty() {
                self.flush_staging(index, index_state).await?;
            }
            let frame_id = self.staging_frame;
            if frame_id > self.data_frame_count {
                return Err(LsmError::Internal("indexed wal segment full".into()));
            }
            self.commit_frame(frame_id, &encoded, 1, index, index_state)
                .await?;
            return Ok(());
        }

        if !self.staging.is_empty() && self.staging.len() + encoded.len() > self.frame_size {
            self.flush_staging(index, index_state).await?;
        }

        if self.staging.is_empty() && encoded.len() == self.frame_size {
            self.staging = encoded;
            self.entries_in_staging = 1;
            return self.flush_staging(index, index_state).await;
        }

        self.staging.extend_from_slice(&encoded);
        self.entries_in_staging += 1;

        if self.sync_mode == WalSyncMode::Fsync || self.sync_mode == WalSyncMode::FsyncAsync {
            self.flush_staging(index, index_state).await?;
        }
        Ok(())
    }

    pub async fn append_entries_batch(
        &mut self,
        entries: &[WALEntry],
        index: &mut WalIndexFile,
        index_state: &mut WalIndexHeader,
    ) -> Result<()> {
        for entry in entries {
            let encoded = Self::encode_entry(entry)?;
            if encoded.len() > self.frame_size {
                if !self.staging.is_empty() {
                    self.flush_staging(index, index_state).await?;
                }
                let frame_id = self.staging_frame;
                if frame_id > self.data_frame_count {
                    return Err(LsmError::Internal("indexed wal segment full".into()));
                }
                self.commit_frame(frame_id, &encoded, 1, index, index_state)
                    .await?;
                continue;
            }
            if !self.staging.is_empty() && self.staging.len() + encoded.len() > self.frame_size {
                self.flush_staging(index, index_state).await?;
            }
            self.staging.extend_from_slice(&encoded);
            self.entries_in_staging += 1;
        }
        if !self.staging.is_empty() {
            self.flush_staging(index, index_state).await?;
        }
        Ok(())
    }

    pub async fn read_committed_entries(
        &self,
        index_state: WalIndexHeader,
    ) -> Result<Vec<WALEntry>> {
        let mut out = Vec::new();
        let scanned = self.list_committed_frame_ids()?;
        let last_frame = index_state
            .committed_frame
            .max(*scanned.last().unwrap_or(&0));
        if last_frame == 0 {
            return Ok(out);
        }

        for frame_id in 1..=last_frame {
            let path = frame_path(&self.frames_dir, frame_id);
            if !path.exists() {
                continue;
            }
            let mut file = OpenOptions::new()
                .read(true)
                .open(&path)
                .await
                .map_err(LsmError::Io)?;
            let file_len = file.metadata().await.map_err(LsmError::Io)?.len() as usize;
            let read_len = file_len.max(self.frame_size);
            let mut frame = vec![0u8; read_len];
            file.read_exact(&mut frame).await.map_err(LsmError::Io)?;

            let limit = if frame_id == last_frame
                && index_state.committed_frame == last_frame
                && index_state.committed_offset > 0
            {
                index_state.committed_offset as usize
            } else {
                file_len
            };
            Self::decode_frames(&frame[..limit], &mut out)?;
        }
        Ok(out)
    }

    fn decode_frames(bytes: &[u8], out: &mut Vec<WALEntry>) -> Result<()> {
        let mut pos = 0usize;
        while pos + 4 <= bytes.len() {
            let size = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if size == 0 || pos + size > bytes.len() {
                break;
            }
            let entry: WALEntry = bincode::deserialize(&bytes[pos..pos + size])
                .map_err(|e| LsmError::Serialization(format!("decode wal entry: {e}")))?;
            out.push(entry);
            pos += size;
        }
        Ok(())
    }

    pub fn frames_dir(&self) -> &Path {
        &self.frames_dir
    }

    pub fn segment_id(&self) -> u32 {
        self.segment_id
    }
}

/// Indexed WAL manager (`WalEngine::Indexed`).
pub struct IndexedWalManager {
    config: WalConfig,
    wal_dir: PathBuf,
    index_path: PathBuf,
    frame_size: usize,
    data_frame_count: u32,
    current_segment: Arc<RwLock<Option<IndexedWalSegment>>>,
    index: Arc<RwLock<Option<WalIndexFile>>>,
    segments: Arc<RwLock<HashMap<u64, PathBuf>>>,
    segment_counter: Arc<std::sync::atomic::AtomicU64>,
}

impl IndexedWalManager {
    pub fn new(config: &WalConfig) -> Result<Self> {
        let frame_size = config.frame_page_size.max(INDEXED_MIN_FRAME_SIZE);
        let data_frame_count = config.indexed_frame_count.max(64);
        Ok(Self {
            config: config.clone(),
            wal_dir: PathBuf::from(&config.dir),
            index_path: PathBuf::from(&config.dir).join("wal.idx"),
            frame_size,
            data_frame_count,
            current_segment: Arc::new(RwLock::new(None)),
            index: Arc::new(RwLock::new(None)),
            segments: Arc::new(RwLock::new(HashMap::new())),
            segment_counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }

    pub async fn initialize(&self) -> Result<()> {
        if !self.wal_dir.exists() {
            tokio::fs::create_dir_all(&self.wal_dir)
                .await
                .map_err(LsmError::Io)?;
        }
        self.scan_existing_segments().await?;

        if self.index_path.exists() {
            let mut index = WalIndexFile::open(
                self.index_path.clone(),
                0,
                self.frame_size as u32,
            )
            .await?;
            if let Ok(state) = index.read_latest().await {
                let frames_dir = segment_frames_dir(&self.wal_dir, state.segment_id);
                if frames_dir.exists() {
                    let has_frames = std::fs::read_dir(&frames_dir)
                        .map(|mut d| d.next().is_some())
                        .unwrap_or(false);
                    if state.entry_count > 0 || has_frames {
                        let resume = state.committed_frame.saturating_add(1);
                        let segment = IndexedWalSegment::open_existing(
                            frames_dir,
                            state.segment_id,
                            self.frame_size,
                            self.data_frame_count,
                            self.config.sync_mode,
                            resume,
                        )
                        .await?;
                        *self.index.write().await = Some(index);
                        *self.current_segment.write().await = Some(segment);
                        return Ok(());
                    }
                }
            }
        }

        self.rotate_segment().await?;
        Ok(())
    }

    async fn scan_existing_segments(&self) -> Result<()> {
        let root = frames_root(&self.wal_dir);
        if !root.exists() {
            return Ok(());
        }
        let mut max_id = 0u64;
        if let Ok(entries) = std::fs::read_dir(&root) {
            for entry in entries.flatten() {
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }
                let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                if let Ok(id) = u64::from_str_radix(name, 16) {
                    max_id = max_id.max(id);
                }
            }
        }
        self.segment_counter
            .store(max_id + 1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    async fn rotate_segment(&self) -> Result<()> {
        let mut current = self.current_segment.write().await;
        if let Some(segment) = current.take() {
            let id = segment.segment_id() as u64;
            self.segments
                .write()
                .await
                .insert(id, segment.frames_dir().to_path_buf());
        }

        let segment_id = self
            .segment_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst) as u32;
        let frames_dir = segment_frames_dir(&self.wal_dir, segment_id);

        let segment = IndexedWalSegment::create(
            frames_dir,
            segment_id,
            self.frame_size,
            self.data_frame_count,
            self.config.sync_mode,
        )
        .await?;

        let mut index = WalIndexFile::open(
            self.index_path.clone(),
            segment_id,
            self.frame_size as u32,
        )
        .await?;
        let mut header = index.latest();
        header.segment_id = segment_id;
        header.frame_size = self.frame_size as u32;
        index.commit(header).await?;

        *self.index.write().await = Some(index);
        *current = Some(segment);
        Ok(())
    }

    pub async fn write_operation(&self, key: &str, value: &f4kvs_value::Value) -> Result<()> {
        let entry = WALEntry::Put {
            key: key.to_string(),
            value: value.clone(),
            timestamp: utils::timestamp_secs(),
        };
        self.write_entry(&entry).await
    }

    pub async fn write_delete(&self, key: &str) -> Result<()> {
        let entry = WALEntry::Delete {
            key: key.to_string(),
            timestamp: utils::timestamp_secs(),
        };
        self.write_entry(&entry).await
    }

    pub async fn write_entry(&self, entry: &WALEntry) -> Result<()> {
        let mut segment_guard = self.current_segment.write().await;
        let segment = segment_guard.as_mut().ok_or_else(|| {
            LsmError::Internal("indexed wal segment not initialized".into())
        })?;
        let mut index_guard = self.index.write().await;
        let index = index_guard.as_mut().ok_or_else(|| {
            LsmError::Internal("wal.idx not initialized".into())
        })?;
        let mut state = index.latest();
        segment.append_entry(entry, index, &mut state).await
    }

    pub async fn batch_write_operations(
        &self,
        items: &[(String, f4kvs_value::Value)],
    ) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let ts = utils::timestamp_secs();
        let entries: Vec<WALEntry> = items
            .iter()
            .map(|(k, v)| WALEntry::Put {
                key: k.clone(),
                value: v.clone(),
                timestamp: ts,
            })
            .collect();

        let mut segment_guard = self.current_segment.write().await;
        let segment = segment_guard.as_mut().ok_or_else(|| {
            LsmError::Internal("indexed wal segment not initialized".into())
        })?;
        let mut index_guard = self.index.write().await;
        let index = index_guard.as_mut().ok_or_else(|| {
            LsmError::Internal("wal.idx not initialized".into())
        })?;
        let mut state = index.latest();
        segment
            .append_entries_batch(&entries, index, &mut state)
            .await
    }

    pub async fn flush(&self) -> Result<()> {
        let mut segment_guard = self.current_segment.write().await;
        if let Some(segment) = segment_guard.as_mut() {
            let mut index_guard = self.index.write().await;
            if let Some(index) = index_guard.as_mut() {
                let mut state = index.latest();
                segment.flush_staging(index, &mut state).await?;
            }
        }
        Ok(())
    }

    pub async fn read_entries_from_disk(&self) -> Result<Vec<WALEntry>> {
        let mut all = Vec::new();
        if !self.wal_dir.exists() {
            return Ok(all);
        }

        let index_path = self.index_path.clone();
        if index_path.exists() {
            let mut index =
                WalIndexFile::open(index_path, 0, self.frame_size as u32).await?;
            if let Ok(state) = index.read_latest().await {
                let frames_dir = segment_frames_dir(&self.wal_dir, state.segment_id);
                if frames_dir.exists() {
                    let segment = IndexedWalSegment::open_existing(
                        frames_dir,
                        state.segment_id,
                        self.frame_size,
                        self.data_frame_count,
                        self.config.sync_mode,
                        state.committed_frame.saturating_add(1),
                    )
                    .await?;
                    all.extend(segment.read_committed_entries(state).await?);
                }
            }
        }

        let segments = self.segments.read().await;
        for (_, frames_dir) in segments.iter() {
            if !frames_dir.exists() {
                continue;
            }
            let segment_id = frames_dir
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|s| u32::from_str_radix(s, 16).ok())
                .unwrap_or(0);
            let segment = IndexedWalSegment::open_existing(
                frames_dir.clone(),
                segment_id,
                self.frame_size,
                self.data_frame_count,
                self.config.sync_mode,
                1,
            )
            .await?;
            let state = WalIndexHeader::new(segment_id, segment.frame_size as u32);
            if let Ok(entries) = segment.read_committed_entries(state).await {
                all.extend(entries);
            }
        }

        all.sort_by_key(entry_timestamp);
        Ok(all)
    }

    async fn remove_frames_dir(path: &Path) {
        if path.exists() {
            tokio::fs::remove_dir_all(path).await.ok();
        }
    }

    pub async fn truncate_after_flush(&self) -> Result<()> {
        let mut current = self.current_segment.write().await;
        if let Some(segment) = current.take() {
            Self::remove_frames_dir(segment.frames_dir()).await;
        }
        let mut segments = self.segments.write().await;
        for (_, frames_dir) in segments.drain() {
            Self::remove_frames_dir(&frames_dir).await;
        }
        drop(current);
        if self.index_path.exists() {
            tokio::fs::remove_file(&self.index_path).await.ok();
        }
        *self.index.write().await = None;
        self.rotate_segment().await
    }

    pub async fn verify_truncated(&self) -> Result<bool> {
        Ok(self.segments.read().await.is_empty())
    }

    pub async fn mark_clean_shutdown(&self) -> Result<()> {
        let checkpoint = WALEntry::Checkpoint {
            timestamp: utils::timestamp_secs(),
        };
        self.write_entry(&checkpoint).await?;
        self.flush().await?;
        self.truncate_after_flush().await
    }

    pub async fn cleanup_old_segments(&self, retention_period: Duration) -> Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| LsmError::Internal(e.to_string()))?
            .as_secs();
        let cutoff = now.saturating_sub(retention_period.as_secs());
        let mut segments = self.segments.write().await;
        let mut remove = Vec::new();
        for (id, path) in segments.iter() {
            if let Ok(meta) = tokio::fs::metadata(path).await {
                if let Ok(created) = meta.created() {
                    if let Ok(secs) = created.duration_since(UNIX_EPOCH) {
                        if secs.as_secs() < cutoff {
                            remove.push(*id);
                        }
                    }
                }
            }
        }
        for id in remove {
            if let Some(path) = segments.remove(&id) {
                Self::remove_frames_dir(&path).await;
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
            Self::remove_frames_dir(&path).await;
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::WalSyncMode;
    use f4kvs_value::Value;

    #[tokio::test]
    async fn indexed_wal_append_and_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = WalConfig::default();
        cfg.dir = dir.path().join("wal");
        cfg.sync_mode = WalSyncMode::Fsync;
        cfg.engine = crate::core::config::WalEngine::Indexed;
        cfg.indexed_frame_count = 128;

        let wal = IndexedWalManager::new(&cfg).unwrap();
        wal.initialize().await.unwrap();

        let payload = Value::Bytes(vec![b'x'; 4096]);
        wal.write_operation("chunk:legal:doc-0001:chunk-000001", &payload)
            .await
            .unwrap();
        wal.write_operation("chunk:legal:doc-0001:chunk-000002", &payload)
            .await
            .unwrap();

        let entries = wal.read_entries_from_disk().await.unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn indexed_wal_reopen_reads_committed() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let mut cfg = WalConfig::default();
        cfg.dir = wal_dir.clone();
        cfg.sync_mode = WalSyncMode::Fsync;
        cfg.indexed_frame_count = 64;

        {
            let wal = IndexedWalManager::new(&cfg).unwrap();
            wal.initialize().await.unwrap();
            let payload = Value::Bytes(vec![b'z'; 512]);
            wal.write_operation("reopen-key", &payload).await.unwrap();
        }

        let wal2 = IndexedWalManager::new(&cfg).unwrap();
        wal2.initialize().await.unwrap();
        let entries = wal2.read_entries_from_disk().await.unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0] {
            WALEntry::Put { key, .. } => assert_eq!(key, "reopen-key"),
            _ => panic!("expected put"),
        }
    }

    #[tokio::test]
    async fn indexed_wal_lsm_engine_integration() {
        use crate::core::config::WalEngine;
        use crate::{LsmConfig, LsmTreeEngine};
        use f4kvs_storage_core::traits::StorageEngine;

        let dir = tempfile::tempdir().unwrap();
        let mut cfg = LsmConfig::default();
        cfg.data_dir = dir.path().to_path_buf();
        cfg.wal.dir = dir.path().join("wal");
        cfg.wal.engine = WalEngine::Indexed;
        cfg.wal.indexed_frame_count = 128;

        let engine = LsmTreeEngine::new(cfg).await.unwrap();
        let payload = Value::Bytes(vec![b'a'; 2048]);
        engine.put("chunk:0001", &payload).await.unwrap();
        let got = engine.get("chunk:0001").await.unwrap();
        assert!(got.is_some());
    }

    #[tokio::test]
    async fn indexed_wal_batch_write() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = WalConfig::default();
        cfg.dir = dir.path().join("wal");
        cfg.sync_mode = WalSyncMode::Fsync;
        cfg.indexed_frame_count = 256;

        let wal = IndexedWalManager::new(&cfg).unwrap();
        wal.initialize().await.unwrap();

        let payload = Value::Bytes(vec![b'y'; 1024]);
        let items: Vec<(String, Value)> = (0..10)
            .map(|i| (format!("k{i}"), payload.clone()))
            .collect();
        wal.batch_write_operations(&items).await.unwrap();

        let entries = wal.read_entries_from_disk().await.unwrap();
        assert_eq!(entries.len(), 10);
    }
}