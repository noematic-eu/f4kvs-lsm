//! WAL v2 durable index (`wal.idx`) — double-buffered 4 KiB slots.

use crate::error::{LsmError, Result};
use std::path::{Path, PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub const WAL_INDEX_MAGIC: [u8; 4] = *b"WIDX";
pub const WAL_INDEX_VERSION: u8 = 1;
pub const WAL_INDEX_SLOT_SIZE: usize = 4096;
pub const WAL_INDEX_FILE_SIZE: usize = WAL_INDEX_SLOT_SIZE * 2;

/// Durable high-water mark for the indexed WAL segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalIndexHeader {
    pub generation: u64,
    pub segment_id: u32,
    pub frame_size: u32,
    /// Highest frame id containing durable bytes (frame 0 = file header).
    pub committed_frame: u32,
    /// Bytes used within `committed_frame` (partial tail frame).
    pub committed_offset: u32,
    pub entry_count: u64,
    pub checksum: u32,
}

impl WalIndexHeader {
    pub fn new(segment_id: u32, frame_size: u32) -> Self {
        let mut header = Self {
            generation: 0,
            segment_id,
            frame_size,
            committed_frame: 0,
            committed_offset: 0,
            entry_count: 0,
            checksum: 0,
        };
        header.checksum = header.compute_checksum();
        header
    }

    pub fn compute_checksum(&self) -> u32 {
        let mut h = crc32fast::Hasher::new();
        h.update(&WAL_INDEX_MAGIC);
        h.update(&[self.version_byte()]);
        h.update(&self.generation.to_le_bytes());
        h.update(&self.segment_id.to_le_bytes());
        h.update(&self.frame_size.to_le_bytes());
        h.update(&self.committed_frame.to_le_bytes());
        h.update(&self.committed_offset.to_le_bytes());
        h.update(&self.entry_count.to_le_bytes());
        h.finalize()
    }

    fn version_byte(&self) -> u8 {
        WAL_INDEX_VERSION
    }

    pub fn verify(&self) -> bool {
        self.checksum == self.compute_checksum()
    }

    pub fn to_slot_bytes(&self) -> [u8; WAL_INDEX_SLOT_SIZE] {
        let mut slot = [0u8; WAL_INDEX_SLOT_SIZE];
        slot[0..4].copy_from_slice(&WAL_INDEX_MAGIC);
        slot[4] = self.version_byte();
        slot[8..16].copy_from_slice(&self.generation.to_le_bytes());
        slot[16..20].copy_from_slice(&self.segment_id.to_le_bytes());
        slot[20..24].copy_from_slice(&self.frame_size.to_le_bytes());
        slot[24..28].copy_from_slice(&self.committed_frame.to_le_bytes());
        slot[28..32].copy_from_slice(&self.committed_offset.to_le_bytes());
        slot[32..40].copy_from_slice(&self.entry_count.to_le_bytes());
        slot[40..44].copy_from_slice(&self.checksum.to_le_bytes());
        slot
    }

    pub fn from_slot_bytes(slot: &[u8]) -> Result<Self> {
        if slot.len() < 44 {
            return Err(LsmError::Corruption("wal.idx slot too small".into()));
        }
        let magic: [u8; 4] = slot[0..4].try_into().map_err(|_| LsmError::Corruption("wal.idx magic".into()))?;
        if magic != WAL_INDEX_MAGIC {
            return Err(LsmError::Corruption("wal.idx bad magic".into()));
        }
        if slot[4] != WAL_INDEX_VERSION {
            return Err(LsmError::Corruption(format!(
                "wal.idx unsupported version {}",
                slot[4]
            )));
        }
        let header = Self {
            generation: u64::from_le_bytes(slot[8..16].try_into().unwrap()),
            segment_id: u32::from_le_bytes(slot[16..20].try_into().unwrap()),
            frame_size: u32::from_le_bytes(slot[20..24].try_into().unwrap()),
            committed_frame: u32::from_le_bytes(slot[24..28].try_into().unwrap()),
            committed_offset: u32::from_le_bytes(slot[28..32].try_into().unwrap()),
            entry_count: u64::from_le_bytes(slot[32..40].try_into().unwrap()),
            checksum: u32::from_le_bytes(slot[40..44].try_into().unwrap()),
        };
        if !header.verify() {
            return Err(LsmError::Corruption("wal.idx checksum mismatch".into()));
        }
        Ok(header)
    }
}

/// Double-buffered on-disk index (8 KiB).
pub struct WalIndexFile {
    path: PathBuf,
    file: File,
    latest: WalIndexHeader,
}

impl WalIndexFile {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn latest(&self) -> WalIndexHeader {
        self.latest
    }

    pub async fn open(path: PathBuf, segment_id: u32, frame_size: u32) -> Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(LsmError::Io)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .await
            .map_err(LsmError::Io)?;

        let metadata = file.metadata().await.map_err(LsmError::Io)?;
        if metadata.len() < WAL_INDEX_FILE_SIZE as u64 {
            file.set_len(WAL_INDEX_FILE_SIZE as u64)
                .await
                .map_err(LsmError::Io)?;
        }

        let mut index = Self {
            path,
            file,
            latest: WalIndexHeader::new(segment_id, frame_size),
        };

        if let Ok(header) = index.read_latest().await {
            index.latest = header;
        }

        Ok(index)
    }

    pub async fn read_latest(&mut self) -> Result<WalIndexHeader> {
        let mut slot_a = [0u8; WAL_INDEX_SLOT_SIZE];
        let mut slot_b = [0u8; WAL_INDEX_SLOT_SIZE];

        self.file
            .seek(tokio::io::SeekFrom::Start(0))
            .await
            .map_err(LsmError::Io)?;
        self.file
            .read_exact(&mut slot_a)
            .await
            .map_err(LsmError::Io)?;
        self.file
            .read_exact(&mut slot_b)
            .await
            .map_err(LsmError::Io)?;

        let mut candidates = Vec::new();
        if slot_a[0..4] == WAL_INDEX_MAGIC {
            if let Ok(h) = WalIndexHeader::from_slot_bytes(&slot_a) {
                candidates.push(h);
            }
        }
        if slot_b[0..4] == WAL_INDEX_MAGIC {
            if let Ok(h) = WalIndexHeader::from_slot_bytes(&slot_b) {
                candidates.push(h);
            }
        }

        candidates
            .into_iter()
            .max_by_key(|h| h.generation)
            .ok_or_else(|| LsmError::Corruption("wal.idx has no valid slot".into()))
    }

    pub async fn commit(&mut self, mut header: WalIndexHeader) -> Result<()> {
        self.write_slot(&mut header, true).await
    }

    /// Update the index slot without fsync — safe when frame files are synced independently.
    pub async fn commit_volatile(&mut self, mut header: WalIndexHeader) -> Result<()> {
        self.write_slot(&mut header, false).await
    }

    async fn write_slot(&mut self, header: &mut WalIndexHeader, durable: bool) -> Result<()> {
        header.generation = self.latest.generation.saturating_add(1);
        header.checksum = header.compute_checksum();

        let slot_index = (header.generation % 2) as u64;
        let offset = slot_index * WAL_INDEX_SLOT_SIZE as u64;
        let bytes = header.to_slot_bytes();

        self.file
            .seek(tokio::io::SeekFrom::Start(offset))
            .await
            .map_err(LsmError::Io)?;
        self.file
            .write_all(&bytes)
            .await
            .map_err(LsmError::Io)?;
        self.file.flush().await.map_err(LsmError::Io)?;
        if durable {
            crate::storage::wal_sync::sync_path(
                &self.path,
                crate::core::config::WalSyncMode::Fsync,
            )
            .await?;
        }

        self.latest = *header;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wal_index_roundtrip_and_checksum() {
        let mut h = WalIndexHeader::new(7, 8192);
        h.committed_frame = 42;
        h.committed_offset = 128;
        h.entry_count = 99;
        h.checksum = h.compute_checksum();
        let bytes = h.to_slot_bytes();
        let parsed = WalIndexHeader::from_slot_bytes(&bytes).unwrap();
        assert_eq!(parsed, h);
    }

    #[tokio::test]
    async fn wal_index_double_buffer_commits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.idx");
        let mut index = WalIndexFile::open(path.clone(), 1, 8192).await.unwrap();

        let mut h = index.latest();
        h.committed_frame = 1;
        h.committed_offset = 100;
        h.entry_count = 1;
        index.commit(h).await.unwrap();

        let mut index2 = WalIndexFile::open(path, 1, 8192).await.unwrap();
        let loaded = index2.read_latest().await.unwrap();
        assert_eq!(loaded.generation, 1);
        assert_eq!(loaded.committed_frame, 1);
        assert_eq!(loaded.entry_count, 1);
    }
}