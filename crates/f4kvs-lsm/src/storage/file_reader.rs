//! Random-access file reader abstraction for SSTables.
//!
//! Provides a single `read_at` entry point with pluggable strategies:
//! - [`SstableReadMode::SeekRead`]: legacy seek + read under an exclusive lock
//! - [`SstableReadMode::PositionedRead`]: `pread` / `read_at` for concurrent reads
//! - [`SstableReadMode::MmapHybrid`]: `mmap` + `mincore` on Linux, positioned reads otherwise

use crate::core::config::SstableReadMode;
use crate::error::{LsmError, Result};
use crate::storage::mmap_reader::MmapHybridReader;
#[cfg(not(unix))]
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tracing::warn;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

/// Cached file handles for SSTable random reads.
pub struct SstableFileReader {
    path: PathBuf,
    mode: SstableReadMode,
    mincore_cache_ttl_secs: u64,
    /// Tokio async file used by [`SstableReadMode::SeekRead`].
    async_file: tokio::sync::RwLock<Option<File>>,
    /// Sync file shared across threads for positioned reads.
    sync_file: RwLock<Option<Arc<std::fs::File>>>,
    /// Hybrid mmap reader for [`SstableReadMode::MmapHybrid`].
    mmap_reader: RwLock<Option<Arc<MmapHybridReader>>>,
}

impl SstableFileReader {
    /// Create a reader for `path` using the given read mode.
    pub fn new(path: PathBuf, mode: SstableReadMode, mincore_cache_ttl_secs: u64) -> Self {
        Self {
            path,
            mode: effective_mode(mode),
            mincore_cache_ttl_secs,
            async_file: tokio::sync::RwLock::new(None),
            sync_file: RwLock::new(None),
            mmap_reader: RwLock::new(None),
        }
    }

    /// File path being read.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Effective read mode (after platform fallbacks).
    pub fn mode(&self) -> SstableReadMode {
        self.mode
    }

    /// Whether a file handle is currently cached.
    pub fn is_open(&self) -> bool {
        match self.mode {
            SstableReadMode::SeekRead => self
                .async_file
                .try_read()
                .map(|guard| guard.is_some())
                .unwrap_or(true),
            SstableReadMode::PositionedRead => self
                .sync_file
                .read()
                .map(|guard| guard.is_some())
                .unwrap_or(false),
            SstableReadMode::MmapHybrid => self
                .mmap_reader
                .read()
                .map(|guard| guard.as_ref().is_some_and(|reader| reader.is_open()))
                .unwrap_or(false),
        }
    }

    /// Open the file if it is not already open.
    pub async fn ensure_open(&self, resilient: bool) -> Result<()> {
        match self.mode {
            SstableReadMode::SeekRead => self.ensure_async_open(resilient).await,
            SstableReadMode::PositionedRead => self.ensure_sync_open(resilient).await,
            SstableReadMode::MmapHybrid => self.ensure_mmap_open(resilient).await,
        }
    }

    /// Close cached file handles.
    pub async fn close(&self) {
        {
            let mut guard = self.async_file.write().await;
            *guard = None;
        }
        if let Ok(mut guard) = self.sync_file.write() {
            *guard = None;
        }
        if let Ok(mut guard) = self.mmap_reader.write() {
            if let Some(reader) = guard.take() {
                reader.close();
            }
        }
    }

    /// Return the on-disk file size in bytes.
    pub async fn file_size(&self) -> Result<u64> {
        self.ensure_open(true).await?;
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || std::fs::metadata(&path).map(|meta| meta.len()))
            .await
            .map_err(|e| LsmError::Internal(format!("spawn_blocking failed: {e}")))?
            .map_err(LsmError::Io)
    }

    /// Read up to `buf.len()` bytes starting at `offset`.
    pub async fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        self.ensure_open(true).await?;

        match self.mode {
            SstableReadMode::SeekRead => {
                let mut guard = self.async_file.write().await;
                let file = guard
                    .as_mut()
                    .ok_or_else(|| LsmError::Internal("File not open".to_string()))?;
                file.seek(SeekFrom::Start(offset))
                    .await
                    .map_err(LsmError::Io)?;
                file.read(buf).await.map_err(LsmError::Io)
            }
            SstableReadMode::PositionedRead => {
                let file = self
                    .sync_file
                    .read()
                    .map_err(|e| LsmError::Internal(format!("sync file lock poisoned: {e}")))?
                    .clone()
                    .ok_or_else(|| LsmError::Internal("Sync file not open".to_string()))?;
                let len = buf.len();
                let data = tokio::task::spawn_blocking(move || read_at_sync(&file, offset, len))
                    .await
                    .map_err(|e| LsmError::Internal(format!("spawn_blocking failed: {e}")))?
                    .map_err(LsmError::Io)?;
                let n = data.len().min(buf.len());
                buf[..n].copy_from_slice(&data[..n]);
                Ok(n)
            }
            SstableReadMode::MmapHybrid => {
                let reader = self
                    .mmap_reader
                    .read()
                    .map_err(|e| LsmError::Internal(format!("mmap reader lock poisoned: {e}")))?
                    .clone()
                    .ok_or_else(|| LsmError::Internal("Mmap reader not open".to_string()))?;
                let len = buf.len();
                let data =
                    tokio::task::spawn_blocking(move || read_at_mmap(&reader, offset, len))
                        .await
                        .map_err(|e| LsmError::Internal(format!("spawn_blocking failed: {e}")))?
                        .map_err(LsmError::Io)?;
                let n = data.len().min(buf.len());
                buf[..n].copy_from_slice(&data[..n]);
                Ok(n)
            }
        }
    }

    /// Read exactly `buf.len()` bytes starting at `offset`.
    pub async fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let mut pos = 0usize;
        let base = offset;
        while pos < buf.len() {
            let n = self.read_at(base + pos as u64, &mut buf[pos..]).await?;
            if n == 0 {
                return Err(LsmError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "Unexpected EOF while reading {} bytes at offset {}",
                        buf.len(),
                        offset
                    ),
                )));
            }
            pos += n;
        }
        Ok(())
    }

    /// Read a little-endian `u32` at `offset`.
    pub async fn read_u32_le_at(&self, offset: u64) -> Result<u32> {
        let mut buf = [0u8; 4];
        self.read_exact_at(offset, &mut buf).await?;
        Ok(u32::from_le_bytes(buf))
    }

    async fn ensure_async_open(&self, resilient: bool) -> Result<()> {
        {
            let guard = self.async_file.read().await;
            if guard.is_some() {
                return Ok(());
            }
        }

        if !resilient {
            return Err(LsmError::Internal(
                "File not open and resilient handling is disabled".to_string(),
            ));
        }

        let file = OpenOptions::new()
            .read(true)
            .open(&self.path)
            .await
            .map_err(LsmError::Io)?;

        let mut guard = self.async_file.write().await;
        *guard = Some(file);
        Ok(())
    }

    async fn ensure_sync_open(&self, resilient: bool) -> Result<()> {
        if self
            .sync_file
            .read()
            .map(|guard| guard.is_some())
            .unwrap_or(false)
        {
            return Ok(());
        }

        if !resilient {
            return Err(LsmError::Internal(
                "Sync file not open and resilient handling is disabled".to_string(),
            ));
        }

        let path = self.path.clone();
        let file = tokio::task::spawn_blocking(move || std::fs::File::open(path))
            .await
            .map_err(|e| LsmError::Internal(format!("spawn_blocking failed: {e}")))?
            .map_err(LsmError::Io)?;

        let mut guard = self
            .sync_file
            .write()
            .map_err(|e| LsmError::Internal(format!("sync file lock poisoned: {e}")))?;
        *guard = Some(Arc::new(file));
        Ok(())
    }

    async fn ensure_mmap_open(&self, resilient: bool) -> Result<()> {
        if self
            .mmap_reader
            .read()
            .map(|guard| guard.is_some())
            .unwrap_or(false)
        {
            return Ok(());
        }

        if !resilient {
            return Err(LsmError::Internal(
                "Mmap reader not open and resilient handling is disabled".to_string(),
            ));
        }

        let path = self.path.clone();
        let ttl = self.mincore_cache_ttl_secs;
        let reader = tokio::task::spawn_blocking(move || MmapHybridReader::open(path, ttl))
            .await
            .map_err(|e| LsmError::Internal(format!("spawn_blocking failed: {e}")))?
            .map_err(LsmError::Io)?;

        let mut guard = self
            .mmap_reader
            .write()
            .map_err(|e| LsmError::Internal(format!("mmap reader lock poisoned: {e}")))?;
        *guard = Some(reader);
        Ok(())
    }
}

fn effective_mode(mode: SstableReadMode) -> SstableReadMode {
    match mode {
        SstableReadMode::MmapHybrid if !cfg!(target_os = "linux") => {
            warn!("SstableReadMode::MmapHybrid is only supported on Linux; using PositionedRead");
            SstableReadMode::PositionedRead
        }
        other => other,
    }
}

fn read_at_sync(file: &std::fs::File, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    read_exact_at_sync(file, offset, &mut buf)?;
    Ok(buf)
}

fn read_at_mmap(reader: &MmapHybridReader, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    let n = reader.read_at(offset, &mut buf)?;
    buf.truncate(n);
    Ok(buf)
}

fn read_exact_at_sync(file: &std::fs::File, mut offset: u64, buf: &mut [u8]) -> std::io::Result<()> {
    let mut pos = 0usize;
    while pos < buf.len() {
        let n = read_at_once(file, offset, &mut buf[pos..])?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "Unexpected EOF while reading {} bytes at offset {}",
                    buf.len(),
                    offset
                ),
            ));
        }
        pos += n;
        offset += n as u64;
    }
    Ok(())
}

fn read_at_once(file: &std::fs::File, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
    #[cfg(unix)]
    {
        file.read_at(buf, offset)
    }
    #[cfg(not(unix))]
    {
        let mut file = file.try_clone()?;
        file.seek(SeekFrom::Start(offset))?;
        file.read(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::SstableConfig;
    use tempfile::NamedTempFile;
    use tokio::io::AsyncWriteExt;

    async fn write_test_file(path: &Path) {
        let mut file = File::create(path).await.unwrap();
        file.write_all(b"abcdefghij").await.unwrap();
        file.flush().await.unwrap();
    }

    #[tokio::test]
    async fn positioned_read_returns_expected_slice() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();
        write_test_file(&path).await;

        let reader = SstableFileReader::new(path, SstableReadMode::PositionedRead, 60);
        let mut buf = [0u8; 4];
        reader.read_exact_at(3, &mut buf).await.unwrap();
        assert_eq!(&buf, b"defg");
    }

    #[tokio::test]
    async fn seek_read_returns_expected_slice() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();
        write_test_file(&path).await;

        let reader = SstableFileReader::new(path, SstableReadMode::SeekRead, 60);
        let mut buf = [0u8; 4];
        reader.read_exact_at(3, &mut buf).await.unwrap();
        assert_eq!(&buf, b"defg");
    }

    #[tokio::test]
    async fn mmap_hybrid_reads_expected_slice_on_linux() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();
        write_test_file(&path).await;

        let reader = SstableFileReader::new(path, SstableReadMode::MmapHybrid, 60);
        if cfg!(target_os = "linux") {
            assert_eq!(reader.mode(), SstableReadMode::MmapHybrid);
        } else {
            assert_eq!(reader.mode(), SstableReadMode::PositionedRead);
        }

        let mut buf = [0u8; 4];
        reader.read_exact_at(3, &mut buf).await.unwrap();
        assert_eq!(&buf, b"defg");
    }

    #[test]
    fn sstable_config_defaults_to_positioned_read() {
        let config = SstableConfig::default();
        assert_eq!(config.read_mode, SstableReadMode::PositionedRead);
        assert_eq!(config.mincore_cache_ttl_secs, 60);
    }
}