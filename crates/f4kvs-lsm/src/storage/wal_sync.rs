//! Platform sync helpers for WAL v2 (index + frame pages).

use crate::core::config::WalSyncMode;
use crate::error::{LsmError, Result};
use std::path::Path;
use tracing::warn;

/// Sync an entire file (used for small `wal.idx`).
pub async fn sync_path(path: &Path, mode: WalSyncMode) -> Result<()> {
    match mode {
        WalSyncMode::None | WalSyncMode::Flush => Ok(()),
        WalSyncMode::Fsync => {
            let path = path.to_path_buf();
            tokio::task::spawn_blocking(move || sync_path_blocking(&path))
                .await
                .map_err(|e| LsmError::Internal(format!("sync join: {e}")))?
        }
        WalSyncMode::FsyncAsync => {
            let path = path.to_path_buf();
            std::thread::spawn(move || {
                if let Err(e) = sync_path_blocking(&path) {
                    warn!("async wal sync failed for {:?}: {}", path, e);
                }
            });
            Ok(())
        }
    }
}

/// Sync byte ranges within a file (frame pages in pre-allocated WAL).
pub async fn sync_file_ranges(path: &Path, ranges: &[(u64, u64)], mode: WalSyncMode) -> Result<()> {
    if ranges.is_empty() {
        return Ok(());
    }
    match mode {
        WalSyncMode::None | WalSyncMode::Flush => Ok(()),
        WalSyncMode::Fsync => {
            let path = path.to_path_buf();
            let ranges: Vec<(u64, u64)> = ranges.to_vec();
            tokio::task::spawn_blocking(move || sync_ranges_blocking(&path, &ranges))
                .await
                .map_err(|e| LsmError::Internal(format!("sync join: {e}")))?
        }
        WalSyncMode::FsyncAsync => {
            let path = path.to_path_buf();
            let ranges: Vec<(u64, u64)> = ranges.to_vec();
            std::thread::spawn(move || {
                if let Err(e) = sync_ranges_blocking(&path, &ranges) {
                    warn!("async wal range sync failed for {:?}: {}", path, e);
                }
            });
            Ok(())
        }
    }
}

fn sync_path_blocking(path: &Path) -> Result<()> {
    use std::fs::OpenOptions;

    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(LsmError::Io)?;
    file.sync_data().map_err(LsmError::Io)?;
    Ok(())
}

fn sync_ranges_blocking(path: &Path, ranges: &[(u64, u64)]) -> Result<()> {
    use std::fs::OpenOptions;

    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(LsmError::Io)?;

    #[cfg(target_os = "linux")]
    {
        use std::io;
        use std::os::unix::io::AsRawFd;
        let fd = file.as_raw_fd();
        for &(offset, len) in ranges {
            let ret = unsafe {
                libc::sync_file_range(
                    fd,
                    offset as libc::off64_t,
                    len as libc::off64_t,
                    libc::SYNC_FILE_RANGE_WRITE
                        | libc::SYNC_FILE_RANGE_WAIT_BEFORE
                        | libc::SYNC_FILE_RANGE_WAIT_AFTER,
                )
            };
            if ret != 0 {
                let err = io::Error::last_os_error();
                return Err(LsmError::Io(err));
            }
        }
        return Ok(());
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = ranges;
        file.sync_data().map_err(LsmError::Io)?;
        Ok(())
    }
}