//! Non-Linux stub for the mmap hybrid reader.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

/// Stub type used when `MmapHybrid` is not available on the target OS.
pub struct MmapHybridReader;

impl MmapHybridReader {
    pub fn open(_path: PathBuf, _mincore_cache_ttl_secs: u64) -> io::Result<Arc<Self>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "MmapHybridReader is only supported on Linux",
        ))
    }

    pub fn is_open(&self) -> bool {
        false
    }

    pub fn close(&self) {}

    pub fn read_at(&self, _offset: u64, _buf: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "MmapHybridReader is only supported on Linux",
        ))
    }
}
