//! Linux `mmap` + `mincore` hybrid reader.

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// Hybrid reader: mmap fast-path with `mincore` gating, `pread` fallback.
pub struct MmapHybridReader {
    inner: RwLock<MmapHybridState>,
}

struct MmapHybridState {
    page_size: u64,
    file_size: u64,
    file: Arc<File>,
    mapping: Option<MmapMapping>,
    mincore_bits: Arc<Vec<AtomicU64>>,
    mincore_next_cleanup: AtomicU64,
    mincore_cache_ttl_secs: u64,
}

struct MmapMapping {
    ptr: *const u8,
    len: usize,
}

impl MmapHybridReader {
    /// Open `path` for hybrid reads. The file is mmap'd lazily on the first read.
    pub fn open(path: PathBuf, mincore_cache_ttl_secs: u64) -> io::Result<Arc<Self>> {
        let file = Arc::new(File::open(&path)?);
        let file_size = file.metadata()?.len();
        let page_size = page_size_bytes();
        let word_count = page_word_count(file_size, page_size);

        Ok(Arc::new(Self {
            inner: RwLock::new(MmapHybridState {
                page_size,
                file_size,
                file,
                mapping: None,
                mincore_bits: Arc::new((0..word_count).map(|_| AtomicU64::new(0)).collect()),
                mincore_next_cleanup: AtomicU64::new(0),
                mincore_cache_ttl_secs,
            }),
        }))
    }

    /// Whether the backing file handle is open.
    pub fn is_open(&self) -> bool {
        self.inner
            .read()
            .map(|state| state.file_size > 0)
            .unwrap_or(false)
    }

    /// Close mmap mapping and release cached state.
    pub fn close(&self) {
        if let Ok(mut state) = self.inner.write() {
            state.mapping = None;
        }
    }

    /// Read up to `buf.len()` bytes at `offset`.
    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let state = self.inner.read().map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("mmap reader lock poisoned: {e}"),
            )
        })?;

        if offset >= state.file_size {
            return Ok(0);
        }

        let available = (state.file_size - offset) as usize;
        let to_read = buf.len().min(available);

        if Self::try_mmap_read(&state, offset, &mut buf[..to_read])? {
            return Ok(to_read);
        }

        drop(state);
        self.read_via_syscall(offset, &mut buf[..to_read])
    }

    fn try_mmap_read(state: &MmapHybridState, offset: u64, buf: &mut [u8]) -> io::Result<bool> {
        let Some(mapping) = state.mapping.as_ref() else {
            return Ok(false);
        };

        if !can_fast_read_via_mmap(
            mapping,
            offset,
            buf.len(),
            state.page_size,
            &state.mincore_bits,
            state.mincore_cache_ttl_secs,
            &state.mincore_next_cleanup,
        ) {
            return Ok(false);
        }

        let end = offset as usize + buf.len();
        if end > mapping.len() {
            return Ok(false);
        }

        buf.copy_from_slice(&mapping.slice()[offset as usize..end]);
        Ok(true)
    }

    fn read_via_syscall(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let mut state = self.inner.write().map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("mmap reader lock poisoned: {e}"),
            )
        })?;

        state.ensure_mapped()?;

        if Self::try_mmap_read(&state, offset, buf)? {
            return Ok(buf.len());
        }

        let file = Arc::clone(&state.file);
        let page_size = state.page_size;
        let mincore_bits = Arc::clone(&state.mincore_bits);
        let len = buf.len();
        drop(state);

        read_exact_at_sync(&file, offset, buf)?;
        mark_pages_resident(offset, len as u64, page_size, &mincore_bits);
        Ok(buf.len())
    }
}

impl MmapHybridState {
    fn ensure_mapped(&mut self) -> io::Result<()> {
        if self.mapping.is_some() {
            return Ok(());
        }

        if self.file_size == 0 {
            self.mapping = Some(MmapMapping::empty());
            return Ok(());
        }

        self.mapping = Some(MmapMapping::map(&self.file, self.file_size as usize)?);
        Ok(())
    }
}

impl MmapMapping {
    fn empty() -> Self {
        Self {
            ptr: std::ptr::null(),
            len: 0,
        }
    }

    fn map(file: &File, len: usize) -> io::Result<Self> {
        let fd = file.as_raw_fd();
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }

        Ok(Self {
            ptr: ptr as *const u8,
            len,
        })
    }

    fn len(&self) -> usize {
        self.len
    }

    fn slice(&self) -> &[u8] {
        if self.len == 0 {
            return &[];
        }
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for MmapMapping {
    fn drop(&mut self) {
        if self.len > 0 && !self.ptr.is_null() {
            unsafe {
                libc::munmap(self.ptr as *mut _, self.len);
            }
        }
    }
}

unsafe impl Send for MmapMapping {}
unsafe impl Sync for MmapMapping {}

fn page_size_bytes() -> u64 {
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size > 0 {
        page_size as u64
    } else {
        4096
    }
}

fn page_word_count(file_size: u64, page_size: u64) -> usize {
    let page_count = file_size.div_ceil(page_size);
    page_count.div_ceil(64) as usize
}

fn unix_timestamp_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn can_fast_read_via_mmap(
    mapping: &MmapMapping,
    offset: u64,
    len: usize,
    page_size: u64,
    mincore_bits: &[AtomicU64],
    mincore_cache_ttl_secs: u64,
    mincore_next_cleanup: &AtomicU64,
) -> bool {
    maybe_cleanup_mincore_cache(mincore_bits, mincore_cache_ttl_secs, mincore_next_cleanup);

    let mut off = offset;
    let end = offset + len as u64;
    off -= off % page_size;

    let data = mapping.slice();
    let mut page_idx = off / page_size;

    while off < end {
        let word_idx = (page_idx / 64) as usize;
        let bit_idx = page_idx % 64;
        let mask = 1u64 << bit_idx;

        if word_idx >= mincore_bits.len() {
            return false;
        }

        let word_ptr = &mincore_bits[word_idx];
        let mut word = word_ptr.load(Ordering::Acquire);

        if word & mask == 0 {
            let page_offset = off as usize;
            if page_offset >= data.len() {
                return false;
            }

            if !page_is_resident(&data[page_offset..]) {
                return false;
            }

            loop {
                match word_ptr.compare_exchange_weak(
                    word,
                    word | mask,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(current) => word = current,
                }
                if word & mask != 0 {
                    break;
                }
            }
        }

        off += page_size;
        page_idx += 1;
    }

    true
}

fn maybe_cleanup_mincore_cache(
    mincore_bits: &[AtomicU64],
    ttl_secs: u64,
    next_cleanup: &AtomicU64,
) {
    let now = unix_timestamp_secs();
    let previous = next_cleanup.load(Ordering::Acquire);
    if now <= previous {
        return;
    }

    if next_cleanup
        .compare_exchange(
            previous,
            now + ttl_secs,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .is_err()
    {
        return;
    }

    for word in mincore_bits {
        word.store(0, Ordering::Release);
    }
}

fn mark_pages_resident(offset: u64, len: u64, page_size: u64, mincore_bits: &[AtomicU64]) {
    let mut off = offset;
    let end = offset + len;
    off -= off % page_size;

    let mut page_idx = off / page_size;
    while off < end {
        let word_idx = (page_idx / 64) as usize;
        let bit_idx = page_idx % 64;
        let mask = 1u64 << bit_idx;

        if word_idx < mincore_bits.len() {
            let word_ptr = &mincore_bits[word_idx];
            let mut word = word_ptr.load(Ordering::Acquire);
            while word & mask == 0 {
                match word_ptr.compare_exchange_weak(
                    word,
                    word | mask,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(current) => word = current,
                }
            }
        }

        off += page_size;
        page_idx += 1;
    }
}

fn page_is_resident(page: &[u8]) -> bool {
    if page.is_empty() {
        return false;
    }

    let mut vec = [0u8; 1];
    let rc = unsafe { libc::mincore(page.as_ptr() as *mut _, 1, vec.as_mut_ptr()) };
    rc == 0 && (vec[0] & 1) != 0
}

fn read_exact_at_sync(file: &File, mut offset: u64, buf: &mut [u8]) -> io::Result<()> {
    let mut pos = 0usize;
    while pos < buf.len() {
        let n = file.read_at(&mut buf[pos..], offset)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::Path;
    use tempfile::NamedTempFile;

    fn write_test_file(path: &Path, data: &[u8]) {
        let mut file = std::fs::File::create(path).unwrap();
        file.write_all(data).unwrap();
        file.sync_all().unwrap();
    }

    #[test]
    fn mmap_hybrid_reads_file_contents() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();
        write_test_file(&path, b"abcdefghij");

        let reader = MmapHybridReader::open(path, 60).unwrap();
        let mut buf = [0u8; 4];
        let n = reader.read_at(3, &mut buf).unwrap();
        assert_eq!(n, 4);
        assert_eq!(&buf, b"defg");
    }

    #[test]
    fn repeated_reads_use_warm_path() {
        let page_size = page_size_bytes() as usize;
        let data = vec![b'x'; page_size + 32];
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();
        write_test_file(&path, &data);

        let reader = MmapHybridReader::open(path, 60).unwrap();
        let mut first = [0u8; 16];
        let mut second = [0u8; 16];
        reader.read_at(0, &mut first).unwrap();
        reader.read_at(0, &mut second).unwrap();
        assert_eq!(first, second);
    }
}
