//! `mmap` + `mincore` hybrid reader for hot page-cache resident SSTable pages.

#[cfg(target_os = "linux")]
mod linux;

#[cfg(not(target_os = "linux"))]
mod stub;

#[cfg(target_os = "linux")]
pub use linux::MmapHybridReader;

#[cfg(not(target_os = "linux"))]
pub use stub::MmapHybridReader;
