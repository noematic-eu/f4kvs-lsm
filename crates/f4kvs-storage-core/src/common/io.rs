//! Centralized I/O utilities for F4KVS storage

use crate::{F4KvsError, Result};
use std::fs;
use std::path::{Path, PathBuf};
use tokio::fs as async_fs;

/// Ensure directory exists, creating it if necessary (synchronous version)
///
/// This is the canonical implementation for synchronous directory creation.
/// Previously duplicated across multiple modules.
pub fn ensure_directory_exists<P: AsRef<Path>>(path: P) -> Result<()> {
    let path = path.as_ref();
    if !path.exists() {
        fs::create_dir_all(path).map_err(|e| {
            F4KvsError::io(format!(
                "Failed to create directory {}: {}",
                path.display(),
                e
            ))
        })?;
    }
    Ok(())
}

/// Ensure directory exists, creating it if necessary (asynchronous version)
///
/// This is the canonical implementation for asynchronous directory creation.
/// Previously duplicated across multiple modules.
pub async fn ensure_directory_exists_async<P: AsRef<Path>>(path: P) -> Result<()> {
    let path = path.as_ref();
    if !path.exists() {
        async_fs::create_dir_all(path).await.map_err(|e| {
            F4KvsError::io(format!(
                "Failed to create directory {}: {}",
                path.display(),
                e
            ))
        })?;
    }
    Ok(())
}

/// Safely write data to a file with atomic replacement
///
/// Writes data to a temporary file first, then atomically moves it to the target.
/// This prevents partial writes and ensures data integrity.
pub fn atomic_write<P: AsRef<Path>>(path: P, data: &[u8]) -> Result<()> {
    let path = path.as_ref();
    let temp_path = path.with_extension("tmp");

    // Write to temporary file
    fs::write(&temp_path, data).map_err(|e| {
        F4KvsError::io(format!(
            "Failed to write to temporary file {}: {}",
            temp_path.display(),
            e
        ))
    })?;

    // Atomically move to target
    fs::rename(&temp_path, path).map_err(|e| {
        // Clean up temp file on error
        let _ = fs::remove_file(&temp_path);
        F4KvsError::io(format!(
            "Failed to move temporary file to {}: {}",
            path.display(),
            e
        ))
    })?;

    Ok(())
}

/// Safely write data to a file with atomic replacement (asynchronous version)
pub async fn atomic_write_async<P: AsRef<Path>>(path: P, data: &[u8]) -> Result<()> {
    let path = path.as_ref();
    let temp_path = path.with_extension("tmp");

    // Write to temporary file
    async_fs::write(&temp_path, data).await.map_err(|e| {
        F4KvsError::io(format!(
            "Failed to write to temporary file {}: {}",
            temp_path.display(),
            e
        ))
    })?;

    // Atomically move to target
    async_fs::rename(&temp_path, path).await.map_err(|e| {
        // Clean up temp file on error (spawn async task to avoid blocking)
        let temp_path = temp_path.clone();
        tokio::spawn(async move {
            let _ = async_fs::remove_file(&temp_path).await;
        });
        F4KvsError::io(format!(
            "Failed to move temporary file to {}: {}",
            path.display(),
            e
        ))
    })?;

    Ok(())
}

/// Read file contents safely
pub fn read_file<P: AsRef<Path>>(path: P) -> Result<Vec<u8>> {
    let path = path.as_ref();
    fs::read(path)
        .map_err(|e| F4KvsError::io(format!("Failed to read file {}: {}", path.display(), e)))
}

/// Read file contents safely (asynchronous version)
pub async fn read_file_async<P: AsRef<Path>>(path: P) -> Result<Vec<u8>> {
    let path = path.as_ref();
    async_fs::read(path)
        .await
        .map_err(|e| F4KvsError::io(format!("Failed to read file {}: {}", path.display(), e)))
}

/// Check if a path exists and is a file
pub fn is_file<P: AsRef<Path>>(path: P) -> bool {
    path.as_ref().is_file()
}

/// Check if a path exists and is a directory
pub fn is_directory<P: AsRef<Path>>(path: P) -> bool {
    path.as_ref().is_dir()
}

/// Get file size in bytes
pub fn file_size<P: AsRef<Path>>(path: P) -> Result<u64> {
    let path = path.as_ref();
    let metadata = fs::metadata(path).map_err(|e| {
        F4KvsError::io(format!(
            "Failed to get metadata for {}: {}",
            path.display(),
            e
        ))
    })?;
    Ok(metadata.len())
}

/// Get file size in bytes (asynchronous version)
pub async fn file_size_async<P: AsRef<Path>>(path: P) -> Result<u64> {
    let path = path.as_ref();
    let metadata = async_fs::metadata(path).await.map_err(|e| {
        F4KvsError::io(format!(
            "Failed to get metadata for {}: {}",
            path.display(),
            e
        ))
    })?;
    Ok(metadata.len())
}

/// Remove file safely
pub fn remove_file<P: AsRef<Path>>(path: P) -> Result<()> {
    let path = path.as_ref();
    fs::remove_file(path)
        .map_err(|e| F4KvsError::io(format!("Failed to remove file {}: {}", path.display(), e)))
}

/// Remove file safely (asynchronous version)
pub async fn remove_file_async<P: AsRef<Path>>(path: P) -> Result<()> {
    let path = path.as_ref();
    async_fs::remove_file(path)
        .await
        .map_err(|e| F4KvsError::io(format!("Failed to remove file {}: {}", path.display(), e)))
}

/// List files in a directory
pub fn list_files<P: AsRef<Path>>(dir: P) -> Result<Vec<PathBuf>> {
    let dir = dir.as_ref();
    let entries = fs::read_dir(dir).map_err(|e| {
        F4KvsError::io(format!("Failed to read directory {}: {}", dir.display(), e))
    })?;

    let mut files = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| {
            F4KvsError::io(format!(
                "Failed to read directory entry in {}: {}",
                dir.display(),
                e
            ))
        })?;

        if entry
            .file_type()
            .map_err(|e| {
                F4KvsError::io(format!(
                    "Failed to get file type for {}: {}",
                    entry.path().display(),
                    e
                ))
            })?
            .is_file()
        {
            files.push(entry.path());
        }
    }

    Ok(files)
}

/// List files in a directory (asynchronous version)
pub async fn list_files_async<P: AsRef<Path>>(dir: P) -> Result<Vec<PathBuf>> {
    let dir = dir.as_ref();
    let mut entries = async_fs::read_dir(dir).await.map_err(|e| {
        F4KvsError::io(format!("Failed to read directory {}: {}", dir.display(), e))
    })?;

    let mut files = Vec::new();
    while let Some(entry) = entries.next_entry().await.map_err(|e| {
        F4KvsError::io(format!(
            "Failed to read directory entry in {}: {}",
            dir.display(),
            e
        ))
    })? {
        if entry
            .file_type()
            .await
            .map_err(|e| {
                F4KvsError::io(format!(
                    "Failed to get file type for {}: {}",
                    entry.path().display(),
                    e
                ))
            })?
            .is_file()
        {
            files.push(entry.path());
        }
    }

    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_ensure_directory_exists() {
        let temp_dir = TempDir::new().expect("test operation failed");
        let test_dir = temp_dir.path().join("test_dir");

        // Directory should not exist initially
        assert!(!test_dir.exists());

        // Create directory
        ensure_directory_exists(&test_dir).expect("test operation failed");
        assert!(test_dir.exists());
        assert!(test_dir.is_dir());

        // Creating again should not fail
        ensure_directory_exists(&test_dir).expect("test operation failed");
    }

    #[test]
    fn test_atomic_write() {
        let temp_dir = TempDir::new().expect("test operation failed");
        let test_file = temp_dir.path().join("test.txt");
        let test_data = b"Hello, World!";

        // Write data atomically
        atomic_write(&test_file, test_data).expect("test operation failed");

        // Verify data was written correctly
        let read_data = fs::read(&test_file).expect("test operation failed");
        assert_eq!(read_data, test_data);
    }

    #[test]
    fn test_file_operations() {
        let temp_dir = TempDir::new().expect("test operation failed");
        let test_file = temp_dir.path().join("test.txt");
        let test_data = b"Test data";

        // Write test file
        fs::write(&test_file, test_data).expect("test operation failed");

        // Test file operations
        assert!(is_file(&test_file));
        assert!(!is_directory(&test_file));
        assert_eq!(
            file_size(&test_file).expect("test operation failed"),
            test_data.len() as u64
        );

        // Test reading
        let read_data = read_file(&test_file).expect("test operation failed");
        assert_eq!(read_data, test_data);

        // Test removal
        remove_file(&test_file).expect("test operation failed");
        assert!(!test_file.exists());
    }

    #[test]
    fn test_list_files() {
        let temp_dir = TempDir::new().expect("test operation failed");
        let test_dir = temp_dir.path().join("test_dir");
        ensure_directory_exists(&test_dir).expect("test operation failed");

        // Create some test files
        fs::write(test_dir.join("file1.txt"), "content1").expect("test operation failed");
        fs::write(test_dir.join("file2.txt"), "content2").expect("test operation failed");
        fs::create_dir(test_dir.join("subdir")).expect("test operation failed");

        // List files
        let files = list_files(&test_dir).expect("test operation failed");
        assert_eq!(files.len(), 2);
        assert!(files
            .iter()
            .any(|f| f.file_name().expect("test operation failed") == "file1.txt"));
        assert!(files
            .iter()
            .any(|f| f.file_name().expect("test operation failed") == "file2.txt"));
    }
}
