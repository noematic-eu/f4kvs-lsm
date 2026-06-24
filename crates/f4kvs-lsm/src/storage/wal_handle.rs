//! Pluggable WAL backend — segment (default) or frame.

use crate::core::config::{WalConfig, WalEngine};
use crate::error::Result;
use crate::storage::wal::{WALEntry, WALManager};
use crate::storage::wal_frame::FrameWalManager;
use std::time::Duration;

/// Unified WAL handle delegating to the configured backend.
pub enum WalHandle {
    Segment(WALManager),
    Frame(FrameWalManager),
}

impl WalHandle {
    pub fn new(config: &WalConfig) -> Result<Self> {
        match config.engine {
            WalEngine::Segment => Ok(Self::Segment(WALManager::new(config)?)),
            WalEngine::Frame => Ok(Self::Frame(FrameWalManager::new(config)?)),
        }
    }

    pub async fn initialize(&self) -> Result<()> {
        match self {
            Self::Segment(wal) => wal.initialize().await,
            Self::Frame(wal) => wal.initialize().await,
        }
    }

    pub async fn write_operation(&self, key: &str, value: &f4kvs_value::Value) -> Result<()> {
        match self {
            Self::Segment(wal) => wal.write_operation(key, value).await,
            Self::Frame(wal) => wal.write_operation(key, value).await,
        }
    }

    pub async fn write_delete(&self, key: &str) -> Result<()> {
        match self {
            Self::Segment(wal) => wal.write_delete(key).await,
            Self::Frame(wal) => wal.write_delete(key).await,
        }
    }

    pub async fn batch_write_operations(
        &self,
        items: &[(String, f4kvs_value::Value)],
    ) -> Result<()> {
        match self {
            Self::Segment(wal) => wal.batch_write_operations(items).await,
            Self::Frame(wal) => wal.batch_write_operations(items).await,
        }
    }

    pub async fn flush(&self) -> Result<()> {
        match self {
            Self::Segment(wal) => wal.flush().await,
            Self::Frame(wal) => wal.flush().await,
        }
    }

    pub async fn truncate_after_flush(&self) -> Result<()> {
        match self {
            Self::Segment(wal) => wal.truncate_after_flush().await,
            Self::Frame(wal) => wal.truncate_after_flush().await,
        }
    }

    pub async fn verify_truncated(&self) -> Result<bool> {
        match self {
            Self::Segment(wal) => wal.verify_truncated().await,
            Self::Frame(wal) => wal.verify_truncated().await,
        }
    }

    pub async fn mark_clean_shutdown(&self) -> Result<()> {
        match self {
            Self::Segment(wal) => wal.mark_clean_shutdown().await,
            Self::Frame(wal) => wal.mark_clean_shutdown().await,
        }
    }

    pub async fn cleanup_old_segments(&self, retention_period: Duration) -> Result<()> {
        match self {
            Self::Segment(wal) => wal.cleanup_old_segments(retention_period).await,
            Self::Frame(wal) => wal.cleanup_old_segments(retention_period).await,
        }
    }

    pub async fn cleanup_flushed_segments(&self, grace_period: Duration) -> Result<()> {
        match self {
            Self::Segment(wal) => wal.cleanup_flushed_segments(grace_period).await,
            Self::Frame(wal) => wal.cleanup_flushed_segments(grace_period).await,
        }
    }

    pub async fn force_cleanup(&self) -> Result<()> {
        match self {
            Self::Segment(wal) => wal.force_cleanup().await,
            Self::Frame(wal) => wal.force_cleanup().await,
        }
    }

    pub async fn read_entries_for_recovery(&self) -> Result<Vec<WALEntry>> {
        match self {
            Self::Segment(wal) => wal.read_entries_from_disk().await,
            Self::Frame(wal) => wal.read_entries_from_disk().await,
        }
    }
}