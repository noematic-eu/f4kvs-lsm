//! Compaction logic for LSM Tree Engine
//!
//! This module contains the compaction management and strategies for
//! maintaining LSM tree performance and storage efficiency.

pub mod adaptive;
pub mod manager;

pub use adaptive::{AdaptiveCompactionConfig, AdaptiveCompactionManager, WorkloadType};
pub use manager::CompactionManager;
