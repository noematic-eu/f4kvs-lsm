//! Core LSM Tree Engine components
//!
//! This module contains the core LSM tree logic including the main engine
//! implementation and configuration management.

pub mod config;
pub mod engine;
pub mod metrics;

pub use config::{LsmConfig, SstableReadMode};
pub use engine::{LsmTreeEngine, PrefixScanState};
pub use metrics::{
    LevelMetrics, OptimizationPriority, OptimizationRecommendation, PerformanceMetrics,
};

/// Re-export the main engine type
pub type LsmStorage = LsmTreeEngine;
