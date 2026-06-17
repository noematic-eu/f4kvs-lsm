//! Adaptive Compaction Strategies for LSM Tree Engine
//!
//! This module provides adaptive compaction strategies that adjust based on
//! workload characteristics, system state, and performance metrics.

use crate::core::config::{
    CompactionConfig, CompactionPriority, CompactionStrategy, LevelConfig, SstableConfig,
};
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Adaptive compaction configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveCompactionConfig {
    /// Enable adaptive scheduling
    pub enable_adaptive_scheduling: bool,
    /// Workload detection window in seconds
    pub workload_detection_window: Duration,
    /// Write amplification threshold
    pub write_amplification_threshold: f64,
    /// Read performance threshold in milliseconds
    pub read_performance_threshold: Duration,
    /// Resource usage threshold (0.0 to 1.0)
    pub resource_usage_threshold: f64,
    /// Enable workload classification
    pub enable_workload_classification: bool,
    /// Enable resource-aware scheduling
    pub enable_resource_aware_scheduling: bool,
    /// Adaptation sensitivity (0.0 to 1.0)
    pub adaptation_sensitivity: f64,
}

impl Default for AdaptiveCompactionConfig {
    fn default() -> Self {
        Self {
            enable_adaptive_scheduling: true,
            workload_detection_window: Duration::from_secs(300), // 5 minutes
            write_amplification_threshold: 2.0,
            read_performance_threshold: Duration::from_millis(10),
            resource_usage_threshold: 0.8,
            enable_workload_classification: true,
            enable_resource_aware_scheduling: true,
            adaptation_sensitivity: 0.5,
        }
    }
}

/// Workload type classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkloadType {
    /// Write-heavy workload
    WriteHeavy,
    /// Read-heavy workload
    ReadHeavy,
    /// Mixed workload
    Mixed,
    /// Unknown workload
    Unknown,
}

/// Workload characteristics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadCharacteristics {
    /// Workload type
    pub workload_type: WorkloadType,
    /// Write rate (operations per second)
    pub write_rate: f64,
    /// Read rate (operations per second)
    pub read_rate: f64,
    /// Write amplification ratio
    pub write_amplification: f64,
    /// Average read latency in milliseconds
    pub avg_read_latency_ms: f64,
    /// P99 read latency in milliseconds
    pub p99_read_latency_ms: f64,
    /// Resource utilization (0.0 to 1.0)
    pub resource_utilization: f64,
    /// Timestamp of last update
    #[serde(skip, default = "Instant::now")]
    pub last_updated: Instant,
}

impl Default for WorkloadCharacteristics {
    fn default() -> Self {
        Self {
            workload_type: WorkloadType::Unknown,
            write_rate: 0.0,
            read_rate: 0.0,
            write_amplification: 1.0,
            avg_read_latency_ms: 0.0,
            p99_read_latency_ms: 0.0,
            resource_utilization: 0.0,
            last_updated: Instant::now(),
        }
    }
}

/// Performance metrics for adaptive decisions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    /// Write amplification ratio
    pub write_amplification: f64,
    /// Read latency percentiles
    pub read_latency_p50: Duration,
    /// 95th percentile read latency
    pub read_latency_p95: Duration,
    /// 99th percentile read latency
    pub read_latency_p99: Duration,
    /// Compaction efficiency (space reclaimed / time spent)
    pub compaction_efficiency: f64,
    /// Resource utilization during compaction
    pub cpu_utilization: f64,
    /// I/O utilization during compaction
    pub io_utilization: f64,
    /// Memory usage during compaction
    pub memory_usage: f64,
    /// Timestamp of metrics collection
    #[serde(skip, default = "Instant::now")]
    pub timestamp: Instant,
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self {
            write_amplification: 1.0,
            read_latency_p50: Duration::from_millis(1),
            read_latency_p95: Duration::from_millis(5),
            read_latency_p99: Duration::from_millis(10),
            compaction_efficiency: 1.0,
            cpu_utilization: 0.0,
            io_utilization: 0.0,
            memory_usage: 0.0,
            timestamp: Instant::now(),
        }
    }
}

/// Adaptive compaction manager
pub struct AdaptiveCompactionManager {
    config: AdaptiveCompactionConfig,
    base_config: CompactionConfig,
    level_config: LevelConfig,
    #[allow(dead_code)]
    sstable_config: SstableConfig,
    workload_characteristics: Arc<RwLock<WorkloadCharacteristics>>,
    performance_metrics: Arc<RwLock<PerformanceMetrics>>,
    workload_history: Arc<RwLock<Vec<WorkloadCharacteristics>>>,
    metrics_history: Arc<RwLock<Vec<PerformanceMetrics>>>,
}

impl AdaptiveCompactionManager {
    /// Create a new adaptive compaction manager
    pub fn new(
        adaptive_config: AdaptiveCompactionConfig,
        base_config: CompactionConfig,
        level_config: LevelConfig,
        sstable_config: SstableConfig,
    ) -> Self {
        Self {
            config: adaptive_config,
            base_config,
            level_config,
            sstable_config,
            workload_characteristics: Arc::new(RwLock::new(WorkloadCharacteristics::default())),
            performance_metrics: Arc::new(RwLock::new(PerformanceMetrics::default())),
            workload_history: Arc::new(RwLock::new(Vec::new())),
            metrics_history: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Update workload characteristics
    pub async fn update_workload_characteristics(
        &self,
        write_ops: u64,
        read_ops: u64,
        write_amplification: f64,
        read_latency_ms: f64,
        resource_utilization: f64,
    ) -> Result<()> {
        let mut characteristics = self.workload_characteristics.write().await;
        let now = Instant::now();
        let time_delta = now
            .duration_since(characteristics.last_updated)
            .as_secs_f64();

        if time_delta > 0.0 {
            // Calculate rates
            let write_rate = write_ops as f64 / time_delta;
            let read_rate = read_ops as f64 / time_delta;

            // Update characteristics
            characteristics.write_rate = write_rate;
            characteristics.read_rate = read_rate;
            characteristics.write_amplification = write_amplification;
            characteristics.avg_read_latency_ms = read_latency_ms;
            characteristics.p99_read_latency_ms = read_latency_ms * 2.0; // Rough estimate
            characteristics.resource_utilization = resource_utilization;
            characteristics.last_updated = now;

            // Classify workload type
            characteristics.workload_type = self.classify_workload_type(write_rate, read_rate);

            // Store in history
            let mut history = self.workload_history.write().await;
            history.push(characteristics.clone());

            // Keep only recent history (last 10 entries)
            if history.len() > 10 {
                history.remove(0);
            }
        }

        Ok(())
    }

    /// Update performance metrics
    #[allow(clippy::too_many_arguments)]
    pub async fn update_performance_metrics(
        &self,
        write_amplification: f64,
        read_latency_p50: Duration,
        read_latency_p95: Duration,
        read_latency_p99: Duration,
        compaction_efficiency: f64,
        cpu_utilization: f64,
        io_utilization: f64,
        memory_usage: f64,
    ) -> Result<()> {
        let mut metrics = self.performance_metrics.write().await;

        metrics.write_amplification = write_amplification;
        metrics.read_latency_p50 = read_latency_p50;
        metrics.read_latency_p95 = read_latency_p95;
        metrics.read_latency_p99 = read_latency_p99;
        metrics.compaction_efficiency = compaction_efficiency;
        metrics.cpu_utilization = cpu_utilization;
        metrics.io_utilization = io_utilization;
        metrics.memory_usage = memory_usage;
        metrics.timestamp = Instant::now();

        // Store in history
        let mut history = self.metrics_history.write().await;
        history.push(metrics.clone());

        // Keep only recent history (last 20 entries)
        if history.len() > 20 {
            history.remove(0);
        }

        Ok(())
    }

    /// Get the optimal compaction strategy for current workload
    pub async fn get_optimal_strategy(&self) -> CompactionStrategy {
        if !self.config.enable_adaptive_scheduling {
            return self.base_config.strategy;
        }

        let characteristics = self.workload_characteristics.read().await;

        match characteristics.workload_type {
            WorkloadType::WriteHeavy => {
                // For write-heavy workloads, prioritize write amplification reduction
                if characteristics.write_amplification > self.config.write_amplification_threshold {
                    CompactionStrategy::SizeTiered
                } else {
                    CompactionStrategy::Leveled
                }
            }
            WorkloadType::ReadHeavy => {
                // For read-heavy workloads, prioritize read performance
                if characteristics.p99_read_latency_ms
                    > self.config.read_performance_threshold.as_millis() as f64
                {
                    CompactionStrategy::Leveled
                } else {
                    CompactionStrategy::Hybrid
                }
            }
            WorkloadType::Mixed => {
                // For mixed workloads, use hybrid approach
                CompactionStrategy::Hybrid
            }
            WorkloadType::Unknown => {
                // Default to base strategy
                self.base_config.strategy
            }
        }
    }

    /// Get the optimal compaction priority for current workload
    pub async fn get_optimal_priority(&self) -> CompactionPriority {
        if !self.config.enable_adaptive_scheduling {
            return self.base_config.priority;
        }

        let characteristics = self.workload_characteristics.read().await;
        let metrics = self.performance_metrics.read().await;

        // Determine priority based on workload and performance
        if characteristics.workload_type == WorkloadType::WriteHeavy {
            if characteristics.write_amplification > self.config.write_amplification_threshold {
                CompactionPriority::WriteOptimized
            } else {
                CompactionPriority::Balanced
            }
        } else if characteristics.workload_type == WorkloadType::ReadHeavy {
            if characteristics.p99_read_latency_ms
                > self.config.read_performance_threshold.as_millis() as f64
            {
                CompactionPriority::ReadOptimized
            } else {
                CompactionPriority::Balanced
            }
        } else if metrics.write_amplification > self.config.write_amplification_threshold {
            CompactionPriority::SpaceOptimized
        } else {
            CompactionPriority::Balanced
        }
    }

    /// Check if compaction should be scheduled based on resource availability
    pub async fn should_schedule_compaction(&self) -> bool {
        if !self.config.enable_resource_aware_scheduling {
            return true;
        }

        let characteristics = self.workload_characteristics.read().await;
        let metrics = self.performance_metrics.read().await;

        // Don't schedule if resource utilization is too high
        if characteristics.resource_utilization > self.config.resource_usage_threshold {
            return false;
        }

        // Don't schedule if CPU or I/O utilization is too high
        if metrics.cpu_utilization > self.config.resource_usage_threshold
            || metrics.io_utilization > self.config.resource_usage_threshold
        {
            return false;
        }

        true
    }

    /// Get adaptive compaction configuration for a level
    pub async fn get_adaptive_level_config(&self, level: usize) -> Result<AdaptiveLevelConfig> {
        let characteristics = self.workload_characteristics.read().await;
        let metrics = self.performance_metrics.read().await;

        // Calculate adaptive parameters based on workload and performance
        let base_size_multiplier = self.level_config.size_multiplier;
        let adaptive_multiplier = self
            .calculate_adaptive_multiplier(&characteristics, &metrics)
            .await;

        let _base_max_sstables = self.level_config.max_sstables_per_level;
        let adaptive_max_sstables = self
            .calculate_adaptive_max_sstables(level, &characteristics)
            .await;

        Ok(AdaptiveLevelConfig {
            level,
            size_multiplier: base_size_multiplier * adaptive_multiplier,
            max_sstables_per_level: adaptive_max_sstables,
            compaction_ratio: self
                .calculate_compaction_ratio(level, &characteristics)
                .await,
            enable_compaction: self
                .should_enable_level_compaction(level, &characteristics)
                .await,
        })
    }

    /// Classify workload type based on read/write ratios
    fn classify_workload_type(&self, write_rate: f64, read_rate: f64) -> WorkloadType {
        let total_rate = write_rate + read_rate;
        if total_rate == 0.0 {
            return WorkloadType::Unknown;
        }

        let write_ratio = write_rate / total_rate;
        let read_ratio = read_rate / total_rate;

        if write_ratio > 0.7 {
            WorkloadType::WriteHeavy
        } else if read_ratio > 0.7 {
            WorkloadType::ReadHeavy
        } else if write_ratio > 0.3 && read_ratio > 0.3 {
            WorkloadType::Mixed
        } else {
            WorkloadType::Unknown
        }
    }

    /// Calculate adaptive size multiplier based on workload characteristics
    async fn calculate_adaptive_multiplier(
        &self,
        characteristics: &WorkloadCharacteristics,
        metrics: &PerformanceMetrics,
    ) -> f64 {
        let mut multiplier = 1.0;

        // Adjust based on workload type
        match characteristics.workload_type {
            WorkloadType::WriteHeavy => {
                // Increase multiplier for write-heavy workloads to reduce write amplification
                multiplier *= 1.2;
            }
            WorkloadType::ReadHeavy => {
                // Decrease multiplier for read-heavy workloads to improve read performance
                multiplier *= 0.8;
            }
            WorkloadType::Mixed => {
                // Slight adjustment for mixed workloads
                multiplier *= 1.05;
            }
            WorkloadType::Unknown => {
                // No adjustment for unknown workloads
            }
        }

        // Adjust based on write amplification
        if metrics.write_amplification > self.config.write_amplification_threshold {
            multiplier *= 1.1; // Increase to reduce write amplification
        }

        // Adjust based on read latency
        if metrics.read_latency_p99 > self.config.read_performance_threshold {
            multiplier *= 0.9; // Decrease to improve read performance
        }

        // Apply adaptation sensitivity
        multiplier = 1.0 + (multiplier - 1.0) * self.config.adaptation_sensitivity;

        // Clamp to reasonable bounds
        multiplier.clamp(0.5, 2.0)
    }

    /// Calculate adaptive max SSTables per level
    async fn calculate_adaptive_max_sstables(
        &self,
        _level: usize,
        characteristics: &WorkloadCharacteristics,
    ) -> usize {
        let base_max = self.level_config.max_sstables_per_level;

        match characteristics.workload_type {
            WorkloadType::WriteHeavy => {
                // Allow more SSTables for write-heavy workloads to reduce compaction frequency
                (base_max as f64 * 1.5) as usize
            }
            WorkloadType::ReadHeavy => {
                // Reduce SSTables for read-heavy workloads to improve read performance
                (base_max as f64 * 0.7) as usize
            }
            WorkloadType::Mixed => {
                // Slight adjustment for mixed workloads
                (base_max as f64 * 1.1) as usize
            }
            WorkloadType::Unknown => base_max,
        }
    }

    /// Calculate compaction ratio for a level
    async fn calculate_compaction_ratio(
        &self,
        _level: usize,
        characteristics: &WorkloadCharacteristics,
    ) -> f64 {
        let base_ratio = 0.1; // 10% default

        match characteristics.workload_type {
            WorkloadType::WriteHeavy => {
                // Higher compaction ratio for write-heavy workloads
                base_ratio * 1.5
            }
            WorkloadType::ReadHeavy => {
                // Lower compaction ratio for read-heavy workloads
                base_ratio * 0.7
            }
            WorkloadType::Mixed => {
                // Moderate compaction ratio for mixed workloads
                base_ratio * 1.1
            }
            WorkloadType::Unknown => base_ratio,
        }
    }

    /// Determine if compaction should be enabled for a level
    async fn should_enable_level_compaction(
        &self,
        level: usize,
        characteristics: &WorkloadCharacteristics,
    ) -> bool {
        // Always enable compaction for L0
        if level == 0 {
            return true;
        }

        // For higher levels, enable based on workload type
        match characteristics.workload_type {
            WorkloadType::WriteHeavy => true, // Always enable for write-heavy
            WorkloadType::ReadHeavy => level <= 3, // Limit for read-heavy
            WorkloadType::Mixed => level <= 5, // Moderate limit for mixed
            WorkloadType::Unknown => true,    // Default enable
        }
    }

    /// Get current workload characteristics
    pub async fn get_workload_characteristics(&self) -> WorkloadCharacteristics {
        self.workload_characteristics.read().await.clone()
    }

    /// Get current performance metrics
    pub async fn get_performance_metrics(&self) -> PerformanceMetrics {
        self.performance_metrics.read().await.clone()
    }

    /// Get workload history
    pub async fn get_workload_history(&self) -> Vec<WorkloadCharacteristics> {
        self.workload_history.read().await.clone()
    }

    /// Get metrics history
    pub async fn get_metrics_history(&self) -> Vec<PerformanceMetrics> {
        self.metrics_history.read().await.clone()
    }

    /// Reset all adaptive data
    pub async fn reset(&self) {
        *self.workload_characteristics.write().await = WorkloadCharacteristics::default();
        *self.performance_metrics.write().await = PerformanceMetrics::default();
        *self.workload_history.write().await = Vec::new();
        *self.metrics_history.write().await = Vec::new();
    }
}

/// Adaptive level configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveLevelConfig {
    /// Level number
    pub level: usize,
    /// Adaptive size multiplier
    pub size_multiplier: f64,
    /// Adaptive max SSTables per level
    pub max_sstables_per_level: usize,
    /// Compaction ratio
    pub compaction_ratio: f64,
    /// Whether compaction is enabled for this level
    pub enable_compaction: bool,
}

impl Default for AdaptiveLevelConfig {
    fn default() -> Self {
        Self {
            level: 0,
            size_multiplier: 1.0,
            max_sstables_per_level: 10,
            compaction_ratio: 0.1,
            enable_compaction: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn create_test_adaptive_manager() -> AdaptiveCompactionManager {
        let adaptive_config = AdaptiveCompactionConfig::default();
        let base_config = CompactionConfig::default();
        let level_config = LevelConfig::default();
        let sstable_config = SstableConfig::default();

        AdaptiveCompactionManager::new(adaptive_config, base_config, level_config, sstable_config)
    }

    #[tokio::test]
    async fn test_workload_classification() {
        let manager = create_test_adaptive_manager();

        // Test write-heavy workload
        manager
            .update_workload_characteristics(1000, 100, 2.5, 5.0, 0.6)
            .await
            .expect("test operation failed");
        let characteristics = manager.get_workload_characteristics().await;
        assert_eq!(characteristics.workload_type, WorkloadType::WriteHeavy);

        // Test read-heavy workload
        manager
            .update_workload_characteristics(100, 1000, 1.2, 3.0, 0.4)
            .await
            .expect("test operation failed");
        let characteristics = manager.get_workload_characteristics().await;
        assert_eq!(characteristics.workload_type, WorkloadType::ReadHeavy);

        // Test mixed workload
        manager
            .update_workload_characteristics(500, 500, 1.8, 4.0, 0.5)
            .await
            .expect("test operation failed");
        let characteristics = manager.get_workload_characteristics().await;
        assert_eq!(characteristics.workload_type, WorkloadType::Mixed);
    }

    #[tokio::test]
    async fn test_optimal_strategy_selection() {
        let manager = create_test_adaptive_manager();

        // Test write-heavy strategy
        manager
            .update_workload_characteristics(1000, 100, 3.0, 5.0, 0.6)
            .await
            .expect("test operation failed");
        let strategy = manager.get_optimal_strategy().await;
        assert_eq!(strategy, CompactionStrategy::SizeTiered);

        // Test read-heavy strategy
        manager
            .update_workload_characteristics(100, 1000, 1.2, 15.0, 0.4)
            .await
            .expect("test operation failed");
        let strategy = manager.get_optimal_strategy().await;
        assert_eq!(strategy, CompactionStrategy::Leveled);
    }

    #[tokio::test]
    async fn test_resource_aware_scheduling() {
        let manager = create_test_adaptive_manager();

        // Test with low resource utilization (should schedule)
        manager
            .update_workload_characteristics(100, 100, 1.5, 5.0, 0.3)
            .await
            .expect("test operation failed");
        manager
            .update_performance_metrics(
                1.5,
                Duration::from_millis(1),
                Duration::from_millis(3),
                Duration::from_millis(5),
                1.0,
                0.3,
                0.2,
                0.4,
            )
            .await
            .expect("test operation failed");

        assert!(manager.should_schedule_compaction().await);

        // Test with high resource utilization (should not schedule)
        manager
            .update_workload_characteristics(100, 100, 1.5, 5.0, 0.9)
            .await
            .expect("test operation failed");
        manager
            .update_performance_metrics(
                1.5,
                Duration::from_millis(1),
                Duration::from_millis(3),
                Duration::from_millis(5),
                1.0,
                0.9,
                0.9,
                0.8,
            )
            .await
            .expect("test operation failed");

        assert!(!manager.should_schedule_compaction().await);
    }

    #[tokio::test]
    async fn test_adaptive_level_config() {
        let manager = create_test_adaptive_manager();

        // Test write-heavy workload
        manager
            .update_workload_characteristics(1000, 100, 2.5, 5.0, 0.6)
            .await
            .expect("test operation failed");
        let level_config = manager
            .get_adaptive_level_config(1)
            .await
            .expect("test operation failed");

        assert!(level_config.size_multiplier > 1.0); // Should be increased for write-heavy
        assert!(level_config.max_sstables_per_level > 10); // Should be increased
        assert!(level_config.enable_compaction); // Should be enabled

        // Test read-heavy workload
        manager
            .update_workload_characteristics(100, 1000, 1.2, 3.0, 0.4)
            .await
            .expect("test operation failed");
        let level_config = manager
            .get_adaptive_level_config(1)
            .await
            .expect("test operation failed");

        assert!(level_config.size_multiplier < 10.0); // Should be decreased for read-heavy (from base 10.0)
        assert!(level_config.max_sstables_per_level < 10); // Should be decreased
    }

    #[tokio::test]
    async fn test_reset_functionality() {
        let manager = create_test_adaptive_manager();

        // Update some data
        manager
            .update_workload_characteristics(1000, 100, 2.5, 5.0, 0.6)
            .await
            .expect("test operation failed");
        manager
            .update_performance_metrics(
                2.5,
                Duration::from_millis(1),
                Duration::from_millis(3),
                Duration::from_millis(5),
                1.0,
                0.6,
                0.4,
                0.5,
            )
            .await
            .expect("test operation failed");

        // Verify data exists
        let characteristics = manager.get_workload_characteristics().await;
        assert_eq!(characteristics.workload_type, WorkloadType::WriteHeavy);

        // Reset
        manager.reset().await;

        // Verify data is reset
        let characteristics = manager.get_workload_characteristics().await;
        assert_eq!(characteristics.workload_type, WorkloadType::Unknown);
        assert_eq!(characteristics.write_rate, 0.0);
    }
}
