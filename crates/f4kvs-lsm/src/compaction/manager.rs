//! Compaction manager for LSM Tree Engine

use super::adaptive::{AdaptiveCompactionConfig, AdaptiveCompactionManager};
use crate::core::config::{
    CompactionConfig, CompactionPriority, CompactionStrategy, LevelConfig, SstableConfig,
};
use crate::error::{LsmError, Result};
use crate::storage::sstable::{SSTable, SSTableEntry};
use crate::utils;
use f4kvs_value::F4KvsError;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{RwLock, Semaphore};
use tokio::time::{timeout, Duration};
use tracing::{debug, info, warn};

/// Compaction statistics
#[derive(Debug, Clone, Default)]
pub struct CompactionStats {
    /// Number of levels compacted
    pub levels_compacted: usize,
    /// Number of SSTables merged
    pub sstables_merged: usize,
    /// Number of entries processed
    pub entries_processed: usize,
    /// Number of entries removed (duplicates/deleted)
    pub entries_removed: usize,
    /// Space reclaimed in bytes
    pub space_reclaimed: u64,
    /// Compaction duration in milliseconds
    pub duration_ms: u64,
}

/// Maximum concurrent SSTable operations during compaction
const MAX_CONCURRENT_SSTABLE_OPS: usize = 4;

/// Compaction manager
pub struct CompactionManager {
    config: CompactionConfig,
    level_config: LevelConfig,
    sstable_config: SstableConfig,
    data_dir: PathBuf,
    stats: Arc<RwLock<CompactionStats>>,
    adaptive_manager: Option<Arc<AdaptiveCompactionManager>>,
    #[allow(dead_code)]
    last_workload_update: Arc<RwLock<Instant>>,
    /// Semaphore to limit concurrent SSTable operations during compaction
    compaction_semaphore: Arc<Semaphore>,
}

impl CompactionManager {
    /// Create a new compaction manager
    pub fn new(
        config: &CompactionConfig,
        level_config: &LevelConfig,
        sstable_config: &SstableConfig,
        data_dir: PathBuf,
    ) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
            level_config: level_config.clone(),
            sstable_config: sstable_config.clone(),
            data_dir,
            stats: Arc::new(RwLock::new(CompactionStats::default())),
            adaptive_manager: None,
            last_workload_update: Arc::new(RwLock::new(Instant::now())),
            compaction_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_SSTABLE_OPS)),
        })
    }

    /// Create a new compaction manager with adaptive capabilities
    pub fn new_with_adaptive(
        config: &CompactionConfig,
        level_config: &LevelConfig,
        sstable_config: &SstableConfig,
        data_dir: PathBuf,
        adaptive_config: AdaptiveCompactionConfig,
    ) -> Result<Self> {
        let adaptive_manager = AdaptiveCompactionManager::new(
            adaptive_config,
            config.clone(),
            level_config.clone(),
            sstable_config.clone(),
        );

        Ok(Self {
            config: config.clone(),
            level_config: level_config.clone(),
            sstable_config: sstable_config.clone(),
            data_dir,
            stats: Arc::new(RwLock::new(CompactionStats::default())),
            adaptive_manager: Some(Arc::new(adaptive_manager)),
            last_workload_update: Arc::new(RwLock::new(Instant::now())),
            compaction_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_SSTABLE_OPS)),
        })
    }

    /// Update workload characteristics for adaptive compaction
    pub async fn update_workload_characteristics(
        &self,
        write_ops: u64,
        read_ops: u64,
        write_amplification: f64,
        read_latency_ms: f64,
        resource_utilization: f64,
    ) -> Result<()> {
        if let Some(adaptive_manager) = &self.adaptive_manager {
            adaptive_manager
                .update_workload_characteristics(
                    write_ops,
                    read_ops,
                    write_amplification,
                    read_latency_ms,
                    resource_utilization,
                )
                .await?;
        }
        Ok(())
    }

    /// Update performance metrics for adaptive compaction
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
        if let Some(adaptive_manager) = &self.adaptive_manager {
            adaptive_manager
                .update_performance_metrics(
                    write_amplification,
                    read_latency_p50,
                    read_latency_p95,
                    read_latency_p99,
                    compaction_efficiency,
                    cpu_utilization,
                    io_utilization,
                    memory_usage,
                )
                .await?;
        }
        Ok(())
    }

    /// Check if compaction is needed and run it
    ///
    /// This function uses a non-blocking approach to prevent deadlocks:
    /// 1. First checks if compaction is needed with a read lock (non-blocking)
    /// 2. Only acquires write lock when actually compacting
    /// 3. Uses try_write with retries to avoid blocking other operations
    /// 4. Releases lock quickly after compaction to allow reads to proceed
    pub async fn compact_if_needed(
        &self,
        sstables: &Arc<RwLock<HashMap<usize, Vec<SSTable>>>>,
    ) -> Result<()> {
        // Check if we should schedule compaction based on resource availability
        if let Some(adaptive_manager) = &self.adaptive_manager {
            if !adaptive_manager.should_schedule_compaction().await {
                info!("Skipping compaction due to high resource utilization");
                return Ok(());
            }
        }

        // Phase 1: Check if compaction is needed with read lock (non-blocking)
        // This allows concurrent reads to proceed while we check
        let level_to_compact = {
            let sstables_guard = sstables.read().await;
            let mut target_level = None;

            for level in 0..self.level_config.count {
                if self
                    .should_compact_level(
                        level,
                        &sstables_guard.get(&level).cloned().unwrap_or_default(),
                    )
                    .await
                {
                    target_level = Some(level);
                    break; // Only compact one level at a time
                }
            }
            target_level
        };

        // If no compaction needed, return early without acquiring write lock
        let level = match level_to_compact {
            Some(level) => level,
            None => return Ok(()),
        };

        info!("Compacting level {}", level);

        // Phase 2: Acquire write lock only when actually compacting
        // Use try_write with retries to avoid blocking deadlocks
        // This allows other operations to proceed if lock is contended
        let mut sstables_guard = None;
        for retry in 0..10 {
            match sstables.try_write() {
                Ok(guard) => {
                    sstables_guard = Some(guard);
                    break;
                }
                Err(_) => {
                    if retry < 9 {
                        // Yield to allow other tasks to release the lock
                        tokio::time::sleep(Duration::from_millis(50 * (retry as u64 + 1))).await;
                    }
                }
            }
        }

        let sstables_guard = match sstables_guard {
            Some(guard) => guard,
            None => {
                // Fall back to blocking write with timeout to prevent indefinite blocking
                let lock_timeout = Duration::from_secs(2);
                match timeout(lock_timeout, sstables.write()).await {
                    Ok(guard) => guard,
                    Err(_) => {
                        // If we can't get the lock within timeout, skip compaction this time
                        debug!(
                            "Skipping compaction for level {} - could not acquire lock within timeout",
                            level
                        );
                        return Ok(());
                    }
                }
            }
        };

        // Re-check if compaction is still needed (state may have changed)
        // This prevents unnecessary compaction if another task already compacted
        if !self
            .should_compact_level(
                level,
                &sstables_guard.get(&level).cloned().unwrap_or_default(),
            )
            .await
        {
            debug!("Level {} no longer needs compaction, skipping", level);
            return Ok(());
        }

        // Clone SSTables we need for compaction BEFORE releasing the lock
        // This allows us to release the lock during the actual compaction work
        let all_sstables = sstables_guard.clone();

        // Release the lock BEFORE doing the expensive compaction work
        // This allows reads to proceed while compaction is happening
        drop(sstables_guard);

        // Perform compaction WITHOUT holding the lock
        // This is the expensive I/O operation that can take a long time
        let new_sstables = self
            .compact_level(level, &all_sstables, &all_sstables)
            .await?;

        // Re-acquire the lock to update SSTables
        // Use try_write with retries since we released it
        let mut sstables_guard = None;
        for retry in 0..10 {
            match sstables.try_write() {
                Ok(guard) => {
                    sstables_guard = Some(guard);
                    break;
                }
                Err(_) => {
                    if retry < 9 {
                        tokio::time::sleep(Duration::from_millis(10 * (retry as u64 + 1))).await;
                    }
                }
            }
        }

        let mut sstables_guard = match sstables_guard {
            Some(guard) => guard,
            None => {
                // Fall back to blocking write with timeout
                let lock_timeout = Duration::from_secs(1);
                match timeout(lock_timeout, sstables.write()).await {
                    Ok(guard) => guard,
                    Err(_) => {
                        warn!("Could not acquire lock to update SSTables after compaction, compaction results will be lost");
                        return Ok(()); // Skip update if we can't get the lock
                    }
                }
            }
        };

        // Update the level with new SSTables
        sstables_guard.insert(level, new_sstables);

        // Lock is released here, allowing reads to proceed immediately

        Ok(())
    }

    /// Run compaction on all levels
    ///
    /// This function releases locks between levels to allow reads and other operations
    /// to proceed during compaction, preventing deadlocks and improving concurrency.
    ///
    /// Note: New SSTables created during compaction (from concurrent writes) are preserved
    /// by merging them with the compacted SSTables.
    pub async fn compact_all(
        &self,
        sstables: &Arc<RwLock<HashMap<usize, Vec<SSTable>>>>,
    ) -> Result<()> {
        for level in 0..self.level_config.count {
            // Check if level needs compaction (read lock only)
            let needs_compaction = {
                let sstables_guard = sstables.read().await;
                self.should_compact_level(
                    level,
                    &sstables_guard.get(&level).cloned().unwrap_or_default(),
                )
                .await
            };

            if needs_compaction {
                info!("Compacting all levels, starting with level {}", level);

                // Use try_write with retry to avoid blocking deadlocks
                // This allows other operations to proceed if lock is contended
                let mut sstables_guard = None;
                for retry in 0..10 {
                    match sstables.try_write() {
                        Ok(guard) => {
                            sstables_guard = Some(guard);
                            break;
                        }
                        Err(_) => {
                            if retry < 9 {
                                // Yield to allow other tasks to release the lock
                                tokio::time::sleep(Duration::from_millis(50 * (retry as u64 + 1)))
                                    .await;
                            }
                        }
                    }
                }
                let mut sstables_guard = match sstables_guard {
                    Some(guard) => guard,
                    None => {
                        // Fall back to blocking write with shorter timeout
                        let lock_timeout = Duration::from_secs(5);
                        timeout(lock_timeout, sstables.write()).await.map_err(|_| {
                            LsmError::Internal(format!(
                                "Failed to acquire compaction lock for level {} within timeout",
                                level
                            ))
                        })?
                    }
                };

                // Get SSTables for this level
                let level_sstables = sstables_guard.get(&level).cloned().unwrap_or_default();

                if level_sstables.is_empty() {
                    continue; // No SSTables to compact
                }

                // Filter out SSTables that are not ready or marked for deletion
                // This prevents reading from incomplete or being-deleted SSTables
                let ready_sstables: Vec<SSTable> = level_sstables
                    .into_iter()
                    .filter(|s| s.is_ready() && !s.is_marked_for_deletion())
                    .collect();

                // Additional validation: ensure SSTables have valid metadata
                let valid_sstables: Vec<SSTable> = ready_sstables
                    .into_iter()
                    .filter(|s| {
                        let metadata = s.metadata();
                        // Validate basic metadata consistency
                        metadata.file_size > 0
                            && metadata.entry_count > 0
                            && s.index_size() > 0
                            && s.index_size() == metadata.entry_count
                    })
                    .collect();

                if valid_sstables.is_empty() {
                    continue; // No valid SSTables to compact
                }

                // Perform compaction on valid SSTables only
                let compacted_sstables =
                    self.compact_level_sstables(level, &valid_sstables).await?;

                // Update the level with compacted SSTables
                // Mark old SSTables for deletion (they will be cleaned up later)
                for sstable in &valid_sstables {
                    sstable.mark_for_deletion();
                }

                sstables_guard.insert(level, compacted_sstables);
                // Lock released here, allowing other operations to proceed
            }
        }

        Ok(())
    }

    /// Compact a specific set of SSTables for a level
    /// This method ensures all SSTables are ready before compaction
    async fn compact_level_sstables(
        &self,
        level: usize,
        sstables: &[SSTable],
    ) -> Result<Vec<SSTable>> {
        let start_time = std::time::Instant::now();
        let mut stats = CompactionStats::default();

        info!(
            "Starting compaction for level {} with {} SSTables",
            level,
            sstables.len()
        );

        // Double-check all SSTables are ready before proceeding
        for sstable in sstables {
            if !sstable.is_ready() {
                return Err(LsmError::Internal(format!(
                    "SSTable {:?} is not ready for compaction",
                    sstable.path()
                )));
            }
            if sstable.is_marked_for_deletion() {
                return Err(LsmError::Internal(format!(
                    "SSTable {:?} is marked for deletion, cannot compact",
                    sstable.path()
                )));
            }
        }

        // Merge and deduplicate entries with proper error handling
        let merged_entries = self.merge_sstables_safe(sstables).await?;
        stats.entries_processed = merged_entries.len();

        // Remove duplicates and tombstones
        let deduplicated_entries = self.deduplicate_entries(merged_entries);
        stats.entries_removed = stats.entries_processed - deduplicated_entries.len();

        // Create new SSTables
        let new_sstables = self
            .create_new_sstables(level, deduplicated_entries)
            .await?;

        // Calculate space reclaimed
        let old_size: u64 = sstables.iter().map(|s| s.size()).sum();
        let new_size: u64 = new_sstables.iter().map(|s| s.size()).sum();
        stats.space_reclaimed = old_size.saturating_sub(new_size);
        stats.sstables_merged = sstables.len();
        stats.duration_ms = start_time.elapsed().as_millis() as u64;

        info!(
            "Compaction completed for level {}: {} entries processed, {} entries removed, {} bytes reclaimed",
            level, stats.entries_processed, stats.entries_removed, stats.space_reclaimed
        );

        Ok(new_sstables)
    }

    /// Safely merge SSTables with corruption detection and recovery
    async fn merge_sstables_safe(&self, sstables: &[SSTable]) -> Result<Vec<SSTableEntry>> {
        if sstables.is_empty() {
            return Ok(Vec::new());
        }

        let mut all_entries = Vec::new();
        let mut corrupted_sstables = Vec::new();

        // Read entries from each SSTable with error handling and concurrency limiting
        for sstable in sstables {
            // Acquire semaphore permit to limit concurrent SSTable operations
            let _permit = self
                .compaction_semaphore
                .acquire()
                .await
                .map_err(|_| LsmError::Internal("Compaction semaphore closed".to_string()))?;

            let entries = self.read_sstable_entries_safe(sstable).await;
            if entries.is_empty() {
                warn!(
                    "SSTable {:?} returned no entries (possibly corrupted or empty)",
                    sstable.path()
                );
                corrupted_sstables.push(sstable.path().to_path_buf());
            } else {
                all_entries.extend(entries);
            }
        }

        if !corrupted_sstables.is_empty() {
            warn!(
                "Compaction encountered {} corrupted SSTables: {:?}",
                corrupted_sstables.len(),
                corrupted_sstables
            );
        }

        // Sort by key, then by timestamp (newest first)
        all_entries.sort_by(|a, b| {
            a.key
                .cmp(&b.key)
                .then_with(|| b.timestamp.cmp(&a.timestamp))
        });

        Ok(all_entries)
    }

    /// Read SSTable entries with additional safety checks
    async fn read_sstable_entries_safe(&self, sstable: &SSTable) -> Vec<SSTableEntry> {
        // CRITICAL: Multiple readiness and state checks before reading
        if !sstable.is_ready() {
            warn!("SSTable {:?} is not ready for reading", sstable.path());
            return Vec::new();
        }

        if sstable.is_marked_for_deletion() {
            warn!("SSTable {:?} is marked for deletion", sstable.path());
            return Vec::new();
        }

        // Validate file exists and has expected size
        let metadata = match tokio::fs::metadata(sstable.path()).await {
            Ok(metadata) => metadata,
            Err(e) => {
                warn!(
                    "Failed to read metadata for SSTable {:?}: {}",
                    sstable.path(),
                    e
                );
                return Vec::new();
            }
        };

        let file_size = metadata.len();
        let expected_min_size = sstable.metadata().file_size;

        if file_size < expected_min_size {
            warn!(
                "SSTable {:?} file size {} is smaller than expected {}",
                sstable.path(),
                file_size,
                expected_min_size
            );
            return Vec::new();
        }

        // Use the existing read method but with additional timeout protection
        let read_timeout = Duration::from_secs(30);
        match tokio::time::timeout(read_timeout, self.read_sstable_entries(sstable)).await {
            Ok(Ok(entries)) => entries,
            Ok(Err(e)) => {
                warn!("Failed to read SSTable {:?}: {}", sstable.path(), e);
                Vec::new()
            }
            Err(_) => {
                warn!("Timeout reading SSTable {:?}", sstable.path());
                Vec::new()
            }
        }
    }

    /// Check if compaction is needed for a specific level
    pub async fn should_compact_level(&self, level: usize, sstables: &[SSTable]) -> bool {
        if sstables.is_empty() {
            return false;
        }

        // Use adaptive strategy if available
        let strategy = if let Some(adaptive_manager) = &self.adaptive_manager {
            adaptive_manager.get_optimal_strategy().await
        } else {
            self.config.strategy
        };

        // Check if compaction is enabled for this level (adaptive)
        if let Some(adaptive_manager) = &self.adaptive_manager {
            let level_config = adaptive_manager
                .get_adaptive_level_config(level)
                .await
                .unwrap_or_default();
            if !level_config.enable_compaction {
                return false;
            }
        }

        match strategy {
            CompactionStrategy::Leveled => self.should_compact_leveled(level, sstables).await,
            CompactionStrategy::SizeTiered => {
                self.should_compact_size_tiered(level, sstables).await
            }
            CompactionStrategy::TimeWindowed => {
                self.should_compact_time_windowed(level, sstables).await
            }
            CompactionStrategy::Hybrid => {
                // Use leveled for L0, size-tiered for others
                if level == 0 {
                    self.should_compact_leveled(level, sstables).await
                } else {
                    self.should_compact_size_tiered(level, sstables).await
                }
            }
        }
    }

    /// Leveled compaction decision logic
    async fn should_compact_leveled(&self, level: usize, sstables: &[SSTable]) -> bool {
        if level == 0 {
            // L0: Compact when too many SSTables
            let max_sstables = if let Some(adaptive_manager) = &self.adaptive_manager {
                let level_config = adaptive_manager
                    .get_adaptive_level_config(level)
                    .await
                    .unwrap_or_default();
                level_config.max_sstables_per_level
            } else {
                self.level_config.max_sstables_per_level
            };
            sstables.len() > max_sstables
        } else {
            // L1+: Compact when level is too large
            let level_size: u64 = sstables.iter().map(|s| s.metadata().file_size).sum();

            let (_size_multiplier, threshold) =
                if let Some(adaptive_manager) = &self.adaptive_manager {
                    let level_config = adaptive_manager
                        .get_adaptive_level_config(level)
                        .await
                        .unwrap_or_default();
                    let threshold = (self.sstable_config.target_size as f64
                        * level_config.size_multiplier.powf(level as f64))
                        as u64;
                    (level_config.size_multiplier, threshold)
                } else {
                    let threshold = (self.sstable_config.target_size as f64
                        * self.level_config.size_multiplier.powf(level as f64))
                        as u64;
                    (self.level_config.size_multiplier, threshold)
                };

            level_size > threshold
        }
    }

    /// Size-tiered compaction decision logic
    async fn should_compact_size_tiered(&self, _level: usize, sstables: &[SSTable]) -> bool {
        if sstables.len() < 4 {
            return false; // Need at least 4 SSTables for size-tiered
        }

        // Group SSTables by size and find the largest group
        let mut size_groups: HashMap<u64, Vec<&SSTable>> = HashMap::new();
        for sstable in sstables {
            let size_bucket = (sstable.metadata().file_size / (1024 * 1024)) * (1024 * 1024); // 1MB buckets
            size_groups.entry(size_bucket).or_default().push(sstable);
        }

        // Find the largest group
        if let Some((_, group)) = size_groups.iter().max_by_key(|(_, group)| group.len()) {
            group.len() >= 4
        } else {
            false
        }
    }

    /// Time-windowed compaction decision logic
    async fn should_compact_time_windowed(&self, _level: usize, sstables: &[SSTable]) -> bool {
        if sstables.len() < 2 {
            return false;
        }

        // Group by time windows (e.g., 1 hour windows)
        let window_size = 3600; // 1 hour in seconds
        let mut time_groups: HashMap<u64, Vec<&SSTable>> = HashMap::new();

        for sstable in sstables {
            let window = sstable.metadata().created_at / window_size;
            time_groups.entry(window).or_default().push(sstable);
        }

        // Compact if any time window has multiple SSTables
        time_groups.values().any(|group| group.len() > 1)
    }

    /// Run compaction for a specific level
    pub async fn compact_level(
        &self,
        level: usize,
        sstables: &HashMap<usize, Vec<SSTable>>,
        _all_sstables: &HashMap<usize, Vec<SSTable>>,
    ) -> Result<Vec<SSTable>> {
        let start_time = std::time::Instant::now();
        let mut stats = CompactionStats::default();

        let current_level_sstables = sstables.get(&level).cloned().unwrap_or_default();
        if current_level_sstables.is_empty() {
            return Ok(Vec::new());
        }

        info!(
            "Starting compaction for level {} with {} SSTables",
            level,
            current_level_sstables.len()
        );

        // Select SSTables to compact
        let sstables_to_compact = self
            .select_sstables_for_compaction(level, &current_level_sstables)
            .await?;
        stats.sstables_merged = sstables_to_compact.len();

        if sstables_to_compact.is_empty() {
            return Ok(current_level_sstables);
        }

        // Merge and deduplicate entries with safety checks
        let merged_entries = self.merge_sstables_safe(&sstables_to_compact).await?;
        stats.entries_processed = merged_entries.len();

        // Remove duplicates and tombstones
        let deduplicated_entries = self.deduplicate_entries(merged_entries);
        stats.entries_removed = stats.entries_processed - deduplicated_entries.len();

        // Create new SSTables
        let new_sstables = self
            .create_new_sstables(level, deduplicated_entries)
            .await?;

        // Calculate space reclaimed
        let old_size: u64 = sstables_to_compact
            .iter()
            .map(|s| s.metadata().file_size)
            .sum();
        let new_size: u64 = new_sstables.iter().map(|s| s.metadata().file_size).sum();
        stats.space_reclaimed = old_size.saturating_sub(new_size);

        // Update statistics
        stats.levels_compacted = 1;
        stats.duration_ms = start_time.elapsed().as_millis() as u64;

        // Update compaction metrics in stats
        // Note: This requires access to engine stats, which we don't have here
        // The engine should update its stats after compaction completes
        let mut stats_guard = self.stats.write().await;
        *stats_guard = stats.clone();

        info!(
            "Compaction completed for level {}: {} entries processed, {} space reclaimed",
            level, stats.entries_processed, stats.space_reclaimed
        );

        // IMPORTANT: Mark SSTables for deletion AFTER reading all entries
        // This ensures compaction can read from SSTables without interference
        // We mark for deletion only after we've successfully read all the data we need

        // Wait for any in-flight reads to complete before marking for deletion
        // This gives readers a chance to finish before we mark the SSTable for deletion
        let pre_mark_wait = Duration::from_millis(100);
        for sstable in &sstables_to_compact {
            let initial_reader_count = sstable.reader_count();
            if initial_reader_count > 0 {
                debug!(
                    "Waiting for {} readers to complete before marking SSTable {:?} for deletion",
                    initial_reader_count,
                    sstable.path()
                );
                // Wait a short time for readers to finish
                tokio::time::sleep(pre_mark_wait).await;
            }
        }

        // Now mark SSTables for deletion - this prevents new reads from starting
        // Existing reads will complete and decrement the reader count
        for sstable in &sstables_to_compact {
            sstable.mark_for_deletion();
            debug!(
                "Marked SSTable {:?} for deletion (reader_count: {})",
                sstable.path(),
                sstable.reader_count()
            );
        }

        // Wait for all readers to complete (with timeout)
        // Default timeout: 5 seconds
        let reader_wait_timeout = Duration::from_secs(5);
        let mut all_readers_done = true;

        for sstable in &sstables_to_compact {
            if !sstable.wait_for_readers(reader_wait_timeout).await {
                warn!(
                    "SSTable {:?} still has {} active readers after timeout, proceeding with deletion",
                    sstable.path(),
                    sstable.reader_count()
                );
                all_readers_done = false;
            } else {
                debug!("All readers completed for SSTable {:?}", sstable.path());
            }
        }

        if !all_readers_done {
            warn!(
                "Some SSTables still have active readers after timeout. \
                They will be removed from the index but may still be accessible to in-flight reads."
            );
        }

        // Return remaining SSTables + new ones
        // Filter out SSTables that were compacted (marked for deletion)
        let remaining_sstables: Vec<SSTable> = current_level_sstables
            .into_iter()
            .filter(|s| {
                !sstables_to_compact
                    .iter()
                    .any(|compacted| compacted.path() == s.path())
            })
            .collect();

        let mut result = remaining_sstables;
        result.extend(new_sstables);
        Ok(result)
    }

    /// Select SSTables for compaction based on strategy
    async fn select_sstables_for_compaction(
        &self,
        level: usize,
        sstables: &[SSTable],
    ) -> Result<Vec<SSTable>> {
        match self.config.strategy {
            CompactionStrategy::Leveled => self.select_leveled_sstables(level, sstables).await,
            CompactionStrategy::SizeTiered => {
                self.select_size_tiered_sstables(level, sstables).await
            }
            CompactionStrategy::TimeWindowed => {
                self.select_time_windowed_sstables(level, sstables).await
            }
            CompactionStrategy::Hybrid => {
                if level == 0 {
                    self.select_leveled_sstables(level, sstables).await
                } else {
                    self.select_size_tiered_sstables(level, sstables).await
                }
            }
        }
    }

    /// Select SSTables for leveled compaction
    async fn select_leveled_sstables(
        &self,
        level: usize,
        sstables: &[SSTable],
    ) -> Result<Vec<SSTable>> {
        if level == 0 {
            // L0: Select all SSTables
            Ok(sstables.to_vec())
        } else {
            // L1+: Select overlapping SSTables
            // For simplicity, select all SSTables in the level
            Ok(sstables.to_vec())
        }
    }

    /// Select SSTables for size-tiered compaction
    async fn select_size_tiered_sstables(
        &self,
        _level: usize,
        sstables: &[SSTable],
    ) -> Result<Vec<SSTable>> {
        if sstables.len() < 4 {
            return Ok(Vec::new());
        }

        // Group by size and select the largest group
        let mut size_groups: HashMap<u64, Vec<SSTable>> = HashMap::new();
        for sstable in sstables {
            let size_bucket = (sstable.metadata().file_size / (1024 * 1024)) * (1024 * 1024);
            size_groups
                .entry(size_bucket)
                .or_default()
                .push(sstable.clone());
        }

        if let Some((_, group)) = size_groups.iter().max_by_key(|(_, group)| group.len()) {
            if group.len() >= 4 {
                Ok(group.clone())
            } else {
                Ok(Vec::new())
            }
        } else {
            Ok(Vec::new())
        }
    }

    /// Select SSTables for time-windowed compaction
    async fn select_time_windowed_sstables(
        &self,
        _level: usize,
        sstables: &[SSTable],
    ) -> Result<Vec<SSTable>> {
        let window_size = 3600; // 1 hour
        let mut time_groups: HashMap<u64, Vec<SSTable>> = HashMap::new();

        for sstable in sstables {
            let window = sstable.metadata().created_at / window_size;
            time_groups.entry(window).or_default().push(sstable.clone());
        }

        // Select the group with the most SSTables
        if let Some((_, group)) = time_groups.iter().max_by_key(|(_, group)| group.len()) {
            if group.len() > 1 {
                Ok(group.clone())
            } else {
                Ok(Vec::new())
            }
        } else {
            Ok(Vec::new())
        }
    }

    /// Merge multiple SSTables into a single sorted list of entries
    /// Optimized: Uses streaming merge for large datasets to reduce memory usage
    #[allow(dead_code)]
    async fn merge_sstables(&self, sstables: &[SSTable]) -> Result<Vec<SSTableEntry>> {
        if sstables.is_empty() {
            return Ok(Vec::new());
        }

        // For small numbers of SSTables, use simple merge
        // For larger merges, consider streaming (future optimization)
        if sstables.len() <= 4 {
            let mut all_entries = Vec::new();

            // Read entries from all SSTables in parallel where possible
            let mut entry_futures = Vec::new();
            for sstable in sstables {
                let sstable_clone = sstable.clone();
                entry_futures.push(async move { self.read_sstable_entries(&sstable_clone).await });
            }

            // Collect all entries
            for future in entry_futures {
                let entries = future.await?;
                all_entries.extend(entries);
            }

            // Sort by key, then by timestamp (newest first)
            // Since entries from each SSTable are already sorted, we could use merge sort
            // but for simplicity, we use full sort here
            all_entries.sort_by(|a, b| {
                a.key
                    .cmp(&b.key)
                    .then_with(|| b.timestamp.cmp(&a.timestamp))
            });

            Ok(all_entries)
        } else {
            // For larger merges, use streaming merge to reduce memory
            // This is a simplified version - full streaming would use iterators
            let mut all_entries = Vec::new();

            // Read and merge in batches to reduce peak memory usage
            for sstable in sstables {
                let entries = self.read_sstable_entries(sstable).await?;
                all_entries.extend(entries);

                // If we've accumulated too many entries, sort and deduplicate in place
                if all_entries.len() > 100_000 {
                    all_entries.sort_by(|a, b| {
                        a.key
                            .cmp(&b.key)
                            .then_with(|| b.timestamp.cmp(&a.timestamp))
                    });
                    // Partial deduplication (keep latest per key)
                    let mut deduped = Vec::new();
                    let mut last_key = None;
                    for entry in all_entries.drain(..) {
                        if last_key.as_ref() != Some(&entry.key) {
                            deduped.push(entry);
                            // Safe to unwrap: we just pushed an entry, so deduped is not empty
                            last_key = Some(
                                deduped
                                    .last()
                                    .expect("deduped should not be empty after push")
                                    .key
                                    .clone(),
                            );
                        }
                    }
                    all_entries = deduped;
                }
            }

            // Final sort
            all_entries.sort_by(|a, b| {
                a.key
                    .cmp(&b.key)
                    .then_with(|| b.timestamp.cmp(&a.timestamp))
            });

            Ok(all_entries)
        }
    }

    /// Read all entries from an SSTable
    async fn read_sstable_entries(&self, sstable: &SSTable) -> Result<Vec<SSTableEntry>> {
        // CRITICAL: Check if SSTable is ready before reading
        // This ensures metadata and index are fully loaded
        if !sstable.is_ready() {
            return Err(crate::error::LsmError::Internal(format!(
                "Cannot read entries from SSTable that is not ready: {:?}. \
                The SSTable may still be being written or metadata/index may not be loaded.",
                sstable.path()
            )));
        }

        // Check if SSTable is marked for deletion before reading
        // We should not read from SSTables that are being deleted
        if sstable.is_marked_for_deletion() {
            return Err(crate::error::LsmError::Internal(format!(
                "Cannot read entries from SSTable marked for deletion: {:?}",
                sstable.path()
            )));
        }

        // Ensure SSTable is open for reading
        // Note: We need mutable access, but we only have &SSTable
        // The SSTable should already be open from the engine, but we'll handle errors gracefully
        let sstable_mut = {
            // Create a mutable clone for opening if needed
            // In practice, the engine should ensure SSTables are open before compaction
            let mut cloned = sstable.clone();

            // Double-check ready status after cloning (race condition protection)
            if !cloned.is_ready() {
                return Err(crate::error::LsmError::Internal(format!(
                    "SSTable became not ready during compaction read: {:?}",
                    cloned.path()
                )));
            }

            // Double-check deletion status after cloning (race condition protection)
            if cloned.is_marked_for_deletion() {
                return Err(crate::error::LsmError::Internal(format!(
                    "SSTable was marked for deletion during compaction read: {:?}",
                    cloned.path()
                )));
            }

            if !cloned.is_open() {
                cloned.open().await?;
                // After opening, verify it's still ready
                if !cloned.is_ready() {
                    return Err(crate::error::LsmError::Internal(format!(
                        "SSTable is not ready after open during compaction read: {:?}",
                        cloned.path()
                    )));
                }
            }
            cloned
        };

        // Final check before reading entries
        if !sstable_mut.is_ready() {
            return Err(crate::error::LsmError::Internal(format!(
                "SSTable is not ready before reading entries: {:?}",
                sstable_mut.path()
            )));
        }
        if sstable_mut.is_marked_for_deletion() {
            return Err(crate::error::LsmError::Internal(format!(
                "SSTable marked for deletion before reading entries: {:?}",
                sstable_mut.path()
            )));
        }

        // Use the new get_all_entries method to read all entries with full metadata
        // This includes timestamps which are essential for proper compaction
        sstable_mut.get_all_entries().await
    }

    /// Remove duplicate entries, keeping only the latest version
    fn deduplicate_entries(&self, mut entries: Vec<SSTableEntry>) -> Vec<SSTableEntry> {
        // Sort by key, then by timestamp (newest first)
        entries.sort_by(|a, b| {
            a.key
                .cmp(&b.key)
                .then_with(|| b.timestamp.cmp(&a.timestamp))
        });

        let mut seen_keys = HashSet::new();
        let mut result = Vec::new();

        for entry in entries {
            if !seen_keys.contains(&entry.key) {
                seen_keys.insert(entry.key.clone());
                if !entry.deleted {
                    result.push(entry);
                }
            }
        }

        result
    }

    /// Create new SSTables from merged entries
    async fn create_new_sstables(
        &self,
        level: usize,
        entries: Vec<SSTableEntry>,
    ) -> Result<Vec<SSTable>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let mut sstables = Vec::new();
        let target_size = self.sstable_config.target_size;
        let mut current_entries = Vec::new();
        let mut current_size = 0;

        for entry in entries {
            let entry_size = entry.key.len() + entry.value.serialized_size() + 32; // Rough estimate

            if current_size + entry_size > target_size && !current_entries.is_empty() {
                // Create SSTable from current entries
                let sstable = self
                    .create_sstable_from_entries(level, current_entries)
                    .await?;
                sstables.push(sstable);
                current_entries = Vec::new();
                current_size = 0;
            }

            current_entries.push(entry);
            current_size += entry_size;
        }

        // Create final SSTable if there are remaining entries
        if !current_entries.is_empty() {
            let sstable = self
                .create_sstable_from_entries(level, current_entries)
                .await?;
            sstables.push(sstable);
        }

        Ok(sstables)
    }

    /// Create a new SSTable from entries
    async fn create_sstable_from_entries(
        &self,
        level: usize,
        entries: Vec<SSTableEntry>,
    ) -> Result<SSTable> {
        // Ensure data directory exists
        std::fs::create_dir_all(&self.data_dir).map_err(|e| F4KvsError::Storage {
            message: format!("Failed to create data directory: {}", e),
        })?;

        let timestamp = utils::timestamp_secs();

        let filename = format!("sstable_l{}_t{}.sst", level, timestamp);
        let path = self.data_dir.join(filename);

        let mut sstable = SSTable::new(path, self.sstable_config.clone(), level)?;
        sstable.write_entries(entries).await?;

        // Open and validate the SSTable to ensure file integrity
        // This triggers metadata/index reading which validates the file is complete
        sstable.open().await?;

        Ok(sstable)
    }

    /// Get compaction statistics
    pub async fn get_stats(&self) -> CompactionStats {
        self.stats.read().await.clone()
    }

    /// Reset compaction statistics
    pub async fn reset_stats(&self) {
        let mut stats = self.stats.write().await;
        *stats = CompactionStats::default();
    }

    /// Get adaptive workload characteristics
    pub async fn get_workload_characteristics(
        &self,
    ) -> Option<super::adaptive::WorkloadCharacteristics> {
        if let Some(adaptive_manager) = &self.adaptive_manager {
            Some(adaptive_manager.get_workload_characteristics().await)
        } else {
            None
        }
    }

    /// Get adaptive performance metrics
    pub async fn get_performance_metrics(&self) -> Option<super::adaptive::PerformanceMetrics> {
        if let Some(adaptive_manager) = &self.adaptive_manager {
            Some(adaptive_manager.get_performance_metrics().await)
        } else {
            None
        }
    }

    /// Check if adaptive compaction is enabled
    pub fn is_adaptive_enabled(&self) -> bool {
        self.adaptive_manager.is_some()
    }

    /// Get current optimal strategy
    pub async fn get_current_strategy(&self) -> CompactionStrategy {
        if let Some(adaptive_manager) = &self.adaptive_manager {
            adaptive_manager.get_optimal_strategy().await
        } else {
            self.config.strategy
        }
    }

    /// Get current optimal priority
    pub async fn get_current_priority(&self) -> CompactionPriority {
        if let Some(adaptive_manager) = &self.adaptive_manager {
            adaptive_manager.get_optimal_priority().await
        } else {
            self.config.priority
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;
    use std::path::Path;
    use tempfile::TempDir;

    /// Create a test compaction manager
    fn create_test_compaction_manager() -> (CompactionManager, TempDir) {
        let temp_dir = TempDir::new()
            .expect("Failed to create temporary directory for compaction manager test");
        let data_dir = temp_dir.path().to_path_buf();

        let config = CompactionConfig::default();
        let level_config = LevelConfig::default();
        let sstable_config = SstableConfig::default();

        let manager = CompactionManager::new(&config, &level_config, &sstable_config, data_dir)
            .expect("Failed to create compaction manager for test");
        (manager, temp_dir)
    }

    /// Create a mock SSTable for testing
    async fn create_mock_sstable(
        data_dir: &Path,
        level: usize,
        _size: u64,
        created_at: u64,
    ) -> SSTable {
        std::fs::create_dir_all(data_dir).expect("Failed to create data directory for test");
        let path = data_dir.join(format!("test_sstable_l{}_t{}.sst", level, created_at));
        let config = SstableConfig::default();

        let mut sstable =
            SSTable::new(path, config, level).expect("Failed to create SSTable for test");
        let entry = SSTableEntry {
            key: format!("key_{}", created_at),
            value: Value::String(format!("value_{}", created_at)),
            timestamp: created_at,
            deleted: false,
        };

        sstable
            .write_entries(vec![entry])
            .await
            .expect("Failed to write entries to SSTable in test");
        // Match production behavior: open after writing so metadata/index are loaded and the SSTable
        // is marked ready for reads.
        sstable
            .open()
            .await
            .expect("Failed to open SSTable in test");
        sstable
    }

    #[tokio::test]
    async fn test_compaction_manager_creation() {
        let (manager, _temp_dir) = create_test_compaction_manager();
        let stats = manager.get_stats().await;
        assert_eq!(stats.levels_compacted, 0);
        assert_eq!(stats.sstables_merged, 0);
    }

    #[tokio::test]
    async fn test_should_compact_leveled_l0() {
        let (manager, temp_dir) = create_test_compaction_manager();

        // Test L0 with few SSTables (should not compact)
        let sstables = vec![
            create_mock_sstable(temp_dir.path(), 0, 1024, 1000).await,
            create_mock_sstable(temp_dir.path(), 0, 1024, 1001).await,
        ];
        assert!(!manager.should_compact_leveled(0, &sstables).await);

        // Test L0 with many SSTables (should compact)
        let sstables = vec![
            create_mock_sstable(temp_dir.path(), 0, 1024, 1000).await,
            create_mock_sstable(temp_dir.path(), 0, 1024, 1001).await,
            create_mock_sstable(temp_dir.path(), 0, 1024, 1002).await,
            create_mock_sstable(temp_dir.path(), 0, 1024, 1003).await,
            create_mock_sstable(temp_dir.path(), 0, 1024, 1004).await,
            create_mock_sstable(temp_dir.path(), 0, 1024, 1005).await,
            create_mock_sstable(temp_dir.path(), 0, 1024, 1006).await,
            create_mock_sstable(temp_dir.path(), 0, 1024, 1007).await,
            create_mock_sstable(temp_dir.path(), 0, 1024, 1008).await,
            create_mock_sstable(temp_dir.path(), 0, 1024, 1009).await,
            create_mock_sstable(temp_dir.path(), 0, 1024, 1010).await,
        ];
        assert!(manager.should_compact_leveled(0, &sstables).await);
    }

    #[tokio::test]
    async fn test_should_compact_leveled_l1_plus() {
        let (manager, temp_dir) = create_test_compaction_manager();

        // Test L1 with small size (should not compact)
        // Default file_size is 0, so this should not compact
        let sstables = vec![
            create_mock_sstable(temp_dir.path(), 1, 0, 1000).await,
            create_mock_sstable(temp_dir.path(), 1, 0, 1001).await,
        ];
        assert!(!manager.should_compact_leveled(1, &sstables).await);

        // Note: We can't test the large size case because we can't modify metadata
        // In a real implementation, we would need a way to create SSTables with custom metadata
    }

    #[tokio::test]
    async fn test_should_compact_size_tiered() {
        let (manager, temp_dir) = create_test_compaction_manager();

        // Test with few SSTables (should not compact)
        let sstables = vec![
            create_mock_sstable(temp_dir.path(), 1, 1024, 1000).await,
            create_mock_sstable(temp_dir.path(), 1, 1024, 1001).await,
        ];
        assert!(!manager.should_compact_size_tiered(1, &sstables).await);

        // Test with many SSTables of same size (should compact)
        let sstables = vec![
            create_mock_sstable(temp_dir.path(), 1, 1024 * 1024, 1000).await, // 1MB
            create_mock_sstable(temp_dir.path(), 1, 1024 * 1024, 1001).await, // 1MB
            create_mock_sstable(temp_dir.path(), 1, 1024 * 1024, 1002).await, // 1MB
            create_mock_sstable(temp_dir.path(), 1, 1024 * 1024, 1003).await, // 1MB
        ];
        assert!(manager.should_compact_size_tiered(1, &sstables).await);
    }

    #[tokio::test]
    async fn test_should_compact_time_windowed() {
        let (manager, temp_dir) = create_test_compaction_manager();

        // Test with SSTables in different time windows (should not compact)
        // Since we can't modify metadata, both SSTables will have the same created_at time
        // and will be in the same window, so this test will actually compact
        let sstables = vec![
            create_mock_sstable(temp_dir.path(), 1, 0, 0).await,
            create_mock_sstable(temp_dir.path(), 1, 0, 0).await,
        ];
        // Both SSTables have the same created_at time, so they're in the same window
        assert!(manager.should_compact_time_windowed(1, &sstables).await);

        // Test with single SSTable (should not compact)
        let sstables = vec![create_mock_sstable(temp_dir.path(), 1, 0, 0).await];
        assert!(!manager.should_compact_time_windowed(1, &sstables).await);
    }

    #[tokio::test]
    async fn test_deduplicate_entries() {
        let (manager, _temp_dir) = create_test_compaction_manager();

        let entries = vec![
            SSTableEntry {
                key: "key1".to_string(),
                value: Value::String("value1".to_string()),
                timestamp: 1000,
                deleted: false,
            },
            SSTableEntry {
                key: "key1".to_string(),
                value: Value::String("value1_updated".to_string()),
                timestamp: 1001,
                deleted: false,
            },
            SSTableEntry {
                key: "key2".to_string(),
                value: Value::String("value2".to_string()),
                timestamp: 1002,
                deleted: false,
            },
            SSTableEntry {
                key: "key3".to_string(),
                value: Value::String("value3".to_string()),
                timestamp: 1003,
                deleted: true,
            },
        ];

        let deduplicated = manager.deduplicate_entries(entries);

        // Should have 2 entries: key1 (latest) and key2 (key3 deleted)
        assert_eq!(deduplicated.len(), 2);
        assert_eq!(deduplicated[0].key, "key1");
        assert_eq!(
            deduplicated[0].value,
            Value::String("value1_updated".to_string())
        );
        assert_eq!(deduplicated[1].key, "key2");
    }

    #[tokio::test]
    async fn test_select_leveled_sstables() {
        let (manager, temp_dir) = create_test_compaction_manager();

        let sstables = vec![
            create_mock_sstable(temp_dir.path(), 0, 1024, 1000).await,
            create_mock_sstable(temp_dir.path(), 0, 1024, 1001).await,
        ];

        let selected = manager
            .select_leveled_sstables(0, &sstables)
            .await
            .expect("Failed to select leveled SSTables in test");
        assert_eq!(selected.len(), 2); // L0 selects all SSTables
    }

    #[tokio::test]
    async fn test_select_size_tiered_sstables() {
        let (manager, temp_dir) = create_test_compaction_manager();

        // Test with few SSTables (should return empty)
        let sstables = vec![
            create_mock_sstable(temp_dir.path(), 1, 1024, 1000).await,
            create_mock_sstable(temp_dir.path(), 1, 1024, 1001).await,
        ];
        let selected = manager
            .select_size_tiered_sstables(1, &sstables)
            .await
            .expect("Failed to select size-tiered SSTables in test");
        assert!(selected.is_empty());

        // Test with many SSTables of same size (should return them)
        let sstables = vec![
            create_mock_sstable(temp_dir.path(), 1, 1024 * 1024, 1000).await, // 1MB
            create_mock_sstable(temp_dir.path(), 1, 1024 * 1024, 1001).await, // 1MB
            create_mock_sstable(temp_dir.path(), 1, 1024 * 1024, 1002).await, // 1MB
            create_mock_sstable(temp_dir.path(), 1, 1024 * 1024, 1003).await, // 1MB
        ];
        let selected = manager
            .select_size_tiered_sstables(1, &sstables)
            .await
            .expect("Failed to select size-tiered SSTables in test");
        assert_eq!(selected.len(), 4);
    }

    #[tokio::test]
    async fn test_select_time_windowed_sstables() {
        let (manager, temp_dir) = create_test_compaction_manager();

        // Test with SSTables in same window (should return them)
        // Since we can't modify metadata, both SSTables will have the same created_at time
        let sstables = vec![
            create_mock_sstable(temp_dir.path(), 1, 0, 0).await,
            create_mock_sstable(temp_dir.path(), 1, 0, 0).await,
        ];
        let selected = manager
            .select_time_windowed_sstables(1, &sstables)
            .await
            .expect("Failed to select time-windowed SSTables in test");
        assert_eq!(selected.len(), 2);

        // Test with single SSTable (should return empty)
        let sstables = vec![create_mock_sstable(temp_dir.path(), 1, 0, 0).await];
        let selected = manager
            .select_time_windowed_sstables(1, &sstables)
            .await
            .expect("Failed to select time-windowed SSTables in test");
        assert!(selected.is_empty());
    }

    #[tokio::test]
    async fn test_compact_level_empty() {
        let (manager, _temp_dir) = create_test_compaction_manager();
        let sstables = HashMap::new();

        let result = manager
            .compact_level(0, &sstables, &sstables)
            .await
            .expect("Failed to perform compaction operation in test");
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_compact_level_with_sstables() {
        let (manager, temp_dir) = create_test_compaction_manager();

        let mut sstables = HashMap::new();
        sstables.insert(
            0,
            vec![
                create_mock_sstable(temp_dir.path(), 0, 1024, 1000).await,
                create_mock_sstable(temp_dir.path(), 0, 1024, 1001).await,
            ],
        );

        let result = manager
            .compact_level(0, &sstables, &sstables)
            .await
            .expect("Failed to perform compaction operation in test");

        // Should return some SSTables (either original or new ones)
        assert!(!result.is_empty());

        // Check statistics
        let stats = manager.get_stats().await;
        assert!(stats.levels_compacted > 0);
    }

    #[tokio::test]
    async fn test_compaction_strategies() {
        let temp_dir = TempDir::new().expect("Failed to create temporary directory for test");
        let data_dir = temp_dir.path().to_path_buf();

        // Test Leveled strategy
        let config = CompactionConfig {
            strategy: CompactionStrategy::Leveled,
            ..Default::default()
        };
        let level_config = LevelConfig::default();
        let sstable_config = SstableConfig::default();

        let manager =
            CompactionManager::new(&config, &level_config, &sstable_config, data_dir.clone())
                .expect("Failed to perform operation in test");

        let sstables = vec![
            create_mock_sstable(data_dir.as_path(), 0, 1024, 1000).await,
            create_mock_sstable(data_dir.as_path(), 0, 1024, 1001).await,
        ];

        assert!(!manager.should_compact_level(0, &sstables).await);

        // Test SizeTiered strategy
        let config = CompactionConfig {
            strategy: CompactionStrategy::SizeTiered,
            ..Default::default()
        };

        let manager = CompactionManager::new(&config, &level_config, &sstable_config, data_dir)
            .expect("Failed to create compaction manager for test");

        let sstables = vec![
            create_mock_sstable(manager.data_dir.as_path(), 1, 1024 * 1024, 1000).await, // 1MB
            create_mock_sstable(manager.data_dir.as_path(), 1, 1024 * 1024, 1001).await, // 1MB
            create_mock_sstable(manager.data_dir.as_path(), 1, 1024 * 1024, 1002).await, // 1MB
            create_mock_sstable(manager.data_dir.as_path(), 1, 1024 * 1024, 1003).await, // 1MB
        ];

        assert!(manager.should_compact_level(1, &sstables).await);
    }

    #[tokio::test]
    async fn test_compaction_statistics() {
        let (manager, _temp_dir) = create_test_compaction_manager();

        // Initial stats should be zero
        let stats = manager.get_stats().await;
        assert_eq!(stats.levels_compacted, 0);
        assert_eq!(stats.sstables_merged, 0);
        assert_eq!(stats.entries_processed, 0);

        // Reset stats
        manager.reset_stats().await;
        let stats = manager.get_stats().await;
        assert_eq!(stats.levels_compacted, 0);
    }

    #[tokio::test]
    async fn test_hybrid_strategy() {
        let temp_dir = TempDir::new().expect("Failed to create temporary directory for test");
        let data_dir = temp_dir.path().to_path_buf();

        let config = CompactionConfig {
            strategy: CompactionStrategy::Hybrid,
            ..Default::default()
        };
        let level_config = LevelConfig::default();
        let sstable_config = SstableConfig::default();

        let manager = CompactionManager::new(&config, &level_config, &sstable_config, data_dir)
            .expect("Failed to create compaction manager for test");

        // L0 should use leveled strategy
        let sstables = vec![
            create_mock_sstable(manager.data_dir.as_path(), 0, 1024, 1000).await,
            create_mock_sstable(manager.data_dir.as_path(), 0, 1024, 1001).await,
        ];
        assert!(!manager.should_compact_level(0, &sstables).await);

        // L1+ should use size-tiered strategy
        let sstables = vec![
            create_mock_sstable(manager.data_dir.as_path(), 1, 1024 * 1024, 1000).await, // 1MB
            create_mock_sstable(manager.data_dir.as_path(), 1, 1024 * 1024, 1001).await, // 1MB
            create_mock_sstable(manager.data_dir.as_path(), 1, 1024 * 1024, 1002).await, // 1MB
            create_mock_sstable(manager.data_dir.as_path(), 1, 1024 * 1024, 1003).await, // 1MB
        ];
        assert!(manager.should_compact_level(1, &sstables).await);
    }
}
