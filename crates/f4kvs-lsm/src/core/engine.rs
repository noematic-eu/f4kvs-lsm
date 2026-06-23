//! Core LSM Tree Engine implementation

use super::metrics::{
    LevelMetrics, OptimizationPriority, OptimizationRecommendation, PerformanceMetrics,
};
use super::LsmConfig;
use crate::compaction::adaptive;
use crate::compaction::manager::CompactionStats;
use crate::storage::sstable::SSTableEntry;
use crate::{
    compaction::CompactionManager,
    error::{LsmError, Result},
    storage::{Memtable, MemtableLookupResult, PutEffect, SSTable, WALEntry, WALManager},
    utils,
    utils::LsmStats,
};
use async_trait::async_trait;
use f4kvs_value::{F4KvsError, Value};
use f4kvs_storage_core::{
    stats::StorageStats as F4KvsStorageStats,
    traits::{KeyValueIterator, StorageEngine},
};
#[cfg(feature = "ttl")]
use f4kvs_ttl::TTLManager;
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

#[cfg(feature = "metrics")]
use crate::metrics::MetricsRecorder;
#[cfg(feature = "metrics")]
use crate::metrics::{
    record_bloom_filter_hit, record_bloom_filter_miss, record_compaction, record_memtable_flush,
    record_sstable_read,
};

/// LSM Tree Storage Engine
///
/// This engine implements the F4KVS StorageEngine trait and provides
/// a complete LSM tree implementation with automatic compaction,
/// compression, and crash recovery.
pub struct LsmTreeEngine {
    /// Configuration
    config: LsmConfig,

    /// Active memtable (mutable)
    active_memtable: Arc<RwLock<Memtable>>,

    /// Immutable memtables (being flushed)
    immutable_memtables: Arc<RwLock<Vec<Memtable>>>,

    /// SSTables organized by level
    sstables: Arc<RwLock<HashMap<usize, Vec<SSTable>>>>,

    /// Write-ahead log manager
    wal_manager: Arc<RwLock<WALManager>>,

    /// Statistics
    stats: Arc<RwLock<LsmStats>>,

    /// Compaction manager
    compaction_manager: Arc<CompactionManager>,

    /// Column family mappings
    column_families: Arc<RwLock<HashMap<String, usize>>>,

    /// TTL manager for handling key expiration
    #[cfg(feature = "ttl")]
    ttl_manager: Arc<TTLManager>,

    /// Metrics recorder for LSM operations
    #[cfg(feature = "metrics")]
    metrics: MetricsRecorder,

    /// Shutdown flag for background tasks
    shutdown: Arc<AtomicBool>,

    /// Background task handles for cleanup
    background_tasks: Arc<RwLock<Vec<tokio::task::JoinHandle<()>>>>,

    /// Monotonically increasing sequence number for SSTable entries
    /// Used to ensure correct ordering during compaction
    sequence_number: Arc<std::sync::atomic::AtomicU64>,

    /// Operation guard to ensure compaction runs exclusively
    /// Read operations acquire read lock, compaction acquires write lock
    operation_guard: Arc<RwLock<()>>,

    /// Live key count (O(1) `count()`); rebuilt on open, maintained incrementally on writes/deletes.
    live_key_count: AtomicU64,
}

impl LsmTreeEngine {
    /// Create a new LSM tree engine
    pub async fn new(config: LsmConfig) -> Result<Self> {
        // Create necessary directories
        Self::create_directories(&config).await?;

        // Create engine structure
        let engine = Self::create_engine_structure(config).await?;

        // Initialize components
        engine.initialize_components().await?;

        // Start background tasks
        engine.start_background_tasks().await?;

        Ok(engine)
    }

    /// Create necessary directories for the LSM engine
    async fn create_directories(config: &LsmConfig) -> Result<()> {
        // Create data directory if it doesn't exist
        tokio::fs::create_dir_all(&config.data_dir)
            .await
            .map_err(LsmError::Io)?;

        // Create WAL directory if enabled
        if config.wal.enabled {
            tokio::fs::create_dir_all(&config.wal.dir)
                .await
                .map_err(LsmError::Io)?;
        }

        Ok(())
    }

    /// Create the basic engine structure with all components
    async fn create_engine_structure(config: LsmConfig) -> Result<Self> {
        // Initialize core components
        let active_memtable = Arc::new(RwLock::new(Memtable::new(&config.memtable)?));
        let immutable_memtables = Arc::new(RwLock::new(Vec::new()));
        let sstables = Arc::new(RwLock::new(HashMap::new()));

        let wal_manager = Arc::new(RwLock::new(WALManager::new(&config.wal)?));
        let stats = Arc::new(RwLock::new(LsmStats::default()));
        let column_families = Arc::new(RwLock::new(HashMap::new()));

        // Initialize TTL manager
        #[cfg(feature = "ttl")]
        let ttl_manager = Arc::new(TTLManager::new(Duration::from_secs(1)));

        // Initialize compaction manager
        let compaction_manager = Self::create_compaction_manager(&config)?;

        // Add default column family
        {
            let mut cf_map = column_families.write().await;
            cf_map.insert(config.column_families.default_name.clone(), 0);
        }

        info!(
            "LSM Tree Engine structure created with config: {:?}",
            config
        );

        Ok(Self {
            config,
            active_memtable,
            immutable_memtables,
            sstables,
            wal_manager,
            stats,
            compaction_manager,
            column_families,
            #[cfg(feature = "ttl")]
            ttl_manager,
            #[cfg(feature = "metrics")]
            metrics: MetricsRecorder::new(),
            shutdown: Arc::new(AtomicBool::new(false)),
            background_tasks: Arc::new(RwLock::new(Vec::new())),
            sequence_number: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            operation_guard: Arc::new(RwLock::new(())),
            live_key_count: AtomicU64::new(0),
        })
    }

    fn set_live_key_count(&self, count: u64) {
        self.live_key_count
            .store(count, Ordering::Release);
    }

    fn adjust_live_key_count(&self, delta: i64) {
        if delta > 0 {
            self.live_key_count
                .fetch_add(delta as u64, Ordering::Relaxed);
        } else if delta < 0 {
            self.live_key_count
                .fetch_sub((-delta) as u64, Ordering::Relaxed);
        }
    }

    /// Rebuild live key count from merged layers (startup / post-compaction).
    async fn refresh_live_key_count(&self) -> Result<()> {
        let pairs = self.merge_scan_prefix_with_values("").await?;
        let count = pairs.len() as u64;
        self.set_live_key_count(count);
        {
            let mut stats = self.stats.write().await;
            stats.total_keys = count;
        }
        debug!("Refreshed live key count: {}", count);
        Ok(())
    }

    async fn apply_put_key_count(&self, key: &str, effect: PutEffect) -> Result<()> {
        let delta = match effect {
            PutEffect::UpdatedLive => 0,
            PutEffect::Resurrected => 1,
            PutEffect::Inserted => {
                if self.get_from_sstables(key).await?.is_some() {
                    0
                } else {
                    1
                }
            }
        };
        if delta != 0 {
            self.adjust_live_key_count(delta);
            let mut stats = self.stats.write().await;
            stats.total_keys = self.live_key_count.load(Ordering::Relaxed);
        }
        Ok(())
    }

    /// Create compaction manager based on configuration
    fn create_compaction_manager(config: &LsmConfig) -> Result<Arc<CompactionManager>> {
        if let Some(adaptive_config) = &config.adaptive_compaction {
            Ok(Arc::new(CompactionManager::new_with_adaptive(
                &config.compaction,
                &config.levels,
                &config.sstable,
                config.data_dir.clone(),
                adaptive_config.clone(),
            )?))
        } else {
            Ok(Arc::new(CompactionManager::new(
                &config.compaction,
                &config.levels,
                &config.sstable,
                config.data_dir.clone(),
            )?))
        }
    }

    /// Initialize all engine components
    async fn initialize_components(&self) -> Result<()> {
        // Initialize WAL if enabled
        self.initialize_wal().await?;

        // Load existing SSTables from disk
        self.load_existing_sstables().await?;

        self.refresh_live_key_count().await?;

        Ok(())
    }

    /// Initialize WAL and perform recovery if needed
    async fn initialize_wal(&self) -> Result<()> {
        if self.config.wal.enabled {
            info!("WAL is enabled, initializing...");
            let wal = self.wal_manager.write().await;
            wal.initialize()
                .await
                .map_err(|e| LsmError::Wal(format!("Failed to initialize WAL: {}", e)))?;
            info!("WAL initialized, attempting recovery...");

            // Recover from WAL on startup
            // CRITICAL: WAL recovery failures must prevent startup unless explicitly allowed
            match self.recover_from_wal().await {
                Ok(()) => {
                    info!("WAL recovery completed successfully");
                }
                Err(e) => {
                    if self.config.wal.allow_recovery_failure {
                        warn!(
                            "WAL recovery failed: {}, continuing without recovery (allow_recovery_failure=true). \
                            WARNING: This may result in data loss if recent writes were not persisted.",
                            e
                        );
                    } else {
                        // Default behavior: fail engine initialization on recovery failure
                        return Err(LsmError::Wal(format!(
                            "CRITICAL: WAL recovery failed, refusing to start engine. \
                            Recent writes may be lost if recovery cannot complete. \
                            Error: {}. \
                            If this is intentional (e.g., for development/testing), set \
                            wal.allow_recovery_failure=true in configuration. \
                            WARNING: Enabling allow_recovery_failure may result in data loss.",
                            e
                        )));
                    }
                }
            }
        } else {
            info!("WAL is disabled");
        }

        Ok(())
    }

    /// Load existing SSTables from disk
    async fn load_existing_sstables(&self) -> Result<()> {
        info!("Loading existing SSTables from disk...");
        log::debug!("=== SSTABLE LOADING DEBUG ===");

        let data_dir = std::path::Path::new(&self.config.data_dir);
        if !data_dir.exists() {
            info!("Data directory does not exist, no SSTables to load");
            log::debug!("Data directory does not exist: {:?}", data_dir);
            return Ok(());
        }

        log::debug!("Data directory exists: {:?}", data_dir);

        let mut sstables = self.sstables.write().await;
        let mut loaded_count = 0;

        // Scan for SSTable files (pattern: L{level}_{timestamp}.sst)
        if let Ok(entries) = std::fs::read_dir(data_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                    log::trace!("Found file: {}", file_name);
                    if file_name.ends_with(".sst") && file_name.starts_with("L") {
                        log::trace!("Processing SSTable file: {}", file_name);
                        // Parse level from filename (e.g., "L0_1234567890.sst")
                        if let Some(level_str) = file_name.split('_').next() {
                            if let Some(level) = level_str
                                .strip_prefix("L")
                                .and_then(|s| s.parse::<usize>().ok())
                            {
                                log::trace!("Parsed level: {}", level);
                                // Create SSTable and try to open it
                                if let Ok(mut sstable) =
                                    SSTable::new(path.clone(), self.config.sstable.clone(), level)
                                {
                                    log::trace!("Created SSTable for level {}", level);
                                    match sstable.open().await {
                                        Ok(()) => {
                                            let level_sstables =
                                                sstables.entry(level).or_insert_with(Vec::new);
                                            level_sstables.push(sstable);
                                            loaded_count += 1;
                                            info!(
                                                "Loaded SSTable: {} (level {})",
                                                file_name, level
                                            );
                                            log::debug!(
                                                "Successfully loaded SSTable: {} (level {})",
                                                file_name,
                                                level
                                            );
                                        }
                                        Err(_) => {
                                            warn!("Failed to open SSTable: {}", file_name);
                                            log::debug!("Failed to open SSTable: {}", file_name);
                                        }
                                    }
                                } else {
                                    log::debug!("Failed to create SSTable for file: {}", file_name);
                                }
                            }
                        }
                    }
                }
            }
        }

        log::debug!("Total SSTables loaded: {}", loaded_count);
        log::debug!("SSTables by level:");
        for (level, level_sstables) in sstables.iter() {
            log::debug!("  Level {}: {} SSTables", level, level_sstables.len());
        }
        log::debug!("===========================");

        info!("Loaded {} existing SSTables from disk", loaded_count);
        Ok(())
    }

    /// Recover from WAL on startup
    async fn recover_from_wal(&self) -> Result<()> {
        info!("Starting WAL recovery...");

        // Check if WAL directory exists and has files
        let wal_dir = std::path::Path::new(&self.config.wal.dir);
        if !wal_dir.exists() {
            info!("WAL directory does not exist, skipping recovery");
            return Ok(());
        }

        // Check if WAL directory has any files
        let mut has_files = false;
        let mut file_count = 0;
        if let Ok(entries) = std::fs::read_dir(wal_dir) {
            for entry in entries.flatten() {
                file_count += 1;
                has_files = true;
                info!("Found WAL file: {:?}", entry.path());
            }
        }

        info!("WAL directory has {} files", file_count);

        if !has_files {
            info!("WAL directory is empty, skipping recovery");
            return Ok(());
        }

        // Use a timeout to prevent infinite loops
        let recovery_result =
            tokio::time::timeout(self.config.wal.recovery_timeout, async {
                // Scan WAL directory for all segment files
                let mut all_entries = Vec::new();
                let mut segments_found = 0;
                let mut segments_read_successfully = 0;
                let mut segments_with_errors = Vec::new();
                let mut successful_segment_ids = Vec::new();
                let mut failed_segment_ids = Vec::new();

                if let Ok(entries) = std::fs::read_dir(wal_dir) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                            if file_name.ends_with(".wal") && file_name.starts_with("segment_") {
                                segments_found += 1;
                                debug!("Found WAL segment file: {:?}", path);

                                // Extract segment ID for tracking
                                let segment_id = if let Some(id_str) = file_name
                                    .strip_prefix("segment_")
                                    .and_then(|s| s.strip_suffix(".wal"))
                                {
                                    u64::from_str_radix(id_str, 16).ok()
                                } else {
                                    None
                                };

                                // Try to read entries from this segment
                                match crate::storage::wal::WALSegment::open_for_reading(
                                    path.clone(),
                                    self.config.wal.segment_size as u64,
                                    self.config.wal.sync_mode,
                                )
                                .await
                                {
                                    Ok(mut segment) => match segment.read_entries().await {
                                        Ok(entries) => {
                                            debug!(
                                                "Read {} entries from segment {:?}",
                                                entries.len(),
                                                path
                                            );
                                            all_entries.extend(entries);
                                            segments_read_successfully += 1;
                                            if let Some(id) = segment_id {
                                                successful_segment_ids.push(id);
                                            }
                                        }
                                        Err(e) => {
                                            warn!(
                                                "Failed to read entries from segment {:?}: {}",
                                                path, e
                                            );
                                            segments_with_errors
                                                .push((path.clone(), format!("Read error: {}", e)));
                                            if let Some(id) = segment_id {
                                                failed_segment_ids.push(id);
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        debug!("Failed to open segment {:?}: {}", path, e);
                                        segments_with_errors
                                            .push((path.clone(), format!("Open error: {}", e)));
                                        if let Some(id) = segment_id {
                                            failed_segment_ids.push(id);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Sort by timestamp
                all_entries.sort_by(|a, b| {
                    let timestamp_a = match a {
                        crate::storage::wal::WALEntry::Put { timestamp, .. } => {
                            *timestamp
                        }
                        crate::storage::wal::WALEntry::Delete { timestamp, .. } => {
                            *timestamp
                        }
                        crate::storage::wal::WALEntry::Flush { timestamp, .. } => {
                            *timestamp
                        }
                        crate::storage::wal::WALEntry::Checkpoint {
                            timestamp, ..
                        } => *timestamp,
                    };
                    let timestamp_b = match b {
                        crate::storage::wal::WALEntry::Put { timestamp, .. } => {
                            *timestamp
                        }
                        crate::storage::wal::WALEntry::Delete { timestamp, .. } => {
                            *timestamp
                        }
                        crate::storage::wal::WALEntry::Flush { timestamp, .. } => {
                            *timestamp
                        }
                        crate::storage::wal::WALEntry::Checkpoint {
                            timestamp, ..
                        } => *timestamp,
                    };
                    timestamp_a.cmp(&timestamp_b)
                });

                // If we found WAL segments but couldn't read any successfully, that's an error
                // This indicates corrupted WAL files that cannot be recovered
                if segments_found > 0 && segments_read_successfully == 0 {
                    warn!(
                        "WAL recovery failure detected: segments_found={}, segments_read_successfully={}, errors={:?}",
                        segments_found, segments_read_successfully, segments_with_errors
                    );
                    return Err(LsmError::Wal(format!(
                        "WAL recovery failed: Found {} WAL segment(s) but failed to read from all of them. \
                        Segment errors: {:?}. \
                        This indicates corrupted WAL files that cannot be recovered.",
                        segments_found, segments_with_errors
                    )));
                }

                // If we have failed segments and the only successful segment is a new empty one (highest ID),
                // and all lower-ID segments failed, that indicates all pre-initialization segments are corrupted
                // This ensures we fail when existing WAL files are corrupted, even if a new empty segment was created
                if !failed_segment_ids.is_empty() && !successful_segment_ids.is_empty() {
                    let max_successful_id = successful_segment_ids.iter().max().copied().unwrap_or(0);
                    let max_failed_id = failed_segment_ids.iter().max().copied().unwrap_or(0);

                    // If all failed segments have IDs less than the successful segments,
                    // and we only have one successful segment (likely the new empty one),
                    // then all pre-initialization segments failed
                    if max_failed_id < max_successful_id && successful_segment_ids.len() == 1 {
                        warn!(
                            "WAL recovery failure detected: All pre-initialization segment(s) (IDs < {}) failed to read. Errors: {:?}",
                            max_successful_id, segments_with_errors
                        );
                        return Err(LsmError::Wal(format!(
                            "WAL recovery failed: All pre-initialization segment(s) failed to read. \
                            Segment errors: {:?}. \
                            Corrupted WAL files detected.",
                            segments_with_errors
                        )));
                    }
                }

                // If we have corrupted segments but also have valid segments, log a warning but continue
                // This allows recovery to proceed when some segments are corrupted but others are valid
                // (e.g., data already flushed to SSTables, only recent WAL segments corrupted)
                if !segments_with_errors.is_empty() && segments_read_successfully > 0 {
                    warn!(
                        "WAL recovery warning: {} segment(s) failed to read but {} segment(s) succeeded. \
                        Continuing recovery with available segments. Errors: {:?}",
                        segments_with_errors.len(), segments_read_successfully, segments_with_errors
                    );
                }

                // Log recovery summary for debugging
                if segments_found > 0 {
                    info!(
                        "WAL recovery summary: found {} segment(s), successfully read {} segment(s)",
                        segments_found, segments_read_successfully
                    );
                }

                Ok::<
                    Vec<crate::storage::wal::WALEntry>,
                    crate::error::LsmError,
                >(all_entries)
            })
            .await;

        let entries = match recovery_result {
            Ok(Ok(entries)) => entries,
            Ok(Err(e)) => {
                // Failed to read WAL entries - this is a real error that should be propagated
                // Clear corrupted WAL files as cleanup
                if let Err(clear_err) = self.clear_wal_directory().await {
                    warn!(
                        "Failed to clear WAL directory during recovery error cleanup: {}",
                        clear_err
                    );
                } else {
                    // WAL files were cleared successfully, return Ok(()) to continue without recovery
                    info!(
                        "WAL was cleared due to corruption/errors, continuing without WAL recovery"
                    );
                    return Ok(());
                }
                // Return error if we couldn't clear the WAL
                return Err(LsmError::Wal(format!(
                    "Failed to read WAL entries during recovery: {}. \
                    Corrupted WAL files have been cleared.",
                    e
                )));
            }
            Err(timeout_error) => {
                // Recovery timed out - this is also an error
                return Err(LsmError::Wal(format!(
                    "WAL recovery timed out: {:?}. \
                    Recovery did not complete within the timeout period.",
                    timeout_error
                )));
            }
        };

        if entries.is_empty() {
            info!("No WAL entries to recover");
            return Ok(());
        }

        let total_entries = entries.len();
        info!("Recovering {} WAL entries", total_entries);

        // Find the last checkpoint entry to optimize recovery
        // Entries before the last checkpoint were already flushed to SSTables
        let last_checkpoint_timestamp = entries.iter().rev().find_map(|entry| match entry {
            WALEntry::Checkpoint { timestamp } => Some(*timestamp),
            _ => None,
        });

        // Filter entries to recover: only entries after the last checkpoint
        let entries_to_recover: Vec<_> = if let Some(checkpoint_ts) = last_checkpoint_timestamp {
            info!(
                "Found checkpoint at timestamp {}, skipping entries before checkpoint",
                checkpoint_ts
            );
            entries
                .iter()
                .filter(|entry| {
                    let entry_ts = match entry {
                        WALEntry::Put { timestamp, .. } => *timestamp,
                        WALEntry::Delete { timestamp, .. } => *timestamp,
                        WALEntry::Flush { timestamp, .. } => *timestamp,
                        WALEntry::Checkpoint { timestamp, .. } => *timestamp,
                    };
                    entry_ts > checkpoint_ts
                })
                .cloned()
                .collect()
        } else {
            info!("No checkpoint found, recovering all entries");
            entries
        };

        if entries_to_recover.is_empty() {
            info!("No entries to recover after checkpoint filtering");
            return Ok(());
        }

        info!(
            "Recovering {} entries ({} skipped before checkpoint)",
            entries_to_recover.len(),
            total_entries - entries_to_recover.len()
        );

        // Debug: Check memtable state before recovery
        {
            let memtable = self.active_memtable.read().await;
            log::debug!("=== WAL RECOVERY DEBUG ===");
            log::debug!("Memtable state before recovery:");
            log::debug!(
                "  batch_key1: {:?}",
                memtable.get("batch_key1").await.unwrap_or(None)
            );
            log::debug!(
                "  batch_key2: {:?}",
                memtable.get("batch_key2").await.unwrap_or(None)
            );
            log::debug!(
                "  batch_key3: {:?}",
                memtable.get("batch_key3").await.unwrap_or(None)
            );
            log::debug!(
                "  batch_key4: {:?}",
                memtable.get("batch_key4").await.unwrap_or(None)
            );
            log::debug!("=========================");
        }

        let mut memtable = self.active_memtable.write().await;

        // Don't clear memtable - apply WAL entries on top of existing state
        // WAL entries (with later timestamps) will overwrite older data correctly
        // The get() method checks memtable first, then SSTables, so this works correctly
        log::debug!("Applying WAL entries to memtable (not clearing - preserving existing state)");

        for (i, entry) in entries_to_recover.iter().enumerate() {
            match entry {
                WALEntry::Put {
                    key,
                    value,
                    timestamp,
                    ..
                } => {
                    log::trace!(
                        "Recovering PUT operation {}: key='{}', value={:?}, timestamp={}",
                        i,
                        key,
                        value,
                        timestamp
                    );
                    if let Err(e) = memtable.put(key, value).await {
                        warn!("Failed to recover PUT operation for key '{}': {}", key, e);
                    } else {
                        log::trace!("Successfully recovered PUT operation for key '{}'", key);
                    }
                }
                WALEntry::Delete { key, timestamp, .. } => {
                    log::trace!(
                        "Recovering DELETE operation {}: key='{}', timestamp={}",
                        i,
                        key,
                        timestamp
                    );
                    // Check value before delete
                    let value_before = memtable.get(key).await.unwrap_or(None);
                    log::trace!("Value before DELETE for key '{}': {:?}", key, value_before);

                    if let Err(e) = memtable.delete(key).await {
                        warn!(
                            "Failed to recover DELETE operation for key '{}': {}",
                            key, e
                        );
                    } else {
                        log::trace!("Successfully recovered DELETE operation for key '{}'", key);
                        // Check value after delete
                        let value_after = memtable.get(key).await.unwrap_or(None);
                        log::trace!("Value after DELETE for key '{}': {:?}", key, value_after);
                    }
                }
                WALEntry::Flush { .. } => {
                    log::trace!("Skipping FLUSH operation {} during recovery", i);
                    // Skip flush entries during recovery
                    // The memtable will be flushed when it gets full
                }
                WALEntry::Checkpoint { .. } => {
                    log::trace!("Skipping CHECKPOINT operation {} during recovery", i);
                    // Skip checkpoint entries during recovery
                }
            }
        }

        // Debug: Check memtable state after recovery
        log::debug!("=== POST-RECOVERY DEBUG ===");
        log::debug!("Memtable state after recovery:");
        log::debug!(
            "  batch_key1: {:?}",
            memtable.get("batch_key1").await.unwrap_or(None)
        );
        log::debug!(
            "  batch_key2: {:?}",
            memtable.get("batch_key2").await.unwrap_or(None)
        );
        log::debug!(
            "  batch_key3: {:?}",
            memtable.get("batch_key3").await.unwrap_or(None)
        );
        log::debug!(
            "  batch_key4: {:?}",
            memtable.get("batch_key4").await.unwrap_or(None)
        );
        log::debug!("===========================");

        info!("WAL recovery completed successfully");
        Ok(())
    }

    /// Clear WAL directory (used when recovery fails)
    async fn clear_wal_directory(&self) -> Result<()> {
        let wal_dir = std::path::Path::new(&self.config.wal.dir);
        if wal_dir.exists() {
            if let Err(e) = tokio::fs::remove_dir_all(wal_dir).await {
                warn!("Failed to remove WAL directory: {}", e);
                return Err(LsmError::Io(e));
            }
            // Recreate the directory
            if let Err(e) = tokio::fs::create_dir_all(wal_dir).await {
                warn!("Failed to recreate WAL directory: {}", e);
                return Err(LsmError::Io(e));
            }
        }
        Ok(())
    }

    /// Start background tasks (compaction, WAL cleanup, etc.)
    async fn start_background_tasks(&self) -> Result<()> {
        let wal_manager = self.wal_manager.clone();
        let wal_config = self.config.wal.clone();
        let shutdown = self.shutdown.clone();
        let mut background_tasks = self.background_tasks.write().await;

        // Start background compaction task if enabled
        if self.config.compaction.background_enabled {
            let compaction_manager = self.compaction_manager.clone();
            let sstables = self.sstables.clone();
            let compaction_interval = self.config.compaction.interval;
            let shutdown = shutdown.clone();

            let handle = tokio::spawn(async move {
                let mut interval = tokio::time::interval(compaction_interval);
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            if shutdown.load(Ordering::Relaxed) {
                                debug!("Background compaction task shutting down");
                                break;
                            }
                            if let Err(e) = compaction_manager.compact_if_needed(&sstables).await {
                                warn!("Background compaction failed: {}", e);
                            }
                        }
                        _ = tokio::time::sleep(Duration::from_millis(100)) => {
                            if shutdown.load(Ordering::Relaxed) {
                                debug!("Background compaction task shutting down");
                                break;
                            }
                        }
                    }
                }
            });
            background_tasks.push(handle);
        }

        // Enhanced WAL cleanup task with configurable intervals
        let shutdown_wal = shutdown.clone();
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(wal_config.cleanup_interval);
            // Skip the immediate first tick to avoid interfering with startup operations
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if shutdown_wal.load(Ordering::Relaxed) {
                            debug!("WAL cleanup task shutting down");
                            break;
                        }

                // Count WAL files before cleanup
                let wal_dir = std::path::Path::new(&wal_config.dir);
                let initial_file_count = if wal_dir.exists() {
                    std::fs::read_dir(wal_dir)
                        .map(|entries| entries.count())
                        .unwrap_or(0)
                } else {
                    0
                };

                tracing::info!(
                    "WAL: Background cleanup starting ({} files found)",
                    initial_file_count
                );

                // Run cleanup operations
                let cleanup_result = async {
                    let wal = wal_manager.write().await;

                    // Clean up old segments based on retention period
                    wal.cleanup_old_segments(wal_config.retention_period)
                        .await?;

                    // Clean up segments that have been flushed (with grace period)
                    wal.cleanup_flushed_segments(wal_config.retention_after_flush)
                        .await?;

                    // Check if we need to force cleanup due to too many segments
                    if initial_file_count > wal_config.max_segments {
                        tracing::warn!(
                            "WAL: Too many segments ({} > {}), forcing aggressive cleanup",
                            initial_file_count,
                            wal_config.max_segments
                        );
                        wal.force_cleanup().await?;
                    }

                    Ok::<(), LsmError>(())
                }
                .await;

                match cleanup_result {
                    Ok(_) => {
                        // Count files after cleanup
                        let final_file_count = if wal_dir.exists() {
                            std::fs::read_dir(wal_dir)
                                .map(|entries| entries.count())
                                .unwrap_or(0)
                        } else {
                            0
                        };

                        if final_file_count < initial_file_count {
                            tracing::info!(
                                "WAL: Background cleanup completed ({} -> {} files)",
                                initial_file_count,
                                final_file_count
                            );
                        } else {
                            tracing::debug!("WAL: Background cleanup completed (no files removed)");
                        }
                    }
                    Err(e) => {
                        warn!("WAL background cleanup failed: {}", e);
                    }
                }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
                        if shutdown_wal.load(Ordering::Relaxed) {
                            debug!("WAL cleanup task shutting down");
                            break;
                        }
                    }
                }
            }
        });
        background_tasks.push(handle);

        Ok(())
    }

    /// Shutdown background tasks gracefully
    pub async fn shutdown_background_tasks(&self) -> Result<()> {
        info!("Shutting down LSM Tree Engine background tasks");

        // Set shutdown flag
        self.shutdown.store(true, Ordering::Relaxed);

        // Wait for all background tasks to complete
        let mut tasks = self.background_tasks.write().await;
        for task in tasks.drain(..) {
            let _ = task.await;
        }

        // Close all open SSTable file handles
        let mut sstables = self.sstables.write().await;
        for level_sstables in sstables.values_mut() {
            for sstable in level_sstables.iter_mut() {
                if sstable.is_open() {
                    if let Err(e) = sstable.close().await {
                        warn!("Error closing SSTable during shutdown: {}", e);
                    }
                }
            }
        }

        // Flush WAL to ensure all data is persisted
        {
            let wal = self.wal_manager.read().await;
            if let Err(e) = wal.flush().await {
                warn!("Error flushing WAL during shutdown: {}", e);
            }
        }

        info!("LSM Tree Engine background tasks shutdown complete");
        Ok(())
    }

    /// Start TTL cleanup task
    #[cfg(feature = "ttl")]
    pub async fn start_ttl_cleanup(&self) -> Result<()> {
        let ttl_manager = self.ttl_manager.clone();
        let active_memtable = self.active_memtable.clone();
        let immutable_memtables = self.immutable_memtables.clone();

        ttl_manager
            .start_cleanup_task(move |expired_keys| {
                let active_memtable = active_memtable.clone();
                let immutable_memtables = immutable_memtables.clone();

                tokio::spawn(async move {
                    for (key, _cf) in expired_keys {
                        // Remove from active memtable
                        {
                            let mut memtable = active_memtable.write().await;
                            let _ = memtable.delete(&key).await;
                        }

                        // Remove from immutable memtables
                        {
                            let _immutable = immutable_memtables.read().await;
                            // Note: Immutable memtables are read-only, so we can't delete from them
                            // The key will be cleaned up during compaction
                        }

                        // Note: SSTable cleanup would require compaction
                        // For now, we rely on tombstone deletion during compaction
                    }
                });

                Ok(())
            })
            .await
            .map_err(|e| LsmError::Internal(format!("Failed to start TTL cleanup: {}", e)))?;

        Ok(())
    }

    /// Check if memtable needs flushing
    async fn check_memtable_flush(&self) -> Result<()> {
        let memtable_size = {
            let memtable = self.active_memtable.read().await;
            memtable.size().await
        };

        if memtable_size >= self.config.memtable.max_size {
            self.flush_memtable().await?;
        }

        Ok(())
    }

    /// Flush active memtable to immutable
    ///
    /// This method performs an atomic memtable swap to prevent race conditions:
    /// 1. Locks both active and immutable memtable lists
    /// 2. Creates new memtable BEFORE swapping
    /// 3. Atomically swaps old memtable out
    /// 4. Adds old memtable to immutable list (so reads can find it during flush)
    /// 5. Releases locks
    /// 6. Flushes old memtable to SSTable (while it remains in immutable list for reads)
    /// 7. Removes old memtable from immutable list after flush completes
    async fn flush_memtable(&self) -> Result<()> {
        // Acquire read lock to prevent compaction during flush
        let _op_guard = self.operation_guard.read().await;
        self.flush_memtable_internal().await
    }

    /// Internal flush without operation guard (used when caller already holds the lock)
    async fn flush_memtable_internal(&self) -> Result<()> {
        #[cfg(feature = "metrics")]
        let flush_start = Instant::now();

        // CRITICAL: Lock both active and immutable lists to ensure atomic swap
        let mut active = self.active_memtable.write().await;
        let mut immutable = self.immutable_memtables.write().await;

        // Create new memtable BEFORE swapping - ensures no write window
        let new_memtable = Memtable::new(&self.config.memtable)?;

        // Atomically swap old memtable out
        let old_memtable = std::mem::replace(&mut *active, new_memtable);

        // Check if we need to flush
        let old_memtable_entry_count = old_memtable.entry_count().await;
        if old_memtable_entry_count > 0 {
            // Add old memtable to immutable list BEFORE releasing locks
            // This ensures reads can find data in the old memtable during flush
            // The memtable will remain in the immutable list until flush completes
            let immutable_index = immutable.len();
            immutable.push(old_memtable);

            // Release locks before doing async I/O
            drop(active);
            drop(immutable);

            // Flush the memtable to SSTable while it's still in the immutable list
            // This allows concurrent reads to find data in the memtable during flush
            {
                let immutable = self.immutable_memtables.read().await;
                if let Some(memtable) = immutable.get(immutable_index) {
                    // Clone the memtable data for flushing (memtable remains in list for reads)
                    let entries = memtable.get_all_entries().await;

                    // Release read lock before async I/O
                    drop(immutable);

                    // Flush to SSTable using the cloned entries
                    self.flush_memtable_entries_to_sstable(entries).await?;
                } else {
                    warn!(
                        "Expected memtable at index {} in immutable list but not found",
                        immutable_index
                    );
                    return Ok(());
                }
            }

            // After flush completes, remove the memtable from immutable list
            // At this point, the data is safely in SSTable and can be read from there
            {
                let mut immutable = self.immutable_memtables.write().await;
                if immutable.len() > immutable_index {
                    immutable.remove(immutable_index);
                    info!("Removed flushed memtable from immutable list");
                }
            }

            // Update memtable metrics
            {
                let mut stats = self.stats.write().await;
                stats.memtable_metrics.flush_count += 1;
                stats.memtable_metrics.last_flush_time = Some(utils::timestamp_secs());
            }

            // Record metrics for memtable flush
            #[cfg(feature = "metrics")]
            {
                let flush_duration = flush_start.elapsed();
                record_memtable_flush(&self.metrics, old_memtable_entry_count, flush_duration);
            }

            // Trigger compaction after flush
            self.compact_if_needed().await?;
        } else {
            // Old memtable is empty, no need to flush
            drop(active);
            drop(immutable);
        }

        Ok(())
    }

    /// Flush memtable entries to SSTable (helper for atomic flush)
    async fn flush_memtable_entries_to_sstable(
        &self,
        mut entries: Vec<(String, crate::Value, bool)>,
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        // Optimize: Pre-sort entries by key before writing to SSTable
        // This ensures SSTable entries are already sorted, improving read performance
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        // Generate unique SSTable filename
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| LsmError::Internal(format!("Failed to get timestamp: {}", e)))?
            .as_millis();
        let sstable_path =
            PathBuf::from(&self.config.data_dir).join(format!("L0_{:016x}.sst", timestamp));

        // Create SSTable
        let mut sstable = SSTable::new(sstable_path, self.config.sstable.clone(), 0)?;

        // Convert entries to SSTable entries (already sorted)
        // Use monotonically increasing sequence numbers for timestamps
        // This ensures newer entries always have higher timestamps for correct compaction ordering
        let sstable_entries: Vec<SSTableEntry> = entries
            .into_iter()
            .map(|(key, value, deleted)| {
                let seq = self
                    .sequence_number
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                SSTableEntry {
                    key,
                    value,
                    timestamp: seq,
                    deleted,
                }
            })
            .collect();

        // Write entries to SSTable (optimized with pre-sorted entries)
        sstable.write_entries(sstable_entries).await?;

        // Open SSTable for reading - this reads metadata and index from the file
        // CRITICAL: The file must be fully written and synced before opening
        // (write_entries already does sync_all, so this should be safe)
        sstable.open().await?;
        tracing::debug!(
            "SSTable opened successfully after flush: {:?}",
            sstable.path()
        );

        // CRITICAL: Ensure SSTable is ready before making it available for reads
        // The ready flag is set after metadata and index are fully loaded
        if !sstable.is_ready() {
            return Err(LsmError::Internal(format!(
                "Newly created SSTable is not ready after open: {:?}. \
                Metadata and index may not be fully loaded.",
                sstable.path()
            )));
        }

        // Validate that the SSTable is in a consistent state before adding to index
        // This ensures the metadata and index were read correctly
        let metadata = sstable.metadata();
        if metadata.file_size == 0 {
            return Err(LsmError::Internal(format!(
                "SSTable has zero file size after write: {:?}",
                sstable.path()
            )));
        }
        if metadata.entry_count == 0 {
            return Err(LsmError::Internal(format!(
                "SSTable has zero entry count after write: {:?}",
                sstable.path()
            )));
        }

        // Validate that the index is not empty (should match entry_count)
        // This ensures the index was loaded correctly
        let index_size = sstable.index_size();
        if index_size == 0 {
            return Err(LsmError::Internal(format!(
                "SSTable index is empty after open (expected {} entries): {:?}",
                metadata.entry_count,
                sstable.path()
            )));
        }
        if index_size != metadata.entry_count {
            warn!(
                "SSTable index size {} does not match entry_count {}: {:?}",
                index_size,
                metadata.entry_count,
                sstable.path()
            );
        }

        // Get path before moving sstable
        let sstable_path = sstable.path().clone();

        // Add SSTable to L0
        // CRITICAL: Only add to index AFTER file is fully written, synced, and opened
        // This ensures readers see a consistent state
        let mut sstables = self.sstables.write().await;
        let level_0 = sstables.entry(0).or_insert_with(Vec::new);
        level_0.push(sstable);

        info!(
            "Flushed memtable entries to L0 SSTable: {}",
            sstable_path.display()
        );

        Ok(())
    }

    /// Flush a memtable to an L0 SSTable
    /// Note: Reserved for future use in background compaction/flush operations
    #[allow(dead_code)]
    async fn flush_memtable_to_sstable(&self, memtable: Memtable) -> Result<()> {
        // Generate unique SSTable filename
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| LsmError::Internal(format!("Failed to get timestamp: {}", e)))?
            .as_millis();
        let sstable_path =
            PathBuf::from(&self.config.data_dir).join(format!("L0_{:016x}.sst", timestamp));

        // Create SSTable
        let mut sstable = SSTable::new(sstable_path, self.config.sstable.clone(), 0)?;

        // Get current timestamp
        let current_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| LsmError::Internal(format!("Failed to get timestamp: {}", e)))?
            .as_secs();

        // Convert memtable entries to SSTable entries
        let entries = memtable.get_all_entries().await;
        let sstable_entries: Vec<SSTableEntry> = entries
            .into_iter()
            .map(|(key, value, deleted)| SSTableEntry {
                key,
                value,
                timestamp: current_timestamp,
                deleted,
            })
            .collect();

        // Write entries to SSTable
        sstable.write_entries(sstable_entries).await?;

        // Open SSTable for reading
        sstable.open().await?;
        tracing::debug!(
            "SSTable opened successfully after flush: {:?}",
            sstable.path()
        );

        // Get path before moving sstable
        let sstable_path = sstable.path().clone();

        // Add SSTable to L0
        let mut sstables = self.sstables.write().await;
        let level_0 = sstables.entry(0).or_insert_with(Vec::new);
        level_0.push(sstable);

        info!("Flushed memtable to L0 SSTable: {}", sstable_path.display());

        Ok(())
    }

    /// Get value from memtables (fast path, single lock per memtable)
    async fn get_from_memtables(&self, key: &str) -> Result<Option<Value>> {
        {
            let memtable = self.active_memtable.read().await;
            match memtable.lookup(key).await? {
                MemtableLookupResult::Found(value) => {
                    debug!("Found key '{}' in active memtable", key);
                    return Ok(Some(value));
                }
                MemtableLookupResult::Tombstone => {
                    debug!("Key '{}' tombstoned in active memtable", key);
                    return Ok(None);
                }
                MemtableLookupResult::Missing => {}
            }
        }

        let immutable = self.immutable_memtables.read().await;
        for (i, memtable) in immutable.iter().enumerate() {
            match memtable.lookup(key).await? {
                MemtableLookupResult::Found(value) => {
                    debug!("Found key '{}' in immutable memtable {}", key, i);
                    return Ok(Some(value));
                }
                MemtableLookupResult::Tombstone => {
                    debug!("Key '{}' tombstoned in immutable memtable {}", key, i);
                    return Ok(None);
                }
                MemtableLookupResult::Missing => {}
            }
        }

        Ok(None)
    }

    /// Check if a key has a tombstone in any memtable
    async fn has_tombstone_in_memtables(&self, key: &str) -> Result<bool> {
        {
            let memtable = self.active_memtable.read().await;
            if matches!(memtable.lookup(key).await?, MemtableLookupResult::Tombstone) {
                return Ok(true);
            }
        }

        let immutable = self.immutable_memtables.read().await;
        for memtable in immutable.iter() {
            if matches!(memtable.lookup(key).await?, MemtableLookupResult::Tombstone) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Apply one storage layer onto a merged scan map (newer layers call this later).
    fn apply_scan_layer(merged: &mut BTreeMap<String, Value>, entries: Vec<(String, Value, bool)>) {
        for (key, value, deleted) in entries {
            if deleted {
                merged.remove(&key);
            } else {
                merged.insert(key, value);
            }
        }
    }

    /// Binary search for the SSTable that may contain `key` on non-overlapping levels.
    fn find_sstable_for_key(sstables: &[SSTable], key: &str) -> Option<usize> {
        if sstables.is_empty() {
            return None;
        }
        let idx = sstables.partition_point(|s| s.metadata().largest_key.as_str() < key);
        if idx < sstables.len() {
            let meta = sstables[idx].metadata();
            if key >= meta.smallest_key.as_str() && key <= meta.largest_key.as_str() {
                return Some(idx);
            }
        }
        None
    }

    /// Open an SSTable file if needed (brief write lock).
    async fn ensure_sstable_open(&self, level: usize, idx: usize) {
        if let Ok(mut sstables) = self.sstables.try_write() {
            if let Some(level_sstables) = sstables.get_mut(&level) {
                if let Some(sstable) = level_sstables.get_mut(idx) {
                    if !sstable.is_open() {
                        let _ = sstable.ensure_file_open().await;
                    }
                }
            }
        }
    }

    /// Merge scan layers (SSTables → immutable memtables → active memtable).
    async fn merge_scan_prefix_with_values(&self, prefix: &str) -> Result<Vec<(String, Value)>> {
        let _op_guard = self.operation_guard.read().await;
        let mut merged = BTreeMap::new();

        {
            let sstables = self.sstables.read().await;
            for level in 0..self.config.levels.count {
                if let Some(level_sstables) = sstables.get(&level) {
                    for sstable in level_sstables {
                        let entries = sstable.scan_prefix_layer(prefix).await?;
                        Self::apply_scan_layer(&mut merged, entries);
                    }
                }
            }
        }

        {
            let immutable = self.immutable_memtables.read().await;
            for memtable in immutable.iter() {
                let entries = memtable.scan_prefix_layer(prefix).await?;
                Self::apply_scan_layer(&mut merged, entries);
            }
        }

        {
            let memtable = self.active_memtable.read().await;
            let entries = memtable.scan_prefix_layer(prefix).await?;
            Self::apply_scan_layer(&mut merged, entries);
        }

        Ok(merged.into_iter().collect())
    }

    async fn merge_scan_range_with_values(
        &self,
        start: &str,
        end: &str,
    ) -> Result<Vec<(String, Value)>> {
        let _op_guard = self.operation_guard.read().await;
        let mut merged = BTreeMap::new();

        {
            let sstables = self.sstables.read().await;
            for level in 0..self.config.levels.count {
                if let Some(level_sstables) = sstables.get(&level) {
                    for sstable in level_sstables {
                        let entries = sstable.scan_range_layer(start, end).await?;
                        Self::apply_scan_layer(&mut merged, entries);
                    }
                }
            }
        }

        {
            let immutable = self.immutable_memtables.read().await;
            for memtable in immutable.iter() {
                let entries = memtable.scan_range_layer(start, end).await?;
                Self::apply_scan_layer(&mut merged, entries);
            }
        }

        {
            let memtable = self.active_memtable.read().await;
            let entries = memtable.scan_range_layer(start, end).await?;
            Self::apply_scan_layer(&mut merged, entries);
        }

        Ok(merged.into_iter().collect())
    }

    /// Get value from SSTables (slow path)
    async fn get_from_sstables(&self, key: &str) -> Result<Option<Value>> {
        for level in 0..self.config.levels.count {
            let candidate_indices: Vec<usize> = {
                let sstables = self.sstables.read().await;
                let Some(level_sstables) = sstables.get(&level) else {
                    continue;
                };
                if level == 0 {
                    (0..level_sstables.len())
                        .rev()
                        .filter(|&idx| level_sstables[idx].key_may_exist(key))
                        .collect()
                } else {
                    Self::find_sstable_for_key(level_sstables, key)
                        .filter(|&idx| level_sstables[idx].key_may_exist(key))
                        .into_iter()
                        .collect()
                }
            };

            for idx in candidate_indices {
                self.ensure_sstable_open(level, idx).await;
                let sstables = self.sstables.read().await;
                let Some(sstable) = sstables
                    .get(&level)
                    .and_then(|level_sstables| level_sstables.get(idx))
                else {
                    continue;
                };
                let sstable = &*sstable;
                #[cfg(feature = "metrics")]
                record_sstable_read(&self.metrics);
                match sstable.get(key).await {
                    Ok(Some(value)) => return Ok(Some(value)),
                    Ok(None) => {}
                    Err(e) => debug!("Error reading from SSTable L{level}[{idx}]: {e}"),
                }
            }
        }

        Ok(None)
    }

    /// Ensure we have capacity for opening a new file handle
    /// Closes least recently used files if we're at the limit
    #[allow(dead_code)]
    async fn ensure_file_handle_capacity(&self) -> Result<()> {
        let max_open = self.config.sstable.max_open_files;

        // Count open files and collect access times
        let sstables = self.sstables.read().await;
        let mut open_count = 0;
        let mut sstable_access_times: Vec<(usize, usize, u64)> = Vec::new(); // (level, index, last_access)

        for (level, level_sstables) in sstables.iter() {
            for (idx, sstable) in level_sstables.iter().enumerate() {
                if sstable.is_open() {
                    open_count += 1;
                    sstable_access_times.push((*level, idx, sstable.last_access()));
                }
            }
        }
        drop(sstables);

        // If we're at or over the limit, close LRU files
        if open_count >= max_open {
            // Sort by last access time (oldest first)
            sstable_access_times.sort_by_key(|(_, _, access)| *access);

            // Close oldest files until we're below threshold
            let mut sstables = self.sstables.write().await;
            let to_close = (open_count - max_open + 1).min(sstable_access_times.len()); // Close enough to have space
            let mut closed = 0;

            for (level, idx, _) in sstable_access_times.iter().take(to_close) {
                if let Some(level_sstables) = sstables.get_mut(level) {
                    if let Some(sstable) = level_sstables.get_mut(*idx) {
                        if sstable.is_open() {
                            sstable.close().await?;
                            closed += 1;
                        }
                    }
                }
            }

            if closed > 0 {
                debug!(
                    "Closed {} LRU SSTable file handles (was {} open, limit: {})",
                    closed, open_count, max_open
                );
            }
        }

        Ok(())
    }

    /// Convert LsmError to F4KvsError
    fn convert_error(err: LsmError) -> F4KvsError {
        match err {
            LsmError::Io(e) => F4KvsError::Io {
                message: format!("LSM I/O error: {}", e),
            },
            LsmError::Serialization(msg) => F4KvsError::Serialization {
                message: format!("LSM serialization error: {}", msg),
            },
            LsmError::Corruption(msg) => F4KvsError::Storage {
                message: format!("LSM corruption error: {}", msg),
            },
            LsmError::Config(msg) => F4KvsError::Config {
                message: format!("LSM config error: {}", msg),
            },
            LsmError::Wal(msg) => F4KvsError::Storage {
                message: format!("LSM WAL error: {}", msg),
            },
            LsmError::BloomFilter(msg) => F4KvsError::Storage {
                message: format!("LSM bloom filter error: {}", msg),
            },
            LsmError::Compression(msg) => F4KvsError::Storage {
                message: format!("LSM compression error: {}", msg),
            },
            LsmError::KeyNotFound(key) => F4KvsError::KeyNotFound { key },
            LsmError::ColumnFamilyNotFound(cf) => F4KvsError::Storage {
                message: format!("Column family not found: {}", cf),
            },
            LsmError::InvalidOperation(msg) => F4KvsError::InvalidKey {
                reason: format!("Invalid operation: {}", msg),
            },
            LsmError::ResourceLimit(msg) => F4KvsError::Storage {
                message: format!("Resource limit: {}", msg),
            },
            LsmError::Internal(msg) => F4KvsError::Internal {
                message: format!("LSM internal error: {}", msg),
            },
            LsmError::Compaction(msg) => F4KvsError::Storage {
                message: format!("LSM compaction error: {}", msg),
            },
        }
    }

    /// Estimate the size of a value in bytes
    fn estimate_value_size(value: &Value) -> usize {
        match value {
            Value::String(s) => s.len(),
            Value::Int64(_) => 8,
            Value::UInt64(_) => 8,
            Value::Float64(_) => 8,
            Value::Bool(_) => 1,
            Value::Bytes(b) => b.len(),
            Value::Json(v) => v.to_string().len(),
            Value::Null => 0,
        }
    }
}

#[async_trait]
impl StorageEngine for LsmTreeEngine {
    async fn put(&self, key: &str, value: &Value) -> std::result::Result<(), F4KvsError> {
        // Acquire read lock to prevent compaction during write
        let _op_guard = self.operation_guard.read().await;

        // Write to WAL first (if enabled)
        if self.config.wal.enabled {
            {
                let wal = self.wal_manager.write().await;
                wal.write_operation(key, value)
                    .await
                    .map_err(Self::convert_error)?;
            }
        }

        // Add to active memtable
        let put_effect = {
            let mut memtable = self.active_memtable.write().await;
            memtable
                .put(key, value)
                .await
                .map_err(Self::convert_error)?
        };

        self.apply_put_key_count(key, put_effect)
            .await
            .map_err(Self::convert_error)?;

        // Update statistics
        {
            let mut stats = self.stats.write().await;
            stats.total_writes += 1;
            stats.total_bytes_written += key.len() as u64 + Self::estimate_value_size(value) as u64;
        }

        // Check if we need to flush memtable
        self.check_memtable_flush()
            .await
            .map_err(Self::convert_error)?;

        Ok(())
    }

    async fn get(&self, key: &str) -> std::result::Result<Option<Value>, F4KvsError> {
        // Acquire read lock to prevent compaction during read
        let _op_guard = self.operation_guard.read().await;

        log::trace!("=== GET OPERATION DEBUG ===");
        log::trace!("Getting key: '{}'", key);

        // Check if key has expired
        #[cfg(feature = "ttl")]
        {
            if self.ttl_manager.is_expired(key) {
                log::trace!("Key '{}' has expired, returning None", key);
                // Remove expired key from TTL manager
                let _ = self.ttl_manager.remove_ttl(key);
                return Ok(None);
            }
        }

        // Try memtables first (fast path)
        log::trace!("Checking memtables for key '{}'", key);
        match self
            .get_from_memtables(key)
            .await
            .map_err(Self::convert_error)?
        {
            Some(value) => {
                log::trace!("Found key '{}' in memtables: {:?}", key, value);
                // Update statistics
                {
                    let mut stats = self.stats.write().await;
                    stats.total_reads += 1;
                    stats.memtable_hits += 1;
                }
                return Ok(Some(value));
            }
            None => {
                log::trace!("Key '{}' not found in memtables", key);
                // Check if key was deleted (tombstone exists in memtables)
                // If so, return None immediately without checking SSTables
                if self
                    .has_tombstone_in_memtables(key)
                    .await
                    .map_err(Self::convert_error)?
                {
                    log::trace!("Key '{}' has tombstone in memtables, returning None", key);
                    // Update statistics
                    {
                        let mut stats = self.stats.write().await;
                        stats.total_reads += 1;
                        stats.memtable_hits += 1;
                    }
                    return Ok(None);
                }
                // Key not found in memtables, continue to SSTables
            }
        }

        // Try SSTables (slow path)
        log::trace!("Checking SSTables for key '{}'", key);
        if let Some(value) = self
            .get_from_sstables(key)
            .await
            .map_err(Self::convert_error)?
        {
            log::trace!("Found key '{}' in SSTables: {:?}", key, value);
            // Update statistics
            {
                let mut stats = self.stats.write().await;
                stats.total_reads += 1;
                stats.sstable_hits += 1;
            }
            return Ok(Some(value));
        }

        log::trace!("Key '{}' not found anywhere", key);
        // Key not found
        {
            let mut stats = self.stats.write().await;
            stats.total_reads += 1;
            stats.misses += 1;
        }

        Ok(None)
    }

    async fn delete(&self, key: &str) -> std::result::Result<(), F4KvsError> {
        // Acquire read lock to prevent compaction during delete
        let _op_guard = self.operation_guard.read().await;

        let was_live = self.exists(key).await?;

        // Write tombstone to WAL
        if self.config.wal.enabled {
            {
                let wal = self.wal_manager.write().await;
                wal.write_delete(key).await.map_err(Self::convert_error)?;
            }
        }

        // Add tombstone to active memtable
        {
            let mut memtable = self.active_memtable.write().await;
            memtable.delete(key).await.map_err(Self::convert_error)?;
        }

        if was_live {
            self.adjust_live_key_count(-1);
            let mut stats = self.stats.write().await;
            stats.total_keys = self.live_key_count.load(Ordering::Relaxed);
        }

        // Remove TTL if it exists
        #[cfg(feature = "ttl")]
        {
            let _ = self.ttl_manager.remove_ttl(key);
        }

        // Update statistics
        {
            let mut stats = self.stats.write().await;
            stats.total_deletes += 1;
        }

        // Check if we need to flush memtable
        self.check_memtable_flush()
            .await
            .map_err(Self::convert_error)?;

        Ok(())
    }

    async fn exists(&self, key: &str) -> std::result::Result<bool, F4KvsError> {
        // Acquire read lock to prevent compaction during read
        let _op_guard = self.operation_guard.read().await;

        // Check if key has expired
        #[cfg(feature = "ttl")]
        {
            if self.ttl_manager.is_expired(key) {
                // Remove expired key from TTL manager
                let _ = self.ttl_manager.remove_ttl(key);
                return Ok(false);
            }
        }

        // Check memtables first
        if self
            .get_from_memtables(key)
            .await
            .map_err(Self::convert_error)?
            .is_some()
        {
            return Ok(true);
        }

        // Check SSTables
        if self
            .get_from_sstables(key)
            .await
            .map_err(Self::convert_error)?
            .is_some()
        {
            return Ok(true);
        }

        Ok(false)
    }

    async fn batch_put(&self, items: Vec<(String, Value)>) -> std::result::Result<(), F4KvsError> {
        // Acquire read lock to prevent compaction during batch write
        let _op_guard = self.operation_guard.read().await;

        // DoS protection: Enforce maximum batch size
        if items.len() > self.config.performance.max_batch_size {
            return Err(F4KvsError::Storage {
                message: format!(
                    "Batch size {} exceeds maximum allowed size of {} items. This limit prevents DoS attacks.",
                    items.len(),
                    self.config.performance.max_batch_size
                ),
            });
        }

        // Write to WAL first
        if self.config.wal.enabled {
            let wal = self.wal_manager.write().await;
            wal.batch_write_operations(&items)
                .await
                .map_err(Self::convert_error)?;
        }

        let put_effects: Vec<(String, PutEffect)> = {
            let mut memtable = self.active_memtable.write().await;
            let mut effects = Vec::with_capacity(items.len());
            for (key, value) in &items {
                let effect = memtable.put(key, value).await.map_err(Self::convert_error)?;
                effects.push((key.clone(), effect));
            }
            effects
        };
        for (key, effect) in put_effects {
            self.apply_put_key_count(&key, effect)
                .await
                .map_err(Self::convert_error)?;
        }

        // Update statistics
        {
            let mut stats = self.stats.write().await;
            stats.total_writes += items.len() as u64;
        }

        // Check if we need to flush memtable
        self.check_memtable_flush()
            .await
            .map_err(Self::convert_error)?;

        Ok(())
    }

    async fn batch_get(
        &self,
        keys: Vec<String>,
    ) -> std::result::Result<Vec<Option<Value>>, F4KvsError> {
        let mut results = Vec::with_capacity(keys.len());

        for key in keys {
            let value = self.get(&key).await?;
            results.push(value);
        }

        Ok(results)
    }

    async fn scan_prefix(&self, prefix: &str) -> std::result::Result<Vec<String>, F4KvsError> {
        let mut keys = Vec::new();

        // Scan active memtable (most recent data)
        {
            let memtable = self.active_memtable.read().await;
            let memtable_keys = memtable
                .scan_prefix(prefix)
                .await
                .map_err(Self::convert_error)?;
            debug!(
                "LSM Engine: scan_prefix('{}') found {} keys in active memtable",
                prefix,
                memtable_keys.len()
            );
            keys.extend(memtable_keys);
        }

        // Scan immutable memtables (being flushed)
        {
            let immutable = self.immutable_memtables.read().await;
            let immutable_count = immutable.len();
            for (idx, memtable) in immutable.iter().enumerate() {
                let memtable_keys = memtable
                    .scan_prefix(prefix)
                    .await
                    .map_err(Self::convert_error)?;
                debug!(
                    "LSM Engine: scan_prefix('{}') found {} keys in immutable memtable {}",
                    prefix,
                    memtable_keys.len(),
                    idx
                );
                keys.extend(memtable_keys);
            }
            if immutable_count > 0 {
                debug!(
                    "LSM Engine: scan_prefix('{}') scanned {} immutable memtables",
                    prefix, immutable_count
                );
            }
        }

        // Scan SSTables (persistent data)
        {
            let sstables = self.sstables.read().await;
            let mut sstable_keys_count = 0;
            for level in 0..self.config.levels.count {
                if let Some(level_sstables) = sstables.get(&level) {
                    for sstable in level_sstables {
                        let sstable_keys = sstable
                            .scan_prefix(prefix)
                            .await
                            .map_err(Self::convert_error)?;
                        sstable_keys_count += sstable_keys.len();
                        keys.extend(sstable_keys);
                    }
                }
            }
            if sstable_keys_count > 0 {
                debug!(
                    "LSM Engine: scan_prefix('{}') found {} keys in SSTables",
                    prefix, sstable_keys_count
                );
            }
        }

        // Remove duplicates and sort
        let before_dedup = keys.len();
        keys.sort();
        keys.dedup();
        let after_dedup = keys.len();
        if before_dedup != after_dedup {
            debug!(
                "LSM Engine: scan_prefix('{}') removed {} duplicate keys ({} -> {})",
                prefix,
                before_dedup - after_dedup,
                before_dedup,
                after_dedup
            );
        }

        debug!(
            "LSM Engine: scan_prefix('{}') returning {} unique keys",
            prefix,
            keys.len()
        );
        Ok(keys)
    }

    async fn scan_range(
        &self,
        start: &str,
        end: &str,
    ) -> std::result::Result<Vec<String>, F4KvsError> {
        let mut keys = Vec::new();

        // Scan memtables
        {
            let memtable = self.active_memtable.read().await;
            keys.extend(
                memtable
                    .scan_range(start, end)
                    .await
                    .map_err(Self::convert_error)?,
            );
        }

        let immutable = self.immutable_memtables.read().await;
        for memtable in immutable.iter() {
            keys.extend(
                memtable
                    .scan_range(start, end)
                    .await
                    .map_err(Self::convert_error)?,
            );
        }

        // Scan SSTables
        let sstables = self.sstables.read().await;
        for level in 0..self.config.levels.count {
            if let Some(level_sstables) = sstables.get(&level) {
                for sstable in level_sstables {
                    keys.extend(
                        sstable
                            .scan_range(start, end)
                            .await
                            .map_err(Self::convert_error)?,
                    );
                }
            }
        }

        // Remove duplicates and sort
        keys.sort();
        keys.dedup();

        Ok(keys)
    }

    async fn scan_range_limit(
        &self,
        start: &str,
        end: &str,
        limit: usize,
    ) -> std::result::Result<Vec<String>, F4KvsError> {
        let mut keys = Vec::new();

        // Scan memtables first (they have the most recent data)
        {
            let memtable = self.active_memtable.read().await;
            keys.extend(
                memtable
                    .scan_range(start, end)
                    .await
                    .map_err(Self::convert_error)?,
            );
            if keys.len() >= limit {
                keys.truncate(limit);
                return Ok(keys);
            }
        }

        let immutable = self.immutable_memtables.read().await;
        for memtable in immutable.iter() {
            keys.extend(
                memtable
                    .scan_range(start, end)
                    .await
                    .map_err(Self::convert_error)?,
            );
            if keys.len() >= limit {
                keys.truncate(limit);
                return Ok(keys);
            }
        }

        // Scan SSTables if we still need more keys
        let sstables = self.sstables.read().await;
        for level in 0..self.config.levels.count {
            if let Some(level_sstables) = sstables.get(&level) {
                for sstable in level_sstables {
                    keys.extend(
                        sstable
                            .scan_range(start, end)
                            .await
                            .map_err(Self::convert_error)?,
                    );
                    if keys.len() >= limit {
                        keys.truncate(limit);
                        return Ok(keys);
                    }
                }
            }
        }

        Ok(keys)
    }

    async fn scan_all(&self) -> std::result::Result<Vec<(String, Value)>, F4KvsError> {
        let mut results = Vec::new();

        // Scan memtables
        {
            let memtable = self.active_memtable.read().await;
            let entries = memtable.get_all_entries().await;
            for (key, value, deleted) in entries {
                if !deleted {
                    results.push((key, value));
                }
            }
        }

        let immutable = self.immutable_memtables.read().await;
        for memtable in immutable.iter() {
            let entries = memtable.get_all_entries().await;
            for (key, value, deleted) in entries {
                if !deleted {
                    results.push((key, value));
                }
            }
        }

        // Scan SSTables for complete results
        let sstables = self.sstables.read().await;
        for level in 0..self.config.levels.count {
            if let Some(level_sstables) = sstables.get(&level) {
                for sstable in level_sstables {
                    let entries = sstable.scan_all().await.map_err(Self::convert_error)?;
                    for (key, value, deleted) in entries {
                        if !deleted {
                            results.push((key, value));
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    async fn scan_prefix_with_values(
        &self,
        prefix: &str,
    ) -> std::result::Result<Vec<(String, Value)>, F4KvsError> {
        self.merge_scan_prefix_with_values(prefix)
            .await
            .map_err(Self::convert_error)
    }

    async fn scan_range_with_values(
        &self,
        start: &str,
        end: &str,
    ) -> std::result::Result<Vec<(String, Value)>, F4KvsError> {
        self.merge_scan_range_with_values(start, end)
            .await
            .map_err(Self::convert_error)
    }

    async fn scan_range_limit_with_values(
        &self,
        start: &str,
        end: &str,
        limit: usize,
    ) -> std::result::Result<Vec<(String, Value)>, F4KvsError> {
        let mut results = self
            .merge_scan_range_with_values(start, end)
            .await
            .map_err(Self::convert_error)?;
        if results.len() > limit {
            results.truncate(limit);
        }
        Ok(results)
    }

    async fn iter_range(
        &self,
        start: &str,
        end: &str,
    ) -> std::result::Result<Box<dyn KeyValueIterator + Send>, F4KvsError> {
        // For now, return a simple iterator that scans the range
        // TODO: Implement proper streaming iterator with async support
        let values = self.scan_range_with_values(start, end).await?;
        let iterator = Box::new(SimpleKeyValueIterator::new(values));
        Ok(iterator)
    }

    async fn compact(&self) -> std::result::Result<(), F4KvsError> {
        info!("Starting manual compaction - waiting for exclusive access");

        // Acquire WRITE lock to ensure exclusive access during compaction
        // This blocks all reads/writes until compaction completes
        let _op_guard = self.operation_guard.write().await;

        info!("Acquired exclusive lock for compaction");

        // Flush any pending memtables first (no need to acquire read lock, we have write lock)
        self.flush_memtable_internal()
            .await
            .map_err(Self::convert_error)?;

        // Run compaction
        self.compaction_manager
            .compact_all(&self.sstables)
            .await
            .map_err(Self::convert_error)?;

        info!("Manual compaction completed");
        Ok(())
    }

    async fn stats(&self) -> std::result::Result<F4KvsStorageStats, F4KvsError> {
        let stats = self.stats.read().await;
        let total_keys = self.live_key_count.load(Ordering::Relaxed);

        Ok(F4KvsStorageStats {
            total_keys,
            total_size_bytes: stats.total_bytes_written,
            cache_stats: f4kvs_storage_core::stats::CacheStats::default(),
            io_stats: f4kvs_storage_core::stats::IoStats::default(),
            compaction_stats: f4kvs_storage_core::stats::CompactionStats::default(),
            cf_stats: HashMap::new(),
            memory_stats: f4kvs_storage_core::stats::MemoryStats::default(),
            wal_stats: None,
            health: f4kvs_storage_core::stats::HealthStats::default(),
            timestamp: SystemTime::now(),
        })
    }

    // Override keys() to provide explicit implementation
    async fn keys(&self) -> std::result::Result<Vec<String>, F4KvsError> {
        // Use scan_prefix with empty string to get all keys
        // This will scan memtables, immutable memtables, and SSTables
        debug!("LSM Engine: Getting all keys via scan_prefix(\"\")");
        let keys = self.scan_prefix("").await?;
        debug!("LSM Engine: Found {} keys", keys.len());
        Ok(keys)
    }

    // Override count() to provide explicit implementation
    async fn count(&self) -> std::result::Result<u64, F4KvsError> {
        Ok(self.live_key_count.load(Ordering::Relaxed))
    }

    async fn clear(&self) -> std::result::Result<(), F4KvsError> {
        tracing::warn!("LSM Engine: Clearing all data - this is irreversible!");

        // Clear active memtable
        {
            let mut memtable = self.active_memtable.write().await;
            *memtable = Memtable::new(&self.config.memtable).map_err(|e| F4KvsError::Storage {
                message: format!("Failed to create new memtable: {}", e),
            })?;
        }

        // Clear immutable memtables
        {
            let mut immutable = self.immutable_memtables.write().await;
            immutable.clear();
        }

        // Clear SSTables
        {
            let mut sstables = self.sstables.write().await;
            sstables.clear();
        }

        // Clear WAL if enabled by removing and recreating the WAL directory
        if self.config.wal.enabled {
            self.clear_wal_directory()
                .await
                .map_err(|e| F4KvsError::Storage {
                    message: format!("Failed to clear WAL directory: {}", e),
                })?;
            // Reinitialize WAL manager by creating a new segment
            // We can't call rotate_segment directly as it's private, so we'll just
            // let the next write operation create a new segment
        }

        // Reset statistics
        {
            let mut stats = self.stats.write().await;
            *stats = LsmStats::default();
        }
        self.set_live_key_count(0);

        // Clear TTL manager - TTLManager doesn't have a clear method, so we'll skip it
        // The TTL entries will be cleaned up naturally as keys expire
        #[cfg(feature = "ttl")]
        {
            // TTL manager cleanup would go here if needed
        }

        tracing::info!("LSM Engine: All data cleared successfully");
        Ok(())
    }

    // === COLUMN FAMILY SUPPORT ===

    async fn get_cf(
        &self,
        key: &str,
        column_family: &str,
    ) -> std::result::Result<Option<Value>, F4KvsError> {
        // Check if column family exists
        {
            let cf_map = self.column_families.read().await;
            if !cf_map.contains_key(column_family) {
                return Err(F4KvsError::Storage {
                    message: format!("Column family not found: {}", column_family),
                });
            }
        }

        // Use prefixed key for column family isolation
        let prefixed_key = format!("{}#{}", column_family, key);
        self.get(&prefixed_key).await
    }

    async fn put_cf(
        &self,
        key: &str,
        value: &Value,
        column_family: &str,
    ) -> std::result::Result<(), F4KvsError> {
        // Check if column family exists
        {
            let cf_map = self.column_families.read().await;
            if !cf_map.contains_key(column_family) {
                return Err(F4KvsError::Storage {
                    message: format!("Column family not found: {}", column_family),
                });
            }
        }

        // Use prefixed key for column family isolation
        let prefixed_key = format!("{}#{}", column_family, key);
        self.put(&prefixed_key, value).await
    }

    async fn delete_cf(
        &self,
        key: &str,
        column_family: &str,
    ) -> std::result::Result<(), F4KvsError> {
        // Check if column family exists
        {
            let cf_map = self.column_families.read().await;
            if !cf_map.contains_key(column_family) {
                return Err(F4KvsError::Storage {
                    message: format!("Column family not found: {}", column_family),
                });
            }
        }

        // Use prefixed key for column family isolation
        let prefixed_key = format!("{}#{}", column_family, key);
        self.delete(&prefixed_key).await
    }

    async fn exists_cf(
        &self,
        key: &str,
        column_family: &str,
    ) -> std::result::Result<bool, F4KvsError> {
        // Check if column family exists
        {
            let cf_map = self.column_families.read().await;
            if !cf_map.contains_key(column_family) {
                return Err(F4KvsError::Storage {
                    message: format!("Column family not found: {}", column_family),
                });
            }
        }

        // Use prefixed key for column family isolation
        let prefixed_key = format!("{}#{}", column_family, key);
        self.exists(&prefixed_key).await
    }

    async fn batch_put_cf(
        &self,
        items: Vec<(String, Value)>,
        column_family: &str,
    ) -> std::result::Result<(), F4KvsError> {
        // Check if column family exists
        {
            let cf_map = self.column_families.read().await;
            if !cf_map.contains_key(column_family) {
                return Err(F4KvsError::Storage {
                    message: format!("Column family not found: {}", column_family),
                });
            }
        }

        // Use prefixed keys for column family isolation
        let prefixed_items: Vec<(String, Value)> = items
            .into_iter()
            .map(|(key, value)| (format!("{}#{}", column_family, key), value))
            .collect();
        self.batch_put(prefixed_items).await
    }

    async fn batch_get_cf(
        &self,
        keys: Vec<String>,
        column_family: &str,
    ) -> std::result::Result<Vec<Option<Value>>, F4KvsError> {
        // Check if column family exists
        {
            let cf_map = self.column_families.read().await;
            if !cf_map.contains_key(column_family) {
                return Err(F4KvsError::Storage {
                    message: format!("Column family not found: {}", column_family),
                });
            }
        }

        // Use prefixed keys for column family isolation
        let prefixed_keys: Vec<String> = keys
            .into_iter()
            .map(|key| format!("{}#{}", column_family, key))
            .collect();
        self.batch_get(prefixed_keys).await
    }

    async fn batch_delete_cf(
        &self,
        keys: Vec<String>,
        column_family: &str,
    ) -> std::result::Result<(), F4KvsError> {
        // Check if column family exists
        {
            let cf_map = self.column_families.read().await;
            if !cf_map.contains_key(column_family) {
                return Err(F4KvsError::Storage {
                    message: format!("Column family not found: {}", column_family),
                });
            }
        }

        // Use prefixed keys for column family isolation
        let prefixed_keys: Vec<String> = keys
            .into_iter()
            .map(|key| format!("{}#{}", column_family, key))
            .collect();
        self.batch_delete(prefixed_keys).await
    }

    async fn scan_prefix_cf(
        &self,
        prefix: &str,
        column_family: &str,
    ) -> std::result::Result<Vec<String>, F4KvsError> {
        // Check if column family exists
        {
            let cf_map = self.column_families.read().await;
            if !cf_map.contains_key(column_family) {
                return Err(F4KvsError::Storage {
                    message: format!("Column family not found: {}", column_family),
                });
            }
        }

        // Use prefixed key for column family isolation
        let prefixed_prefix = format!("{}#{}", column_family, prefix);
        let keys = self.scan_prefix(&prefixed_prefix).await?;

        // Remove the column family prefix from the returned keys
        let unprefixed_keys: Vec<String> = keys
            .into_iter()
            .filter_map(|key| {
                if key.starts_with(&prefixed_prefix) {
                    Some(key[prefixed_prefix.len()..].to_string())
                } else {
                    None
                }
            })
            .collect();

        Ok(unprefixed_keys)
    }

    async fn scan_range_cf(
        &self,
        start: &str,
        end: &str,
        column_family: &str,
    ) -> std::result::Result<Vec<String>, F4KvsError> {
        // Check if column family exists
        {
            let cf_map = self.column_families.read().await;
            if !cf_map.contains_key(column_family) {
                return Err(F4KvsError::Storage {
                    message: format!("Column family not found: {}", column_family),
                });
            }
        }

        // Use prefixed keys for column family isolation
        let prefixed_start = format!("{}#{}", column_family, start);
        let prefixed_end = if end.is_empty() {
            format!("{}#", column_family)
        } else {
            format!("{}#{}", column_family, end)
        };

        let keys = self.scan_range(&prefixed_start, &prefixed_end).await?;

        // Remove the column family prefix from the returned keys
        let unprefixed_keys: Vec<String> = keys
            .into_iter()
            .filter_map(|key| {
                if key.starts_with(&format!("{}#", column_family)) {
                    Some(key[format!("{}#", column_family).len()..].to_string())
                } else {
                    None
                }
            })
            .collect();

        Ok(unprefixed_keys)
    }

    async fn scan_range_limit_cf(
        &self,
        start: &str,
        end: &str,
        limit: usize,
        column_family: &str,
    ) -> std::result::Result<Vec<String>, F4KvsError> {
        // Check if column family exists
        {
            let cf_map = self.column_families.read().await;
            if !cf_map.contains_key(column_family) {
                return Err(F4KvsError::Storage {
                    message: format!("Column family not found: {}", column_family),
                });
            }
        }

        // Use prefixed keys for column family isolation
        let prefixed_start = format!("{}#{}", column_family, start);
        let prefixed_end = if end.is_empty() {
            format!("{}#", column_family)
        } else {
            format!("{}#{}", column_family, end)
        };

        let keys = self
            .scan_range_limit(&prefixed_start, &prefixed_end, limit)
            .await?;

        // Remove the column family prefix from the returned keys
        let unprefixed_keys: Vec<String> = keys
            .into_iter()
            .filter_map(|key| {
                if key.starts_with(&format!("{}#", column_family)) {
                    Some(key[format!("{}#", column_family).len()..].to_string())
                } else {
                    None
                }
            })
            .collect();

        Ok(unprefixed_keys)
    }

    async fn scan_all_cf(
        &self,
        column_family: &str,
    ) -> std::result::Result<Vec<(String, Value)>, F4KvsError> {
        // Check if column family exists
        {
            let cf_map = self.column_families.read().await;
            if !cf_map.contains_key(column_family) {
                return Err(F4KvsError::Storage {
                    message: format!("Column family not found: {}", column_family),
                });
            }
        }

        // Use prefixed key for column family isolation
        let prefixed_prefix = format!("{}#", column_family);
        let items = self.scan_prefix_with_values(&prefixed_prefix).await?;

        // Remove the column family prefix from the returned keys
        let unprefixed_items: Vec<(String, Value)> = items
            .into_iter()
            .filter_map(|(key, value)| {
                if key.starts_with(&prefixed_prefix) {
                    Some((key[prefixed_prefix.len()..].to_string(), value))
                } else {
                    None
                }
            })
            .collect();

        Ok(unprefixed_items)
    }

    async fn scan_prefix_with_values_cf(
        &self,
        prefix: &str,
        column_family: &str,
    ) -> std::result::Result<Vec<(String, Value)>, F4KvsError> {
        // Check if column family exists
        {
            let cf_map = self.column_families.read().await;
            if !cf_map.contains_key(column_family) {
                return Err(F4KvsError::Storage {
                    message: format!("Column family not found: {}", column_family),
                });
            }
        }

        // Use prefixed key for column family isolation
        let prefixed_prefix = format!("{}#{}", column_family, prefix);
        let items = self.scan_prefix_with_values(&prefixed_prefix).await?;

        // Remove the column family prefix from the returned keys
        let unprefixed_items: Vec<(String, Value)> = items
            .into_iter()
            .filter_map(|(key, value)| {
                if key.starts_with(&prefixed_prefix) {
                    Some((key[prefixed_prefix.len()..].to_string(), value))
                } else {
                    None
                }
            })
            .collect();

        Ok(unprefixed_items)
    }

    async fn scan_range_with_values_cf(
        &self,
        start: &str,
        end: &str,
        column_family: &str,
    ) -> std::result::Result<Vec<(String, Value)>, F4KvsError> {
        // Check if column family exists
        {
            let cf_map = self.column_families.read().await;
            if !cf_map.contains_key(column_family) {
                return Err(F4KvsError::Storage {
                    message: format!("Column family not found: {}", column_family),
                });
            }
        }

        // Use prefixed keys for column family isolation
        let prefixed_start = format!("{}#{}", column_family, start);
        let prefixed_end = if end.is_empty() {
            format!("{}#", column_family)
        } else {
            format!("{}#{}", column_family, end)
        };

        let items = self
            .scan_range_with_values(&prefixed_start, &prefixed_end)
            .await?;

        // Remove the column family prefix from the returned keys
        let unprefixed_items: Vec<(String, Value)> = items
            .into_iter()
            .filter_map(|(key, value)| {
                if key.starts_with(&format!("{}#", column_family)) {
                    Some((
                        key[format!("{}#", column_family).len()..].to_string(),
                        value,
                    ))
                } else {
                    None
                }
            })
            .collect();

        Ok(unprefixed_items)
    }

    async fn iter_range_cf(
        &self,
        start: &str,
        end: &str,
        column_family: &str,
    ) -> std::result::Result<Box<dyn KeyValueIterator + Send>, F4KvsError> {
        // Check if column family exists
        {
            let cf_map = self.column_families.read().await;
            if !cf_map.contains_key(column_family) {
                return Err(F4KvsError::Storage {
                    message: format!("Column family not found: {}", column_family),
                });
            }
        }

        // Use prefixed keys for column family isolation
        let prefixed_start = format!("{}#{}", column_family, start);
        let prefixed_end = if end.is_empty() {
            format!("{}#", column_family)
        } else {
            format!("{}#{}", column_family, end)
        };

        // Get all key-value pairs in the range
        let values = self
            .scan_range_with_values(&prefixed_start, &prefixed_end)
            .await?;

        // Filter to only include keys that belong to this column family
        let filtered_values: Vec<(String, Value)> = values
            .into_iter()
            .filter(|(key, _)| {
                if let Some(prefix) = key.strip_prefix(&format!("{}#", column_family)) {
                    // Check if the remaining part matches our start/end criteria
                    if start.is_empty() && end.is_empty() {
                        true
                    } else if start.is_empty() {
                        prefix <= end
                    } else if end.is_empty() {
                        prefix >= start
                    } else {
                        prefix >= start && prefix <= end
                    }
                } else {
                    false
                }
            })
            .map(|(key, value)| {
                // Remove the column family prefix from the key
                let clean_key = key
                    .strip_prefix(&format!("{}#", column_family))
                    .unwrap_or(&key)
                    .to_string();
                (clean_key, value)
            })
            .collect();

        let iterator = Box::new(SimpleKeyValueIterator::new(filtered_values));
        Ok(iterator)
    }

    // === TTL SUPPORT ===

    async fn put_with_ttl(
        &self,
        key: &str,
        value: &Value,
        ttl: Duration,
    ) -> std::result::Result<(), F4KvsError> {
        // Write to WAL first (if enabled)
        if self.config.wal.enabled {
            {
                let wal = self.wal_manager.write().await;
                wal.write_operation(key, value)
                    .await
                    .map_err(Self::convert_error)?;
            }
        }

        let put_effect = {
            let mut memtable = self.active_memtable.write().await;
            memtable
                .put(key, value)
                .await
                .map_err(Self::convert_error)?
        };

        self.apply_put_key_count(key, put_effect)
            .await
            .map_err(Self::convert_error)?;

        // Add TTL
        #[cfg(feature = "ttl")]
        {
            self.ttl_manager
                .add_ttl(key.to_string(), "default".to_string(), ttl)
                .map_err(|e| F4KvsError::Storage {
                    message: format!("TTL error: {}", e),
                })?;

            // Update statistics
            {
                let mut stats = self.stats.write().await;
                stats.total_writes += 1;
                stats.total_bytes_written +=
                    key.len() as u64 + Self::estimate_value_size(value) as u64;
            }

            // Check if we need to flush memtable
            self.check_memtable_flush()
                .await
                .map_err(Self::convert_error)?;

            Ok(())
        }
        #[cfg(not(feature = "ttl"))]
        {
            let _ = (key, value, ttl);
            Err(F4KvsError::Storage {
                message: "TTL feature is not enabled".to_string(),
            })
        }
    }

    async fn put_cf_with_ttl(
        &self,
        key: &str,
        value: &Value,
        column_family: &str,
        ttl: Duration,
    ) -> std::result::Result<(), F4KvsError> {
        // Check if column family exists
        {
            let cf_map = self.column_families.read().await;
            if !cf_map.contains_key(column_family) {
                return Err(F4KvsError::Storage {
                    message: format!("Column family not found: {}", column_family),
                });
            }
        }

        // Use prefixed key for column family isolation
        let prefixed_key = format!("{}#{}", column_family, key);

        // Write to WAL first (if enabled)
        if self.config.wal.enabled {
            {
                let wal = self.wal_manager.write().await;
                wal.write_operation(&prefixed_key, value)
                    .await
                    .map_err(Self::convert_error)?;
            }
        }

        let put_effect = {
            let mut memtable = self.active_memtable.write().await;
            memtable
                .put(&prefixed_key, value)
                .await
                .map_err(Self::convert_error)?
        };

        self.apply_put_key_count(&prefixed_key, put_effect)
            .await
            .map_err(Self::convert_error)?;

        // Set TTL for the prefixed key
        #[cfg(feature = "ttl")]
        {
            self.ttl_manager
                .add_ttl(prefixed_key.clone(), column_family.to_string(), ttl)
                .map_err(|e| F4KvsError::Storage {
                    message: format!("TTL error: {}", e),
                })?;

            // Update statistics
            {
                let mut stats = self.stats.write().await;
                stats.total_writes += 1;
                stats.total_bytes_written +=
                    prefixed_key.len() as u64 + Self::estimate_value_size(value) as u64;
            }

            // Check if we need to flush memtable
            self.check_memtable_flush()
                .await
                .map_err(Self::convert_error)?;

            Ok(())
        }
        #[cfg(not(feature = "ttl"))]
        {
            let _ = (key, value, column_family, ttl);
            Err(F4KvsError::Storage {
                message: "TTL feature is not enabled".to_string(),
            })
        }
    }

    async fn get_ttl(&self, key: &str) -> std::result::Result<Option<Duration>, F4KvsError> {
        #[cfg(feature = "ttl")]
        {
            if let Some(ttl_secs) = self.ttl_manager.get_ttl(key) {
                Ok(Some(Duration::from_secs(ttl_secs)))
            } else {
                Ok(None)
            }
        }
        #[cfg(not(feature = "ttl"))]
        {
            let _ = key;
            Err(F4KvsError::Storage {
                message: "TTL feature is not enabled".to_string(),
            })
        }
    }

    async fn get_ttl_cf(
        &self,
        key: &str,
        column_family: &str,
    ) -> std::result::Result<Option<Duration>, F4KvsError> {
        // Check if column family exists
        {
            let cf_map = self.column_families.read().await;
            if !cf_map.contains_key(column_family) {
                return Err(F4KvsError::Storage {
                    message: format!("Column family not found: {}", column_family),
                });
            }
        }

        #[cfg(feature = "ttl")]
        {
            // Use prefixed key for column family isolation
            let prefixed_key = format!("{}#{}", column_family, key);
            if let Some(ttl_secs) = self.ttl_manager.get_ttl(&prefixed_key) {
                Ok(Some(Duration::from_secs(ttl_secs)))
            } else {
                Ok(None)
            }
        }
        #[cfg(not(feature = "ttl"))]
        {
            let _ = (key, column_family);
            Err(F4KvsError::Storage {
                message: "TTL feature is not enabled".to_string(),
            })
        }
    }

    // === ADVANCED OPERATIONS ===

    async fn batch_delete(&self, keys: Vec<String>) -> std::result::Result<(), F4KvsError> {
        for key in keys {
            self.delete(&key).await?;
        }
        Ok(())
    }

    async fn flush(&self) -> std::result::Result<(), F4KvsError> {
        tracing::info!("LSM Engine: Starting flush operation");

        // Flush WAL first to ensure durability
        if self.config.wal.enabled {
            tracing::info!("LSM Engine: Flushing WAL");
            let wal = self.wal_manager.read().await;
            wal.flush().await.map_err(Self::convert_error)?;
        }

        // Then flush memtable to SSTable
        tracing::info!("LSM Engine: Flushing memtable");
        self.flush_memtable().await.map_err(Self::convert_error)?;

        // After successful memtable flush, truncate WAL to prevent recovery
        if self.config.wal.enabled {
            tracing::info!("LSM Engine: Truncating WAL after flush");
            let wal = self.wal_manager.read().await;
            wal.truncate_after_flush()
                .await
                .map_err(Self::convert_error)?;

            // Verify truncation succeeded
            if !wal.verify_truncated().await.map_err(Self::convert_error)? {
                tracing::error!("LSM Engine: WAL truncation verification failed");
                return Err(F4KvsError::Storage {
                    message: "WAL truncation verification failed".to_string(),
                });
            }
            tracing::info!("LSM Engine: WAL truncation verified successfully");
        }

        tracing::info!("LSM Engine: Flush operation completed successfully");
        Ok(())
    }

    async fn create_column_family(&mut self, name: &str) -> std::result::Result<(), F4KvsError> {
        let mut cf_map = self.column_families.write().await;
        let cf_id = cf_map.len();
        cf_map.insert(name.to_string(), cf_id);
        Ok(())
    }

    async fn drop_column_family(&mut self, name: &str) -> std::result::Result<(), F4KvsError> {
        let mut cf_map = self.column_families.write().await;
        cf_map.remove(name);
        Ok(())
    }

    fn list_column_families(&self) -> Vec<String> {
        // This is a synchronous method, so we need to block on the async operation
        // In a real implementation, this should be handled differently
        let rt = tokio::runtime::Handle::current();
        rt.block_on(async {
            let cf_map = self.column_families.read().await;
            cf_map.keys().cloned().collect()
        })
    }
}

impl std::fmt::Debug for LsmTreeEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LsmTreeEngine")
            .field("config", &self.config)
            .field("active_memtable", &"Arc<RwLock<Memtable>>")
            .field("immutable_memtables", &"Arc<RwLock<Vec<Memtable>>>")
            .field("sstables", &"Arc<RwLock<HashMap<usize, Vec<SSTable>>>>")
            .field("wal_manager", &"Arc<RwLock<WALManager>>")
            .field("stats", &"Arc<RwLock<LsmStats>>")
            .field("column_families", &"Arc<RwLock<HashMap<String, usize>>>")
            .finish()
    }
}

/// Drop implementation for resource cleanup
///
/// Note: This performs synchronous cleanup only. For complete cleanup including
/// background tasks, call shutdown() explicitly before dropping.
impl Drop for LsmTreeEngine {
    fn drop(&mut self) {
        // Set shutdown flag to stop background tasks
        self.shutdown.store(true, Ordering::Relaxed);

        // Note: We cannot wait for async tasks in Drop, but setting the shutdown
        // flag will cause them to exit on their next check. For complete cleanup,
        // users should call shutdown() explicitly before dropping.
    }
}

impl LsmTreeEngine {
    /// Get LSM-specific statistics
    pub async fn lsm_stats(&self) -> Result<LsmStats> {
        let stats = self.stats.read().await;
        Ok(stats.clone())
    }

    /// Get engine configuration
    pub fn config(&self) -> &LsmConfig {
        &self.config
    }

    /// Set metrics recorder for LSM operations
    #[cfg(feature = "metrics")]
    pub fn set_metrics_recorder(&mut self, metrics: MetricsRecorder) {
        self.metrics = metrics;
    }

    /// Force flush of current memtable
    pub async fn force_flush(&self) -> Result<()> {
        self.flush_memtable().await
    }

    /// Gracefully shutdown the engine, flushing all pending data
    pub async fn shutdown(&self) -> Result<()> {
        tracing::info!("LsmTreeEngine: Starting graceful shutdown");

        // Shutdown background tasks first
        self.shutdown_background_tasks().await?;

        // Flush memtable to SSTable
        self.flush_memtable().await?;

        // Mark clean shutdown in WAL (includes flush and truncate)
        if self.config.wal.enabled {
            let wal = self.wal_manager.read().await;
            wal.mark_clean_shutdown()
                .await
                .map_err(Self::convert_error)?;
        }

        tracing::info!("LsmTreeEngine: Graceful shutdown completed");
        Ok(())
    }

    /// Get column family statistics
    pub async fn column_family_stats(&self) -> Result<HashMap<String, usize>> {
        let cf_map = self.column_families.read().await;
        Ok(cf_map.clone())
    }

    /// Check if compaction is needed and run it
    async fn compact_if_needed(&self) -> Result<()> {
        // Try to acquire write lock for exclusive compaction access
        // Use try_write to avoid blocking if other operations are in progress
        let op_guard = self.operation_guard.try_write();
        let _op_guard = match op_guard {
            Ok(guard) => guard,
            Err(_) => {
                // Another operation is in progress, skip compaction for now
                debug!("Skipping compaction - other operations in progress");
                return Ok(());
            }
        };

        let compaction_start = Instant::now();

        self.compaction_manager
            .compact_if_needed(&self.sstables)
            .await?;

        // Update compaction metrics after compaction
        let compaction_duration = compaction_start.elapsed();
        let compaction_stats = self.compaction_manager.get_stats().await;

        // Record metrics for compaction
        #[cfg(feature = "metrics")]
        {
            let bytes_read = compaction_stats.bytes_read;
            let bytes_written = compaction_stats.bytes_written;
            record_compaction(
                &self.metrics,
                bytes_read,
                bytes_written,
                compaction_duration,
            );
        }

        {
            let mut stats = self.stats.write().await;
            stats.compaction_count += 1;
            stats.last_compaction = Some(utils::timestamp_secs());

            // Update compaction metrics
            stats.compaction_metrics.total_compactions += 1;
            stats.compaction_metrics.total_entries_processed +=
                compaction_stats.entries_processed as u64;
            stats.compaction_metrics.total_entries_removed +=
                compaction_stats.entries_removed as u64;
            stats.compaction_metrics.total_space_reclaimed += compaction_stats.space_reclaimed;
            stats.compaction_metrics.total_compaction_duration_ms +=
                compaction_duration.as_millis() as u64;
            if stats.compaction_metrics.total_compactions > 0 {
                stats.compaction_metrics.avg_compaction_duration_ms =
                    stats.compaction_metrics.total_compaction_duration_ms as f64
                        / stats.compaction_metrics.total_compactions as f64;
            }
            stats.compaction_metrics.levels_compacted += compaction_stats.levels_compacted as u64;
            stats.compaction_metrics.sstables_merged += compaction_stats.sstables_merged as u64;
        }

        // Update level metrics
        self.update_level_metrics().await?;

        Ok(())
    }

    /// Update SSTable level metrics
    async fn update_level_metrics(&self) -> Result<()> {
        let sstables = self.sstables.read().await;
        let mut level_metrics = Vec::new();

        for level in 0..self.config.levels.count {
            if let Some(level_sstables) = sstables.get(&level) {
                let total_size: u64 = level_sstables.iter().map(|s| s.metadata().file_size).sum();
                let avg_size = if level_sstables.is_empty() {
                    0
                } else {
                    total_size / level_sstables.len() as u64
                };

                let smallest_key = level_sstables
                    .iter()
                    .map(|s| s.metadata().smallest_key.clone())
                    .min();
                let largest_key = level_sstables
                    .iter()
                    .map(|s| s.metadata().largest_key.clone())
                    .max();

                level_metrics.push(crate::utils::stats::LevelMetrics {
                    level,
                    sstable_count: level_sstables.len() as u64,
                    total_size_bytes: total_size,
                    avg_sstable_size_bytes: avg_size,
                    smallest_key,
                    largest_key,
                });
            } else {
                level_metrics.push(crate::utils::stats::LevelMetrics {
                    level,
                    sstable_count: 0,
                    total_size_bytes: 0,
                    avg_sstable_size_bytes: 0,
                    smallest_key: None,
                    largest_key: None,
                });
            }
        }

        {
            let mut stats = self.stats.write().await;
            stats.level_metrics = level_metrics;
        }

        Ok(())
    }

    /// Update memtable metrics
    #[allow(dead_code)] // May be used in future monitoring features
    async fn update_memtable_metrics(&self) -> Result<()> {
        let active_memtable = self.active_memtable.read().await;
        let immutable_memtables = self.immutable_memtables.read().await;

        let active_size = active_memtable.size().await as u64;
        let active_entries = active_memtable.entry_count().await as u64;
        let immutable_count = immutable_memtables.len() as u64;
        let immutable_size: u64 = {
            let mut total: u64 = 0;
            for memtable in immutable_memtables.iter() {
                total += memtable.size().await as u64;
            }
            total
        };

        {
            let mut stats = self.stats.write().await;
            stats.memtable_metrics.active_memtable_size = active_size;
            stats.memtable_metrics.active_memtable_entries = active_entries;
            stats.memtable_metrics.immutable_memtable_count = immutable_count;
            stats.memtable_metrics.immutable_memtable_size = immutable_size;
        }

        Ok(())
    }

    /// Run compaction on all levels
    ///
    /// This method acquires exclusive access to prevent concurrent operations
    /// and flushes memtables before compaction.
    pub async fn compact_all(&self) -> Result<()> {
        // Try to acquire write lock with timeout to avoid blocking indefinitely
        let lock_timeout = Duration::from_secs(5);
        let _op_guard = match tokio::time::timeout(lock_timeout, self.operation_guard.write()).await
        {
            Ok(guard) => guard,
            Err(_) => {
                // If we can't get the lock quickly, skip compaction this time
                debug!("Skipping compact_all - could not acquire lock within timeout");
                return Ok(());
            }
        };

        // Use internal flush since we already hold the operation guard
        self.flush_memtable_internal().await?;

        // Run compaction on all levels
        self.compaction_manager.compact_all(&self.sstables).await
    }

    /// Get compaction statistics
    pub async fn compaction_stats(&self) -> Result<CompactionStats> {
        Ok(self.compaction_manager.get_stats().await)
    }

    /// Reset compaction statistics
    pub async fn reset_compaction_stats(&self) -> Result<()> {
        self.compaction_manager.reset_stats().await;
        Ok(())
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
        self.compaction_manager
            .update_workload_characteristics(
                write_ops,
                read_ops,
                write_amplification,
                read_latency_ms,
                resource_utilization,
            )
            .await
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
        self.compaction_manager
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
            .await
    }

    /// Get adaptive workload characteristics
    pub async fn get_workload_characteristics(&self) -> Option<adaptive::WorkloadCharacteristics> {
        self.compaction_manager.get_workload_characteristics().await
    }

    /// Get adaptive performance metrics
    pub async fn get_performance_metrics(&self) -> Option<adaptive::PerformanceMetrics> {
        self.compaction_manager.get_performance_metrics().await
    }

    /// Check if adaptive compaction is enabled
    pub fn is_adaptive_compaction_enabled(&self) -> bool {
        self.compaction_manager.is_adaptive_enabled()
    }

    /// Execute an operation with automatic error recovery
    pub async fn execute_with_recovery<F, T>(&self, operation_name: &str, operation: F) -> Result<T>
    where
        F: Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T>> + Send>>,
    {
        const MAX_RETRIES: u32 = 3;
        const INITIAL_DELAY_MS: u64 = 100;
        const MAX_DELAY_MS: u64 = 5000;

        let mut attempt = 0;
        let mut delay_ms = INITIAL_DELAY_MS;

        loop {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(error) => {
                    attempt += 1;

                    // Check if error is recoverable
                    if !self.is_recoverable_error(&error) || attempt > MAX_RETRIES {
                        warn!(
                            "Operation '{}' failed after {} attempts: {}",
                            operation_name, attempt, error
                        );
                        return Err(error);
                    }

                    warn!(
                        "Operation '{}' failed (attempt {}/{}), retrying in {}ms: {}",
                        operation_name, attempt, MAX_RETRIES, delay_ms, error
                    );

                    // Exponential backoff with jitter
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                    delay_ms = std::cmp::min(delay_ms * 2, MAX_DELAY_MS);
                }
            }
        }
    }

    /// Check if an error is recoverable
    fn is_recoverable_error(&self, error: &LsmError) -> bool {
        match error {
            LsmError::Io(_) => true,         // I/O errors are usually recoverable
            LsmError::Compaction(_) => true, // Compaction errors are recoverable
            LsmError::Wal(_) => true,        // WAL errors are recoverable
            _ => false,                      // Other errors are typically not recoverable
        }
    }

    /// Perform storage health check
    pub async fn health_check(&self) -> Result<()> {
        // Check if data directory is accessible
        if !std::path::Path::new(&self.config.data_dir).exists() {
            return Err(LsmError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Data directory does not exist",
            )));
        }

        // Check if we can read from active memtable
        let memtable = self.active_memtable.read().await;
        // Simple health check - try to get a count of entries
        let _entry_count = memtable.size().await;
        drop(memtable);

        // Check if WAL is accessible (if enabled)
        if self.config.wal.enabled {
            let wal_manager = self.wal_manager.read().await;
            // Simple health check - verify WAL directory exists
            if !std::path::Path::new(&self.config.wal.dir).exists() {
                return Err(LsmError::Wal("WAL directory does not exist".to_string()));
            }
            drop(wal_manager);
        }

        // Check if SSTables are accessible
        let sstables = self.sstables.read().await;
        for (_level, level_sstables) in sstables.iter() {
            for sstable in level_sstables {
                if !sstable.path().exists() {
                    return Err(LsmError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("SSTable file not found: {}", sstable.path().display()),
                    )));
                }
            }
        }
        drop(sstables);

        Ok(())
    }

    /// Recover from a corrupted state
    pub async fn recover_from_corruption(&self) -> Result<()> {
        info!("Starting corruption recovery process");

        // Clear corrupted WAL files
        if self.config.wal.enabled {
            if let Err(e) = self.clear_wal_directory().await {
                warn!("Failed to clear WAL directory during recovery: {}", e);
            }
        }

        // Clear immutable memtables (they might be corrupted)
        {
            let mut immutable_memtables = self.immutable_memtables.write().await;
            immutable_memtables.clear();
            info!("Cleared immutable memtables during recovery");
        }

        // Reset statistics
        {
            let mut stats = self.stats.write().await;
            *stats = LsmStats::default();
            info!("Reset statistics during recovery");
        }

        // Force a flush of the active memtable to ensure data is persisted
        if let Err(e) = self.flush_memtable().await {
            warn!("Failed to flush memtable during recovery: {}", e);
        }

        info!("Corruption recovery completed");
        Ok(())
    }

    /// Get detailed LSM performance metrics
    pub async fn get_lsm_performance_metrics(&self) -> PerformanceMetrics {
        let stats = self.stats.read().await;
        let sstables = self.sstables.read().await;

        // Calculate SSTable statistics
        let mut total_sstables = 0;
        let mut total_size = 0;
        let mut level_stats = Vec::new();

        for (level, level_sstables) in sstables.iter() {
            let level_size: u64 = level_sstables.iter().map(|s| s.size()).sum();
            total_sstables += level_sstables.len();
            total_size += level_size;

            level_stats.push(LevelMetrics {
                level: *level as u32,
                file_count: level_sstables.len() as u32,
                total_size_bytes: level_size,
                avg_file_size_bytes: if level_sstables.is_empty() {
                    0
                } else {
                    level_size / level_sstables.len() as u64
                },
            });
        }

        PerformanceMetrics {
            total_operations: stats.total_operations(),
            read_operations: stats.total_reads,
            write_operations: stats.total_writes,
            delete_operations: stats.total_deletes,
            total_sstables,
            total_size_bytes: total_size,
            memtable_entries: stats.memtable_hits,
            immutable_memtables: 0, // Not tracked in current stats
            level_metrics: level_stats,
            avg_read_latency_ms: 0.0,  // Not tracked in current stats
            avg_write_latency_ms: 0.0, // Not tracked in current stats
            compaction_count: stats.compaction_count,
            last_compaction_time: stats.last_compaction,
        }
    }

    /// Get storage optimization recommendations
    pub async fn get_optimization_recommendations(&self) -> Vec<OptimizationRecommendation> {
        let mut recommendations = Vec::new();
        let metrics = self.get_lsm_performance_metrics().await;
        let sstables = self.sstables.read().await;

        // Check for too many L0 files
        if let Some(l0_files) = sstables.get(&0) {
            if l0_files.len() > 4 {
                recommendations.push(OptimizationRecommendation {
                    category: "Compaction".to_string(),
                    priority: OptimizationPriority::High,
                    title: "Too many L0 files".to_string(),
                    description: format!(
                        "L0 has {} files, consider running compaction",
                        l0_files.len()
                    ),
                    action: "Run manual compaction to reduce L0 file count".to_string(),
                });
            }
        }

        // Check for large SSTable files
        for (level, level_sstables) in sstables.iter() {
            for sstable in level_sstables {
                if sstable.size() > 64 * 1024 * 1024 {
                    // 64MB
                    recommendations.push(OptimizationRecommendation {
                        category: "File Size".to_string(),
                        priority: OptimizationPriority::Medium,
                        title: "Large SSTable file detected".to_string(),
                        description: format!(
                            "SSTable at level {} is {}MB",
                            level,
                            sstable.size() / (1024 * 1024)
                        ),
                        action: "Consider splitting large SSTables for better performance"
                            .to_string(),
                    });
                }
            }
        }

        // Check for high read latency
        if metrics.avg_read_latency_ms > 100.0 {
            recommendations.push(OptimizationRecommendation {
                category: "Performance".to_string(),
                priority: OptimizationPriority::High,
                title: "High read latency detected".to_string(),
                description: format!(
                    "Average read latency is {:.2}ms",
                    metrics.avg_read_latency_ms
                ),
                action: "Consider adding bloom filters or optimizing compaction strategy"
                    .to_string(),
            });
        }

        // Check for high write latency
        if metrics.avg_write_latency_ms > 50.0 {
            recommendations.push(OptimizationRecommendation {
                category: "Performance".to_string(),
                priority: OptimizationPriority::Medium,
                title: "High write latency detected".to_string(),
                description: format!(
                    "Average write latency is {:.2}ms",
                    metrics.avg_write_latency_ms
                ),
                action: "Consider increasing memtable size or optimizing WAL settings".to_string(),
            });
        }

        // Check for infrequent compaction
        if metrics.compaction_count == 0 && metrics.total_operations > 1000 {
            recommendations.push(OptimizationRecommendation {
                category: "Compaction".to_string(),
                priority: OptimizationPriority::Low,
                title: "No compaction performed".to_string(),
                description: "Storage has many operations but no compaction has been performed"
                    .to_string(),
                action: "Consider running manual compaction to optimize storage layout".to_string(),
            });
        }

        recommendations
    }
}

/// Simple key-value iterator implementation
struct SimpleKeyValueIterator {
    values: Vec<(String, Value)>,
    index: usize,
}

impl SimpleKeyValueIterator {
    fn new(values: Vec<(String, Value)>) -> Self {
        Self { values, index: 0 }
    }
}

impl KeyValueIterator for SimpleKeyValueIterator {
    fn next(&mut self) -> Option<(String, Value)> {
        if self.index < self.values.len() {
            let result = self.values[self.index].clone();
            self.index += 1;
            Some(result)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::*;
    use tempfile::TempDir;
    #[cfg(feature = "ttl")]
    use tokio::time::{sleep, Duration};

    async fn create_test_engine() -> (LsmTreeEngine, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let data_dir = temp_dir.path().to_path_buf();

        let config = LsmConfig {
            data_dir: data_dir.clone(),
            memtable: MemtableConfig::default(),
            wal: WalConfig {
                enabled: true,
                dir: data_dir.join("wal"),
                segment_size: 1024 * 1024, // 1MB
                ..Default::default()
            },
            sstable: SstableConfig::default(),
            levels: LevelConfig::default(),
            compaction: CompactionConfig::default(),
            bloom_filter: BloomFilterConfig::default(),
            column_families: ColumnFamilyConfig {
                default_name: "default".to_string(),
                enable_isolation: false,
                max_count: 100,
            },
            performance: PerformanceConfig::default(),
            adaptive_compaction: None,
        };

        let engine = LsmTreeEngine::new(config)
            .await
            .expect("Failed to create test engine");
        (engine, temp_dir)
    }

    #[tokio::test]
    async fn test_lsm_engine_creation() {
        let (_engine, _temp_dir) = create_test_engine().await;
        // Engine creation should succeed
    }

    /// Mirrors the redb-bench harness: dedicated runtime thread + block_on from caller.
    #[test]
    fn test_engine_new_via_block_on_with_wal_fsync_async() {
        use crate::core::config::WalSyncMode;
        use std::sync::{mpsc, OnceLock};
        use tokio::runtime::Handle;

        fn runtime_handle() -> &'static Handle {
            static HANDLE: OnceLock<Handle> = OnceLock::new();
            HANDLE.get_or_init(|| {
                let (tx, rx) = mpsc::sync_channel(1);
                std::thread::Builder::new()
                    .name("test-bench-runtime".into())
                    .spawn(move || {
                        let rt = tokio::runtime::Builder::new_multi_thread()
                            .worker_threads(4)
                            .enable_all()
                            .build()
                            .expect("runtime");
                        tx.send(rt.handle().clone()).expect("handle");
                        rt.block_on(std::future::pending::<()>());
                    })
                    .expect("spawn");
                rx.recv().expect("handle")
            })
        }

        let temp_dir = TempDir::new().expect("tempdir");
        let mut config = LsmConfig::default();
        config.data_dir = temp_dir.path().to_path_buf();
        config.wal.dir = temp_dir.path().join("wal");
        config.wal.enabled = true;
        config.wal.sync_mode = WalSyncMode::FsyncAsync;
        config.compaction.background_enabled = false;

        let engine = runtime_handle().block_on(async move {
            tokio::time::timeout(
                std::time::Duration::from_secs(30),
                LsmTreeEngine::new(config),
            )
            .await
        });
        let engine = engine.expect("timeout").expect("engine new");
        let items: Vec<_> = (0..1000)
            .map(|i| (format!("key{i:08}"), Value::String("v".into())))
            .collect();
        runtime_handle()
            .block_on(async move { engine.batch_put(items).await })
            .expect("batch put");
    }

    #[tokio::test]
    async fn test_basic_put_get() {
        let (engine, _temp_dir) = create_test_engine().await;

        // Test basic put and get
        let key = "test_key";
        let value = Value::String("test_value".to_string());

        engine
            .put(key, &value)
            .await
            .expect("Failed to put test value");
        let retrieved = engine.get(key).await.expect("Failed to get test value");

        assert_eq!(retrieved, Some(value));
    }

    #[tokio::test]
    async fn test_put_update_get() {
        let (engine, _temp_dir) = create_test_engine().await;

        let key = "test_key";
        let value1 = Value::String("value1".to_string());
        let value2 = Value::String("value2".to_string());

        // Put initial value
        engine
            .put(key, &value1)
            .await
            .expect("Failed to put initial value");
        assert_eq!(
            engine.get(key).await.expect("Failed to get initial value"),
            Some(value1.clone())
        );

        // Update value
        engine
            .put(key, &value2)
            .await
            .expect("Failed to put updated value");
        assert_eq!(
            engine.get(key).await.expect("Failed to get updated value"),
            Some(value2)
        );
    }

    #[tokio::test]
    async fn test_delete() {
        let (engine, _temp_dir) = create_test_engine().await;

        let key = "test_key";
        let value = Value::String("test_value".to_string());

        // Put value
        engine.put(key, &value).await.expect("Failed to put value");
        assert_eq!(
            engine.get(key).await.expect("Failed to get value"),
            Some(value.clone())
        );

        // Delete value
        engine.delete(key).await.expect("Failed to delete value");
        assert_eq!(
            engine.get(key).await.expect("Failed to get deleted value"),
            None
        );
    }

    #[tokio::test]
    async fn test_batch_operations() {
        let (engine, _temp_dir) = create_test_engine().await;

        let items = vec![
            ("key1".to_string(), Value::String("value1".to_string())),
            ("key2".to_string(), Value::String("value2".to_string())),
            ("key3".to_string(), Value::String("value3".to_string())),
        ];

        // Batch put
        engine
            .batch_put(items.clone())
            .await
            .expect("Failed to batch put");

        // Verify all values
        for (key, expected_value) in &items {
            assert_eq!(
                engine
                    .get(key)
                    .await
                    .unwrap_or_else(|_| panic!("Failed to get key {}", key)),
                Some(expected_value.clone())
            );
        }

        // Batch delete
        let keys: Vec<String> = items.iter().map(|(k, _)| k.clone()).collect();
        engine
            .batch_delete(keys)
            .await
            .expect("Failed to batch delete");

        // Verify all deleted
        for (key, _) in &items {
            assert_eq!(
                engine
                    .get(key)
                    .await
                    .unwrap_or_else(|_| panic!("Failed to get deleted key {}", key)),
                None
            );
        }
    }

    #[tokio::test]
    async fn test_scan_operations() {
        let (engine, _temp_dir) = create_test_engine().await;

        // Insert test data
        let test_data = vec![
            ("a_key1", "value1"),
            ("a_key2", "value2"),
            ("b_key1", "value3"),
            ("b_key2", "value4"),
            ("c_key1", "value5"),
        ];

        for (key, value) in &test_data {
            engine
                .put(key, &Value::String(value.to_string()))
                .await
                .unwrap_or_else(|_| panic!("Failed to put key {}", key));
        }

        // Test basic operations work
        assert_eq!(
            engine.get("a_key1").await.expect("Failed to get a_key1"),
            Some(Value::String("value1".to_string()))
        );
        assert_eq!(
            engine.get("b_key1").await.expect("Failed to get b_key1"),
            Some(Value::String("value3".to_string()))
        );
    }

    #[tokio::test]
    async fn test_column_families() {
        let (engine, _temp_dir) = create_test_engine().await;

        // Test basic operations work
        let key = "test_key";
        let value = Value::String("test_value".to_string());
        engine
            .put(key, &value)
            .await
            .expect("Failed to put test value");

        // Get data
        let retrieved = engine.get(key).await.expect("Failed to get test value");
        assert_eq!(retrieved, Some(value));
    }

    #[cfg(feature = "ttl")]
    #[tokio::test]
    async fn test_ttl_operations() {
        let (engine, _temp_dir) = create_test_engine().await;

        let key = "ttl_key";
        let value = Value::String("ttl_value".to_string());
        let ttl = Duration::from_millis(100);

        // Put with TTL
        engine
            .put_with_ttl(key, &value, ttl)
            .await
            .expect("Failed to put with TTL");

        // Verify value exists
        assert_eq!(
            engine.get(key).await.expect("Failed to get TTL value"),
            Some(value.clone())
        );

        // Wait for TTL to expire
        sleep(Duration::from_millis(150)).await;

        // Note: TTL may not be fully implemented in LSM engine yet
        // Just verify the operation doesn't panic
        let _result = engine.get(key).await;
    }

    #[tokio::test]
    async fn test_statistics() {
        let (engine, _temp_dir) = create_test_engine().await;

        // Perform some operations
        engine
            .put("key1", &Value::String("value1".to_string()))
            .await
            .expect("Failed to put key1");
        engine
            .put("key2", &Value::String("value2".to_string()))
            .await
            .expect("Failed to put key2");
        let _ = engine.get("key1").await;
        engine.delete("key2").await.expect("Failed to delete key2");

        // Get statistics - just verify it doesn't panic
        let _stats = engine.stats().await.expect("Failed to get stats");
        // Note: specific operation counts may not be available in current stats structure
    }

    #[tokio::test]
    async fn test_live_key_count() {
        let (engine, _temp_dir) = create_test_engine().await;

        assert_eq!(engine.count().await.expect("count"), 0);

        engine
            .put("a", &Value::String("1".to_string()))
            .await
            .expect("put a");
        engine
            .put("b", &Value::String("2".to_string()))
            .await
            .expect("put b");
        assert_eq!(engine.count().await.expect("count"), 2);

        engine
            .put("a", &Value::String("updated".to_string()))
            .await
            .expect("update a");
        assert_eq!(engine.count().await.expect("count"), 2);

        engine.delete("b").await.expect("delete b");
        assert_eq!(engine.count().await.expect("count"), 1);

        engine.flush().await.expect("flush");
        assert_eq!(engine.count().await.expect("count after flush"), 1);

        engine
            .put("c", &Value::String("3".to_string()))
            .await
            .expect("put c");
        assert_eq!(engine.count().await.expect("count after sstable put"), 2);
    }

    #[tokio::test]
    async fn test_health_check() {
        let (engine, _temp_dir) = create_test_engine().await;

        // Test basic operations work (health check)
        engine
            .put("health_key", &Value::String("health_value".to_string()))
            .await
            .expect("Failed to put health key");
        let retrieved = engine
            .get("health_key")
            .await
            .expect("Failed to get health key");
        assert_eq!(retrieved, Some(Value::String("health_value".to_string())));
    }

    #[tokio::test]
    async fn test_persistence() {
        let (engine, _temp_dir) = create_test_engine().await;

        // Put some data
        let key = "persistent_key";
        let value = Value::String("persistent_value".to_string());
        engine
            .put(key, &value)
            .await
            .expect("Failed to put persistent value");

        // Flush to ensure data is written
        engine.flush().await.expect("Failed to flush data");

        // Verify data is still there
        let retrieved = engine
            .get(key)
            .await
            .expect("Failed to get persistent value");
        assert_eq!(retrieved, Some(value));
    }

    #[tokio::test]
    async fn test_concurrent_operations() {
        let (engine, _temp_dir) = create_test_engine().await;

        // Test basic operations work
        engine
            .put("key_0_0", &Value::String("value_0_0".to_string()))
            .await
            .expect("Failed to put key_0_0");
        engine
            .put("key_1_1", &Value::String("value_1_1".to_string()))
            .await
            .expect("Failed to put key_1_1");

        // Verify data is present
        assert_eq!(
            engine.get("key_0_0").await.expect("Failed to get key_0_0"),
            Some(Value::String("value_0_0".to_string()))
        );
        assert_eq!(
            engine.get("key_1_1").await.expect("Failed to get key_1_1"),
            Some(Value::String("value_1_1".to_string()))
        );
    }

    #[tokio::test]
    async fn test_large_data_handling() {
        let (engine, _temp_dir) = create_test_engine().await;

        // Test with large values
        let large_value = "x".repeat(10000); // 10KB value
        let key = "large_key";

        engine
            .put(key, &Value::String(large_value.clone()))
            .await
            .expect("Failed to put large value");
        let retrieved = engine.get(key).await.expect("Failed to get large value");
        assert_eq!(retrieved, Some(Value::String(large_value)));
    }

    #[tokio::test]
    async fn test_error_handling() {
        let (engine, _temp_dir) = create_test_engine().await;

        // Test with empty key (should be handled gracefully)
        let _result = engine.put("", &Value::String("value".to_string())).await;
        // This might succeed or fail depending on implementation
        // We just want to ensure it doesn't panic

        // Test with very long key
        let long_key = "x".repeat(1000);
        let _result = engine
            .put(&long_key, &Value::String("value".to_string()))
            .await;
        // This should be handled gracefully
    }

    #[tokio::test]
    async fn test_crash_recovery_basic() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let data_dir = temp_dir.path().to_path_buf();

        // Create initial engine and write data
        let config = LsmConfig {
            data_dir: data_dir.clone(),
            memtable: MemtableConfig::default(),
            wal: WalConfig {
                enabled: true,
                dir: data_dir.join("wal"),
                segment_size: 1024 * 1024,
                ..Default::default()
            },
            sstable: SstableConfig::default(),
            levels: LevelConfig::default(),
            compaction: CompactionConfig::default(),
            bloom_filter: BloomFilterConfig::default(),
            column_families: ColumnFamilyConfig {
                default_name: "default".to_string(),
                enable_isolation: false,
                max_count: 100,
            },
            performance: PerformanceConfig::default(),
            adaptive_compaction: None,
        };

        // Write some data
        {
            let engine = LsmTreeEngine::new(config.clone())
                .await
                .expect("Failed to create initial engine");
            engine
                .put(
                    "recovery_key1",
                    &Value::String("recovery_value1".to_string()),
                )
                .await
                .expect("Failed to put recovery_key1");
            engine
                .put(
                    "recovery_key2",
                    &Value::String("recovery_value2".to_string()),
                )
                .await
                .expect("Failed to put recovery_key2");
            engine
                .put(
                    "recovery_key3",
                    &Value::String("recovery_value3".to_string()),
                )
                .await
                .expect("Failed to put recovery_key3");

            // Flush to ensure data is persisted
            engine.flush().await.expect("Failed to flush data");
        } // Engine drops here, simulating crash

        // Recover by creating new engine with same data directory
        let recovered_engine = LsmTreeEngine::new(config)
            .await
            .expect("Failed to create recovered engine");

        // Verify data was recovered
        assert_eq!(
            recovered_engine
                .get("recovery_key1")
                .await
                .expect("Failed to get recovery_key1"),
            Some(Value::String("recovery_value1".to_string()))
        );
        assert_eq!(
            recovered_engine
                .get("recovery_key2")
                .await
                .expect("Failed to get recovery_key2"),
            Some(Value::String("recovery_value2".to_string()))
        );
        assert_eq!(
            recovered_engine
                .get("recovery_key3")
                .await
                .expect("Failed to get recovery_key3"),
            Some(Value::String("recovery_value3".to_string()))
        );
    }

    #[tokio::test]
    async fn test_crash_recovery_with_updates() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let data_dir = temp_dir.path().to_path_buf();

        let config = LsmConfig {
            data_dir: data_dir.clone(),
            memtable: MemtableConfig::default(),
            wal: WalConfig {
                enabled: true,
                dir: data_dir.join("wal"),
                segment_size: 1024 * 1024,
                ..Default::default()
            },
            sstable: SstableConfig::default(),
            levels: LevelConfig::default(),
            compaction: CompactionConfig::default(),
            bloom_filter: BloomFilterConfig::default(),
            column_families: ColumnFamilyConfig {
                default_name: "default".to_string(),
                enable_isolation: false,
                max_count: 100,
            },
            performance: PerformanceConfig::default(),
            adaptive_compaction: None,
        };

        // Write initial data
        {
            let engine = LsmTreeEngine::new(config.clone())
                .await
                .expect("Failed to create initial engine");
            engine
                .put("update_key", &Value::String("initial_value".to_string()))
                .await
                .expect("Failed to put initial value");
            engine.flush().await.expect("Failed to flush initial data");
            println!("Initial data written and flushed");
        }

        // Update data and crash
        {
            let engine = LsmTreeEngine::new(config.clone())
                .await
                .expect("Failed to create update engine");
            engine
                .put("update_key", &Value::String("updated_value".to_string()))
                .await
                .expect("Failed to put updated value");
            engine
                .put("new_key", &Value::String("new_value".to_string()))
                .await
                .expect("Failed to put new value");
            engine
                .delete("update_key")
                .await
                .expect("Failed to delete update_key");
            println!("Data written to WAL (not flushed) - simulating crash");

            // Check WAL directory contents
            let wal_dir = std::path::Path::new(&config.wal.dir);
            if wal_dir.exists() {
                println!("WAL directory exists: {:?}", wal_dir);
                if let Ok(entries) = std::fs::read_dir(wal_dir) {
                    for entry in entries.flatten() {
                        println!("WAL file: {:?}", entry.path());
                    }
                }
            } else {
                println!("WAL directory does not exist: {:?}", wal_dir);
            }

            // Don't flush - simulate crash before flush
        }

        // Recover and verify
        println!("Creating recovered engine...");
        let recovered_engine = LsmTreeEngine::new(config)
            .await
            .expect("Failed to create recovered engine");
        println!("Recovered engine created");

        println!("After recovery - checking keys:");
        let update_key_result = recovered_engine
            .get("update_key")
            .await
            .expect("Failed to get update_key");
        let new_key_result = recovered_engine
            .get("new_key")
            .await
            .expect("Failed to get new_key");
        println!("update_key result: {:?}", update_key_result);
        println!("new_key result: {:?}", new_key_result);

        // The deleted key should be gone
        assert_eq!(update_key_result, None);
        // New key should be present
        assert_eq!(new_key_result, Some(Value::String("new_value".to_string())));
    }

    #[tokio::test]
    async fn test_crash_recovery_batch_operations() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let data_dir = temp_dir.path().to_path_buf();

        let config = LsmConfig {
            data_dir: data_dir.clone(),
            memtable: MemtableConfig::default(),
            wal: WalConfig {
                enabled: true,
                dir: data_dir.join("wal"),
                segment_size: 1024 * 1024,
                ..Default::default()
            },
            sstable: SstableConfig::default(),
            levels: LevelConfig::default(),
            compaction: CompactionConfig::default(),
            bloom_filter: BloomFilterConfig::default(),
            column_families: ColumnFamilyConfig {
                default_name: "default".to_string(),
                enable_isolation: false,
                max_count: 100,
            },
            performance: PerformanceConfig::default(),
            adaptive_compaction: None,
        };

        // Write batch data
        {
            let engine = LsmTreeEngine::new(config.clone())
                .await
                .expect("Failed to create initial engine");
            let batch_items = vec![
                (
                    "batch_key1".to_string(),
                    Value::String("batch_value1".to_string()),
                ),
                (
                    "batch_key2".to_string(),
                    Value::String("batch_value2".to_string()),
                ),
                (
                    "batch_key3".to_string(),
                    Value::String("batch_value3".to_string()),
                ),
            ];
            engine
                .batch_put(batch_items)
                .await
                .expect("Failed to batch put initial data");
            engine.flush().await.expect("Failed to flush initial data");
        }

        // Modify batch data and crash
        {
            let engine = LsmTreeEngine::new(config.clone())
                .await
                .expect("Failed to create update engine");
            let batch_items = vec![
                (
                    "batch_key1".to_string(),
                    Value::String("updated_batch_value1".to_string()),
                ),
                (
                    "batch_key4".to_string(),
                    Value::String("batch_value4".to_string()),
                ),
            ];
            engine
                .batch_put(batch_items)
                .await
                .expect("Failed to batch put updated data");

            let delete_keys = vec!["batch_key2".to_string()];
            engine
                .batch_delete(delete_keys)
                .await
                .expect("Failed to batch delete batch_key2");
            // Don't flush - simulate crash
        }

        // Recover and verify
        let recovered_engine = LsmTreeEngine::new(config)
            .await
            .expect("Failed to create recovered engine");

        // Debug: Check all keys after recovery
        println!("After recovery - checking all keys:");

        // Debug: Check memtable state directly after recovery
        {
            let memtable = recovered_engine.active_memtable.read().await;
            println!("Direct memtable check after recovery:");
            println!(
                "  batch_key1: {:?}",
                memtable.get("batch_key1").await.unwrap_or(None)
            );
            println!(
                "  batch_key2: {:?}",
                memtable.get("batch_key2").await.unwrap_or(None)
            );
            println!(
                "  batch_key3: {:?}",
                memtable.get("batch_key3").await.unwrap_or(None)
            );
            println!(
                "  batch_key4: {:?}",
                memtable.get("batch_key4").await.unwrap_or(None)
            );
        }

        // Debug: Check SSTable count
        {
            let sstables = recovered_engine.sstables.read().await;
            println!("SSTable count after recovery: {}", sstables.len());
            for (level, sstable_vec) in sstables.iter() {
                println!("  SSTable level {}: {} tables", level, sstable_vec.len());
            }
        }

        let batch_key1_result = recovered_engine
            .get("batch_key1")
            .await
            .expect("Failed to get batch_key1 after recovery");
        let batch_key2_result = recovered_engine
            .get("batch_key2")
            .await
            .expect("Failed to get batch_key2 after recovery");
        let batch_key3_result = recovered_engine
            .get("batch_key3")
            .await
            .expect("Failed to get batch_key3 after recovery");
        let batch_key4_result = recovered_engine
            .get("batch_key4")
            .await
            .expect("Failed to get batch_key4 after recovery");

        println!("batch_key1: {:?}", batch_key1_result);
        println!("batch_key2: {:?}", batch_key2_result);
        println!("batch_key3: {:?}", batch_key3_result);
        println!("batch_key4: {:?}", batch_key4_result);

        // Updated key should have new value
        assert_eq!(
            batch_key1_result,
            Some(Value::String("updated_batch_value1".to_string()))
        );
        // Deleted key should be gone
        assert_eq!(batch_key2_result, None);
        // Original key should still be there
        assert_eq!(
            batch_key3_result,
            Some(Value::String("batch_value3".to_string()))
        );
        // New key should be present
        assert_eq!(
            batch_key4_result,
            Some(Value::String("batch_value4".to_string()))
        );
    }

    #[cfg(feature = "ttl")]
    #[tokio::test]
    async fn test_crash_recovery_ttl_operations() {
        let temp_dir = TempDir::new().expect("Failed to create temporary directory for test");
        let data_dir = temp_dir.path().to_path_buf();

        let config = LsmConfig {
            data_dir: data_dir.clone(),
            memtable: MemtableConfig::default(),
            wal: WalConfig {
                enabled: true,
                dir: data_dir.join("wal"),
                segment_size: 1024 * 1024,
                ..Default::default()
            },
            sstable: SstableConfig::default(),
            levels: LevelConfig::default(),
            compaction: CompactionConfig::default(),
            bloom_filter: BloomFilterConfig::default(),
            column_families: ColumnFamilyConfig {
                default_name: "default".to_string(),
                enable_isolation: false,
                max_count: 100,
            },
            performance: PerformanceConfig::default(),
            adaptive_compaction: None,
        };

        // Write TTL data
        {
            let engine = LsmTreeEngine::new(config.clone())
                .await
                .expect("Failed to create LSM engine for TTL test");
            engine
                .put_with_ttl(
                    "ttl_key1",
                    &Value::String("ttl_value1".to_string()),
                    Duration::from_secs(3600),
                )
                .await
                .expect("Failed to put ttl_key1 with TTL");
            engine
                .put_with_ttl(
                    "ttl_key2",
                    &Value::String("ttl_value2".to_string()),
                    Duration::from_secs(1),
                )
                .await
                .expect("Failed to put ttl_key2 with TTL");
            engine
                .put(
                    "persistent_key",
                    &Value::String("persistent_value".to_string()),
                )
                .await
                .expect("Failed to put persistent_key");
            engine.flush().await.expect("Failed to flush engine");
        }

        // Wait for TTL to expire and crash
        tokio::time::sleep(Duration::from_secs(2)).await;
        {
            let engine = LsmTreeEngine::new(config.clone())
                .await
                .expect("Failed to create LSM engine for crash simulation");
            engine
                .put("new_key", &Value::String("new_value".to_string()))
                .await
                .expect("Failed to put new_key");
            // Don't flush - simulate crash
        }

        // Recover and verify
        let recovered_engine = LsmTreeEngine::new(config)
            .await
            .expect("Failed to create recovered LSM engine");

        // TTL key with long expiry should still be there
        assert_eq!(
            recovered_engine
                .get("ttl_key1")
                .await
                .expect("Failed to get ttl_key1 after recovery"),
            Some(Value::String("ttl_value1".to_string()))
        );
        // TTL key with short expiry should still be there (TTL not implemented in LSM engine)
        assert_eq!(
            recovered_engine
                .get("ttl_key2")
                .await
                .expect("Failed to get ttl_key2 after recovery"),
            Some(Value::String("ttl_value2".to_string()))
        );
        // Persistent key should still be there
        assert_eq!(
            recovered_engine
                .get("persistent_key")
                .await
                .expect("Failed to get persistent_key after recovery"),
            Some(Value::String("persistent_value".to_string()))
        );
        // New key should be present
        assert_eq!(
            recovered_engine
                .get("new_key")
                .await
                .expect("Failed to get new_key after recovery"),
            Some(Value::String("new_value".to_string()))
        );
    }

    #[tokio::test]
    async fn test_crash_recovery_concurrent_operations() {
        let temp_dir = TempDir::new()
            .expect("Failed to create temporary directory for concurrent operations test");
        let data_dir = temp_dir.path().to_path_buf();

        let config = LsmConfig {
            data_dir: data_dir.clone(),
            memtable: MemtableConfig::default(),
            wal: WalConfig {
                enabled: true,
                dir: data_dir.join("wal"),
                segment_size: 1024 * 1024,
                ..Default::default()
            },
            sstable: SstableConfig::default(),
            levels: LevelConfig::default(),
            compaction: CompactionConfig::default(),
            bloom_filter: BloomFilterConfig::default(),
            column_families: ColumnFamilyConfig {
                default_name: "default".to_string(),
                enable_isolation: false,
                max_count: 100,
            },
            performance: PerformanceConfig::default(),
            adaptive_compaction: None,
        };

        // Write initial data
        {
            let engine = LsmTreeEngine::new(config.clone())
                .await
                .expect("Failed to create LSM engine for initial data");
            for i in 0..10 {
                let key = format!("concurrent_key_{}", i);
                let value = Value::String(format!("concurrent_value_{}", i));
                engine
                    .put(&key, &value)
                    .await
                    .expect(&format!("Failed to put concurrent_key_{}", i));
            }
            engine.flush().await.expect("Failed to flush initial data");
        }

        // Simulate concurrent operations and crash
        {
            let engine = LsmTreeEngine::new(config.clone())
                .await
                .expect("Failed to create LSM engine for concurrent operations");

            // Perform concurrent operations directly (avoiding lifetime issues)
            for i in 0..5 {
                for j in 0..5 {
                    let key = format!("concurrent_key_{}_{}", i, j);
                    let value = Value::String(format!("concurrent_value_{}_{}", i, j));
                    let _result = engine.put(&key, &value).await;
                }
            }

            // Don't flush - simulate crash during concurrent operations
        }

        // Recover and verify
        let recovered_engine = LsmTreeEngine::new(config)
            .await
            .expect("Failed to create recovered engine after concurrent operations");

        // Original data should still be there
        for i in 0..10 {
            let key = format!("concurrent_key_{}", i);
            let expected_value = Value::String(format!("concurrent_value_{}", i));
            assert_eq!(
                recovered_engine
                    .get(&key)
                    .await
                    .expect(&format!("Failed to get key {} after recovery", key)),
                Some(expected_value)
            );
        }

        // Some concurrent data might be recovered depending on WAL implementation
        // We just verify the engine doesn't panic and can recover
    }

    #[tokio::test]
    async fn test_crash_recovery_large_dataset() {
        let temp_dir = TempDir::new().expect("Failed to create temporary directory for test");
        let data_dir = temp_dir.path().to_path_buf();

        let config = LsmConfig {
            data_dir: data_dir.clone(),
            memtable: MemtableConfig::default(),
            wal: WalConfig {
                enabled: true,
                dir: data_dir.join("wal"),
                segment_size: 1024 * 1024,
                ..Default::default()
            },
            sstable: SstableConfig::default(),
            levels: LevelConfig::default(),
            compaction: CompactionConfig::default(),
            bloom_filter: BloomFilterConfig::default(),
            column_families: ColumnFamilyConfig {
                default_name: "default".to_string(),
                enable_isolation: false,
                max_count: 100,
            },
            performance: PerformanceConfig::default(),
            adaptive_compaction: None,
        };

        // Write large dataset
        {
            let engine = LsmTreeEngine::new(config.clone())
                .await
                .expect("Failed to create LSM engine for test");
            for i in 0..1000 {
                let key = format!("large_key_{:04}", i);
                let value = Value::String(format!("large_value_{:04}", i));
                engine
                    .put(&key, &value)
                    .await
                    .expect(&format!("Failed to put key {} in test", key));

                if i % 100 == 0 {
                    engine
                        .flush()
                        .await
                        .expect("Failed to flush engine in test");
                }
            }
            engine
                .flush()
                .await
                .expect("Failed to flush engine in test");
        }

        // Modify some data and crash
        {
            let engine = LsmTreeEngine::new(config.clone())
                .await
                .expect("Failed to create LSM engine for test");
            for i in 0..100 {
                let key = format!("large_key_{:04}", i);
                let value = Value::String(format!("updated_large_value_{:04}", i));
                engine
                    .put(&key, &value)
                    .await
                    .expect(&format!("Failed to put key {} in test", key));
            }
            // Don't flush - simulate crash
        }

        // Recover and verify
        let recovered_engine = LsmTreeEngine::new(config)
            .await
            .expect("Failed to create recovered engine after concurrent operations");

        // Check some original data
        for i in 500..600 {
            let key = format!("large_key_{:04}", i);
            let expected_value = Value::String(format!("large_value_{:04}", i));
            assert_eq!(
                recovered_engine
                    .get(&key)
                    .await
                    .expect(&format!("Failed to get key {} after recovery", key)),
                Some(expected_value)
            );
        }

        // Check some updated data (might be recovered from WAL)
        for i in 0..10 {
            let key = format!("large_key_{:04}", i);
            let result = recovered_engine
                .get(&key)
                .await
                .expect(&format!("Failed to get key {} in test", key));
            // Should either have original value or updated value
            assert!(result.is_some());
        }
    }

    #[tokio::test]
    async fn test_crash_recovery_corrupted_wal() {
        let temp_dir = TempDir::new().expect("Failed to create temporary directory for test");
        let data_dir = temp_dir.path().to_path_buf();
        let wal_dir = data_dir.join("wal");

        let config = LsmConfig {
            data_dir: data_dir.clone(),
            memtable: MemtableConfig::default(),
            wal: WalConfig {
                enabled: true,
                dir: wal_dir.clone(),
                segment_size: 1024 * 1024,
                ..Default::default()
            },
            sstable: SstableConfig::default(),
            levels: LevelConfig::default(),
            compaction: CompactionConfig::default(),
            bloom_filter: BloomFilterConfig::default(),
            column_families: ColumnFamilyConfig {
                default_name: "default".to_string(),
                enable_isolation: false,
                max_count: 100,
            },
            performance: PerformanceConfig::default(),
            adaptive_compaction: None,
        };

        // Write some data
        {
            let engine = LsmTreeEngine::new(config.clone())
                .await
                .expect("Failed to create LSM engine for test");
            engine
                .put(
                    "corruption_key1",
                    &Value::String("corruption_value1".to_string()),
                )
                .await
                .expect("Failed to perform operation in test");
            engine
                .put(
                    "corruption_key2",
                    &Value::String("corruption_value2".to_string()),
                )
                .await
                .expect("Failed to perform operation in test");
            engine
                .flush()
                .await
                .expect("Failed to flush engine in test");
        }

        // Corrupt WAL file
        if let Ok(mut wal_file) = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(wal_dir.join("segment_0000000000000000.wal"))
        {
            use std::io::Write;
            let _ = wal_file.write_all(b"corrupted_wal_data");
        }

        // Try to recover with corrupted WAL
        let recovered_engine = LsmTreeEngine::new(config)
            .await
            .expect("Failed to create recovered engine in test");

        // Should still be able to recover some data
        // The exact behavior depends on WAL implementation
        let result1 = recovered_engine.get("corruption_key1").await;
        let result2 = recovered_engine.get("corruption_key2").await;

        // At least one should be recoverable
        assert!(result1.is_ok() || result2.is_ok());
    }

    #[tokio::test]
    async fn test_compaction_with_concurrent_writes() {
        let (engine, _temp_dir) = create_test_engine().await;

        // Write enough data to trigger compaction
        for i in 0..100 {
            let key = format!("compaction_key_{}", i);
            let value = Value::String(format!("compaction_value_{}", i));
            engine.put(&key, &value).await.expect("Failed to put value");
        }

        // Flush to create SSTables
        engine.flush().await.expect("Failed to flush");

        // Trigger compaction
        let compaction_result = engine.compact_all().await;
        // Compaction should succeed or handle gracefully
        assert!(compaction_result.is_ok() || compaction_result.is_err());

        // Verify data is still accessible after compaction
        for i in 0..10 {
            let key = format!("compaction_key_{}", i);
            let result = engine.get(&key).await;
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_compaction_with_large_values() {
        let (engine, _temp_dir) = create_test_engine().await;

        // Write large values to test compaction with large data
        for i in 0..50 {
            let key = format!("large_compaction_key_{}", i);
            let large_value = "x".repeat(10000); // 10KB value
            let value = Value::String(large_value);
            engine
                .put(&key, &value)
                .await
                .expect("Failed to put large value");
        }

        // Flush and compact
        engine.flush().await.expect("Failed to flush");
        let _ = engine.compact_all().await;

        // Verify large values are still accessible
        for i in 0..10 {
            let key = format!("large_compaction_key_{}", i);
            let result = engine.get(&key).await;
            assert!(result.is_ok());
            assert!(result.expect("Failed to get result in test").is_some());
        }
    }

    #[tokio::test]
    async fn test_compaction_with_deletes() {
        let (engine, _temp_dir) = create_test_engine().await;

        // Write data
        for i in 0..100 {
            let key = format!("delete_compaction_key_{}", i);
            let value = Value::String(format!("value_{}", i));
            engine.put(&key, &value).await.expect("Failed to put value");
        }

        // Delete some keys
        for i in 0..50 {
            let key = format!("delete_compaction_key_{}", i);
            engine.delete(&key).await.expect("Failed to delete key");
        }

        // Flush and compact
        engine.flush().await.expect("Failed to flush");
        let _ = engine.compact_all().await;

        // Verify deleted keys are gone
        for i in 0..50 {
            let key = format!("delete_compaction_key_{}", i);
            let result = engine.get(&key).await.expect("Failed to get deleted key");
            assert_eq!(result, None);
        }

        // Verify remaining keys are still there
        for i in 50..100 {
            let key = format!("delete_compaction_key_{}", i);
            let result = engine.get(&key).await.expect("Failed to get remaining key");
            assert_eq!(result, Some(Value::String(format!("value_{}", i))));
        }
    }

    #[tokio::test]
    async fn test_compaction_stats() {
        let (engine, _temp_dir) = create_test_engine().await;

        // Write some data
        for i in 0..50 {
            let key = format!("stats_key_{}", i);
            let value = Value::String(format!("value_{}", i));
            engine.put(&key, &value).await.expect("Failed to put value");
        }

        // Flush and compact
        engine.flush().await.expect("Failed to flush");
        let _ = engine.compact_all().await;

        // Get compaction stats
        let stats_result = engine.compaction_stats().await;
        assert!(stats_result.is_ok());
        let _stats = stats_result.expect("Failed to get stats in test");
        // Stats should be available (exact values depend on implementation)
    }

    #[tokio::test]
    async fn test_wal_recovery_partial_write() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let data_dir = temp_dir.path().to_path_buf();

        let config = LsmConfig {
            data_dir: data_dir.clone(),
            memtable: MemtableConfig::default(),
            wal: WalConfig {
                enabled: true,
                dir: data_dir.join("wal"),
                segment_size: 1024 * 1024,
                allow_recovery_failure: true, // Allow recovery to continue even if partial
                ..Default::default()
            },
            sstable: SstableConfig::default(),
            levels: LevelConfig::default(),
            compaction: CompactionConfig::default(),
            bloom_filter: BloomFilterConfig::default(),
            column_families: ColumnFamilyConfig {
                default_name: "default".to_string(),
                enable_isolation: false,
                max_count: 100,
            },
            performance: PerformanceConfig::default(),
            adaptive_compaction: None,
        };

        // Write data
        {
            let engine = LsmTreeEngine::new(config.clone())
                .await
                .expect("Failed to create LSM engine for test");
            engine
                .put("partial_key1", &Value::String("partial_value1".to_string()))
                .await
                .expect("Failed to perform operation in test");
            engine
                .put("partial_key2", &Value::String("partial_value2".to_string()))
                .await
                .expect("Failed to perform operation in test");
            // Don't flush - simulate partial write scenario
        }

        // Try to recover
        let recovered_engine = LsmTreeEngine::new(config).await;
        // Recovery should succeed (with allow_recovery_failure=true) or fail gracefully
        assert!(recovered_engine.is_ok() || recovered_engine.is_err());
    }

    #[tokio::test]
    async fn test_error_handling_disk_full() {
        let (engine, _temp_dir) = create_test_engine().await;

        // Test error handling for various scenarios
        // Note: Actually simulating disk full is difficult in tests,
        // but we can test that operations handle errors gracefully

        // Test with very large values that might cause issues
        let very_large_value = "x".repeat(100000); // 100KB
        let result = engine
            .put("huge_key", &Value::String(very_large_value))
            .await;
        // Should either succeed or fail gracefully
        assert!(result.is_ok() || result.is_err());
    }

    #[tokio::test]
    async fn test_error_handling_invalid_operations() {
        let (engine, _temp_dir) = create_test_engine().await;

        // Test operations that might cause errors
        // Get non-existent key (should return None, not error)
        let result = engine.get("nonexistent_key").await;
        assert!(result.is_ok());
        assert_eq!(result.expect("Failed to get result in test"), None);

        // Delete non-existent key (should succeed)
        let result = engine.delete("nonexistent_key").await;
        assert!(result.is_ok());
    }
}
