//! Flush and compaction operations for LsmTreeEngine.
//!
//! This module contains background operations for:
//! - MemTable flushing to SSTables
//! - Leveled compaction
//! - WAL archiving
//! - Stale SSTable cleanup

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tonic::Status;

use super::manifest::ManifestEntry;
use super::memtable::MemTable;
use super::sstable::{self, MappedSSTable};
use super::wal::GroupCommitWalWriter;
use super::{LsmTreeEngine, SstableHandle};

impl LsmTreeEngine {
    /// Check if MemTable needs to be flushed and trigger if necessary
    pub(super) fn trigger_flush_if_needed(&self) {
        if let Some(immutable) = self.mem_state.needs_flush() {
            let engine_clone = self.clone();
            tokio::spawn(async move {
                if let Err(e) = engine_clone.flush_memtable(immutable).await {
                    eprintln!("Failed to flush memtable: {}", e);
                }
            });
        }
    }

    /// Flush an immutable MemTable to an SSTable
    pub(super) async fn flush_memtable(&self, memtable: Arc<MemTable>) -> Result<(), Status> {
        let sst_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        // Get current WAL ID to include in SSTable filename
        let wal_id = self.wal.current_id();
        let sst_path = sstable::generate_path(&self.data_dir, sst_id, wal_id);

        println!("Flushing MemTable to {:?}...", sst_path);

        // create_from_memtable returns the embedded Bloom filter
        let bloom = sstable::create_from_memtable(&sst_path, &memtable)?;

        // Record flush metrics (estimate bytes from SSTable file size)
        if let Ok(metadata) = std::fs::metadata(&sst_path) {
            self.metrics.record_flush(metadata.len());
        }

        // Open mmap for the newly created SSTable
        let mmap = MappedSSTable::open(&sst_path)?;

        // Get min/max keys and entry count for manifest
        let min_key = mmap.min_key().to_vec();
        let max_key = mmap.max_key().to_vec();
        let entry_count = mmap.entry_count();
        let file_size = std::fs::metadata(&sst_path).map(|m| m.len()).unwrap_or(0);
        let filename = sst_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();

        let handle = Arc::new(SstableHandle { mmap, bloom });

        {
            let mut leveled = self.leveled_sstables.lock().unwrap();
            leveled.add_to_level(0, handle);
        }

        // Update manifest with new SSTable and current WAL sequence
        {
            let mut manifest = self.manifest.lock().unwrap();
            manifest.add_entry(ManifestEntry::new(
                filename,
                0,
                &min_key,
                &max_key,
                file_size,
                Some(entry_count),
            ));
            // Record the current WAL sequence for LSN-based recovery
            // All entries with seq <= this value are now persisted in SSTables
            manifest.set_last_flushed_wal_seq(self.wal.current_seq().saturating_sub(1));
            if let Err(e) = manifest.save(&self.data_dir) {
                eprintln!("Warning: Failed to save manifest: {}", e);
            }
        }

        self.mem_state.remove_immutable(&memtable);

        // Record old WAL file's max sequence before rotating
        let old_wal_id = self.wal.current_id();
        let old_wal_max_seq = self.wal.current_seq().saturating_sub(1);

        // Rotate WAL: create a new WAL file
        if let Err(e) = self.wal.rotate() {
            eprintln!("Warning: Failed to rotate WAL: {}", e);
        } else {
            // Record the old WAL file's max sequence for LSN-based cleanup
            let mut manifest = self.manifest.lock().unwrap();
            manifest.record_wal_file_max_seq(old_wal_id, old_wal_max_seq);
            if let Err(e) = manifest.save(&self.data_dir) {
                eprintln!("Warning: Failed to save manifest after WAL rotation: {}", e);
            }
        }

        // Archive old WAL files if configured
        self.archive_old_wal_files();

        println!("Flush finished.");

        // Check if compaction is needed
        self.check_compaction();

        Ok(())
    }

    /// Check if compaction should be triggered and run it if needed
    pub(super) fn check_compaction(&self) {
        // Find which level needs compaction
        let compaction_level = {
            let leveled = self.leveled_sstables.lock().unwrap();

            // Check L0 file count first
            let l0_count = leveled.l0_sstables().len();
            if self.compaction_handler.l0_needs_compaction(l0_count) {
                Some(0)
            } else {
                // Check L1+ for size-based compaction
                let mut needs_compaction = None;
                for level in 1..leveled.level_count() {
                    let level_size = leveled.level_size(level);
                    if self
                        .compaction_handler
                        .level_needs_compaction(level, level_size)
                    {
                        needs_compaction = Some(level);
                        break;
                    }
                }
                needs_compaction
            }
        };

        if let Some(level) = compaction_level {
            let mut in_progress = self.compaction_in_progress.lock().unwrap();
            if !*in_progress {
                *in_progress = true;
                drop(in_progress);

                let engine_clone = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = engine_clone.run_compaction_for_level(level).await {
                        eprintln!("Compaction failed for level {}: {}", level, e);
                    }
                    *engine_clone.compaction_in_progress.lock().unwrap() = false;
                });
            }
        }
    }

    /// Check if there are stale SSTables that need compaction.
    /// A SSTable is considered stale if its WAL ID is significantly older than the current WAL ID.
    /// Returns the count of stale SSTables.
    fn count_stale_sstables(&self, current_wal_id: u64, threshold: u64) -> usize {
        let leveled = self.leveled_sstables.lock().unwrap();
        let mut stale_count = 0;

        for level in 0..leveled.level_count() {
            for handle in leveled.get_level(level) {
                if let Some(wal_id) = sstable::extract_wal_id_from_sstable(handle.mmap.path())
                    && current_wal_id > wal_id
                    && current_wal_id - wal_id >= threshold
                {
                    stale_count += 1;
                }
            }
        }

        stale_count
    }

    /// Trigger compaction for stale SSTables asynchronously.
    /// This runs in the background and doesn't block startup.
    pub(super) fn trigger_stale_compaction(&self) {
        let current_wal_id = self.wal.current_id();

        // Only check for stale files if WAL ID is high enough
        // This prevents unnecessary compaction on fresh/test setups
        const MIN_WAL_ID_FOR_STALE_CHECK: u64 = 3;
        if current_wal_id < MIN_WAL_ID_FOR_STALE_CHECK {
            return;
        }

        // Consider SSTables stale if their WAL ID is at least 2 behind current
        const STALE_THRESHOLD: u64 = 2;

        let stale_count = self.count_stale_sstables(current_wal_id, STALE_THRESHOLD);

        if stale_count == 0 {
            return;
        }

        println!(
            "Found {} stale SSTables (WAL ID < {}), triggering background compaction",
            stale_count,
            current_wal_id.saturating_sub(STALE_THRESHOLD)
        );

        let engine_clone = self.clone();
        tokio::spawn(async move {
            engine_clone.compact_stale_l0().await;
        });
    }

    /// Compact L0 level to clean up stale SSTables after recovery.
    /// L0 compaction merges all L0 files with overlapping L1 files,
    /// which effectively cleans up old SSTables by merging them into newer ones.
    async fn compact_stale_l0(&self) {
        // Wait a bit before starting to allow the engine to fully initialize
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Wait for any ongoing compaction to finish first
        loop {
            let is_in_progress = {
                let in_progress = self.compaction_in_progress.lock().unwrap();
                *in_progress
            };
            if !is_in_progress {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }

        // Compact L0 (this is where stale files accumulate)
        // L0 compaction merges all L0 files with overlapping L1 files
        let has_l0_files = {
            let leveled = self.leveled_sstables.lock().unwrap();
            !leveled.get_level(0).is_empty()
        };

        if !has_l0_files {
            println!("Stale compaction: no L0 files to compact");
            return;
        }

        {
            let mut in_progress = self.compaction_in_progress.lock().unwrap();
            *in_progress = true;
        }

        println!("Stale compaction: running L0 compaction");
        if let Err(e) = self.run_compaction_for_level(0).await {
            eprintln!("Stale compaction failed for L0: {}", e);
        }

        {
            let mut in_progress = self.compaction_in_progress.lock().unwrap();
            *in_progress = false;
        }

        println!("Background stale SSTable compaction finished");
    }

    /// Archive old WAL files based on LSN, retention period and size limits.
    /// Only archives WAL files where all entries have been flushed to SSTables.
    pub(super) fn archive_old_wal_files(&self) {
        // Skip if archiving is disabled
        if self.wal_archive_config.retention_secs.is_none()
            && self.wal_archive_config.max_size_bytes.is_none()
        {
            return;
        }

        // Get WAL IDs that can be safely deleted based on LSN
        let deletable_wal_ids: std::collections::HashSet<u64> = {
            let manifest = self.manifest.lock().unwrap();
            manifest.get_deletable_wal_ids().into_iter().collect()
        };

        // Collect WAL files with their metadata
        let mut wal_files: Vec<(u64, PathBuf, u64, std::time::SystemTime)> = Vec::new();

        if let Ok(entries) = fs::read_dir(&self.data_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(filename) = path.file_name().and_then(|n| n.to_str())
                    && let Some(wal_id) = GroupCommitWalWriter::parse_wal_filename(filename)
                {
                    // Only consider WAL files that have been fully flushed (based on LSN)
                    if deletable_wal_ids.contains(&wal_id)
                        && let Ok(metadata) = fs::metadata(&path)
                    {
                        let size = metadata.len();
                        let modified = metadata.modified().unwrap_or(std::time::UNIX_EPOCH);
                        wal_files.push((wal_id, path, size, modified));
                    }
                }
            }
        }

        if wal_files.is_empty() {
            return;
        }

        // Sort by WAL ID (oldest first)
        wal_files.sort_by_key(|(id, _, _, _)| *id);

        // Calculate total size
        let total_size: u64 = wal_files.iter().map(|(_, _, size, _)| size).sum();
        let now = std::time::SystemTime::now();

        let mut files_to_delete: Vec<(u64, PathBuf)> = Vec::new();
        let mut remaining_size = total_size;

        for (wal_id, path, size, modified) in &wal_files {
            let mut should_delete = false;

            // Check retention period
            if let Some(retention_secs) = self.wal_archive_config.retention_secs
                && let Ok(age) = now.duration_since(*modified)
                && age.as_secs() > retention_secs
            {
                should_delete = true;
            }

            // Check size limit (delete oldest files first to get under limit)
            if let Some(max_size) = self.wal_archive_config.max_size_bytes
                && remaining_size > max_size
            {
                should_delete = true;
            }

            if should_delete {
                files_to_delete.push((*wal_id, path.clone()));
                remaining_size = remaining_size.saturating_sub(*size);
            }
        }

        // Delete the files and update manifest
        if !files_to_delete.is_empty() {
            let mut deleted_ids = Vec::new();
            for (wal_id, path) in &files_to_delete {
                match fs::remove_file(path) {
                    Ok(_) => {
                        println!(
                            "Archived (deleted) old WAL file: {:?} (id={})",
                            path, wal_id
                        );
                        deleted_ids.push(*wal_id);
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to archive WAL file {:?}: {}", path, e);
                    }
                }
            }

            // Remove deleted WAL files from manifest tracking
            if !deleted_ids.is_empty() {
                let mut manifest = self.manifest.lock().unwrap();
                for wal_id in deleted_ids {
                    manifest.remove_wal_file_seq(wal_id);
                }
                if let Err(e) = manifest.save(&self.data_dir) {
                    eprintln!("Warning: Failed to save manifest after WAL cleanup: {}", e);
                }
            }
        }
    }

    /// Get the maximum WAL ID that has been flushed to SSTables
    /// Note: Now using LSN-based cleanup via manifest.get_deletable_wal_ids()
    #[allow(dead_code)]
    fn get_max_flushed_wal_id(&self) -> u64 {
        let leveled = self.leveled_sstables.lock().unwrap();
        let mut max_wal_id = 0u64;

        // Check all levels for the max WAL ID
        for level in 0..leveled.level_count() {
            for handle in leveled.get_level(level) {
                if let Some(wal_id) = sstable::extract_wal_id_from_sstable(handle.mmap.path())
                    && wal_id > max_wal_id
                {
                    max_wal_id = wal_id;
                }
            }
        }

        max_wal_id
    }

    /// Run leveled compaction for the specified level
    /// - Level 0: Compact all L0 files into L1
    /// - Level 1+: Compact one file from Ln into Ln+1
    pub(super) async fn run_compaction_for_level(&self, source_level: usize) -> Result<(), Status> {
        println!("Starting leveled compaction for L{}...", source_level);

        let target_level = source_level + 1;

        // Get source and target level file paths
        let (source_paths, target_paths): (Vec<PathBuf>, Vec<PathBuf>) = {
            let leveled = self.leveled_sstables.lock().unwrap();
            let source = leveled
                .get_level(source_level)
                .iter()
                .map(|h| h.mmap.path().to_path_buf())
                .collect();
            let target = leveled
                .get_level(target_level)
                .iter()
                .map(|h| h.mmap.path().to_path_buf())
                .collect();
            (source, target)
        };

        if source_paths.is_empty() {
            println!("No L{} SSTables to compact", source_level);
            return Ok(());
        }

        // Plan compaction based on level
        let task = if source_level == 0 {
            // L0 → L1: Compact all L0 files
            match self
                .compaction_handler
                .plan_l0_compaction(&source_paths, &target_paths)?
            {
                Some(t) => t,
                None => {
                    println!("No L0 compaction needed");
                    return Ok(());
                }
            }
        } else {
            // Ln → Ln+1: Compact one file from source level
            match self.compaction_handler.plan_level_compaction(
                source_level,
                &source_paths,
                &target_paths,
            )? {
                Some(t) => t,
                None => {
                    println!("No L{} compaction needed", source_level);
                    return Ok(());
                }
            }
        };

        println!(
            "Compacting {} L{} files + {} L{} files → L{}",
            task.source_files.len(),
            source_level,
            task.target_files.len(),
            target_level,
            target_level
        );

        // Execute compaction
        let mut next_sst_id = self.next_sst_id.load(Ordering::SeqCst);
        let wal_id = self.wal.current_id();
        let result = self
            .compaction_handler
            .execute(&task, &mut next_sst_id, wal_id)?;

        // Update next_sst_id
        self.next_sst_id.store(next_sst_id, Ordering::SeqCst);

        // Record compaction metrics
        self.metrics
            .record_compaction(result.bytes_read, result.bytes_written);

        // Build handles for new SSTables
        let mut new_handles = Vec::new();
        for output in &result.new_files {
            let mmap = MappedSSTable::open(&output.path)?;
            let bloom = mmap.read_bloom_filter()?;
            new_handles.push((output.level, Arc::new(SstableHandle { mmap, bloom })));
        }

        // Update leveled sstables
        {
            let mut leveled = self.leveled_sstables.lock().unwrap();

            // Remove compacted files from their levels
            for path in &result.files_to_delete {
                let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                leveled.remove_by_filename(filename);
            }

            // Add new SSTables to their target levels
            for (level, handle) in new_handles {
                leveled.add_to_level(level, handle);
            }
        }

        // Update manifest
        {
            let mut manifest = self.manifest.lock().unwrap();

            // Remove old entries
            for path in &result.files_to_delete {
                let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                manifest.remove_entry(filename);
            }

            // Add new entries
            for output in &result.new_files {
                manifest.add_entry(output.to_manifest_entry());
            }

            if let Err(e) = manifest.save(&self.data_dir) {
                eprintln!("Warning: Failed to save manifest after compaction: {}", e);
            }
        }

        // Delete old SSTable files
        {
            let _lock = self.snapshot_lock.write().unwrap();
            for path in &result.files_to_delete {
                if let Err(e) = fs::remove_file(path) {
                    eprintln!("Failed to delete old SSTable {:?}: {}", path, e);
                } else {
                    println!("Deleted old SSTable: {:?}", path);
                    self.block_cache.invalidate_file(path);
                }
            }
        }

        println!(
            "Leveled compaction L{} → L{} finished. Created {} new SSTables.",
            source_level,
            target_level,
            result.new_files.len()
        );
        Ok(())
    }
}
