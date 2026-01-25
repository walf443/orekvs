//! Manifest file for tracking SSTable level assignments.
//!
//! The manifest file persists the level structure of SSTables to disk,
//! allowing recovery of the leveled compaction state after restart.
//!
//! File format: JSON for simplicity and debuggability.
//! Atomic updates via write-rename pattern.

use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

/// Encode bytes to hex string
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Decode hex string to bytes
#[allow(dead_code)]
fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// Current manifest format version
const MANIFEST_VERSION: u32 = 1;

/// Manifest file name
const MANIFEST_FILENAME: &str = "MANIFEST";

/// Temporary manifest file name for atomic writes
const MANIFEST_TMP_FILENAME: &str = "MANIFEST.tmp";

/// Entry in the manifest representing a single SSTable
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestEntry {
    /// SSTable file name (relative to data directory)
    pub filename: String,
    /// Level this SSTable belongs to (0 = L0, 1 = L1, etc.)
    pub level: usize,
    /// Minimum key in this SSTable (hex-encoded for JSON compatibility)
    pub min_key_hex: String,
    /// Maximum key in this SSTable (hex-encoded for JSON compatibility)
    pub max_key_hex: String,
    /// File size in bytes
    pub size_bytes: u64,
}

impl ManifestEntry {
    /// Create a new manifest entry
    pub fn new(
        filename: String,
        level: usize,
        min_key: &[u8],
        max_key: &[u8],
        size_bytes: u64,
    ) -> Self {
        Self {
            filename,
            level,
            min_key_hex: hex_encode(min_key),
            max_key_hex: hex_encode(max_key),
            size_bytes,
        }
    }

    /// Get the minimum key as bytes
    #[allow(dead_code)]
    pub fn min_key(&self) -> Vec<u8> {
        hex_decode(&self.min_key_hex).unwrap_or_default()
    }

    /// Get the maximum key as bytes
    #[allow(dead_code)]
    pub fn max_key(&self) -> Vec<u8> {
        hex_decode(&self.max_key_hex).unwrap_or_default()
    }
}

/// The manifest file structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Manifest format version
    pub version: u32,
    /// List of all SSTable entries with their level assignments
    pub entries: Vec<ManifestEntry>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self::new()
    }
}

impl Manifest {
    /// Create a new empty manifest
    pub fn new() -> Self {
        Self {
            version: MANIFEST_VERSION,
            entries: Vec::new(),
        }
    }

    /// Load manifest from the data directory
    /// Returns None if manifest doesn't exist
    pub fn load(data_dir: &Path) -> std::io::Result<Option<Self>> {
        let manifest_path = data_dir.join(MANIFEST_FILENAME);

        if !manifest_path.exists() {
            return Ok(None);
        }

        let file = File::open(&manifest_path)?;
        let reader = BufReader::new(file);

        match serde_json::from_reader(reader) {
            Ok(manifest) => Ok(Some(manifest)),
            Err(e) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to parse manifest: {}", e),
            )),
        }
    }

    /// Save manifest to the data directory atomically
    /// Uses write-rename pattern for crash safety
    pub fn save(&self, data_dir: &Path) -> std::io::Result<()> {
        let manifest_path = data_dir.join(MANIFEST_FILENAME);
        let tmp_path = data_dir.join(MANIFEST_TMP_FILENAME);

        // Write to temporary file
        {
            let file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;
            let mut writer = BufWriter::new(file);

            serde_json::to_writer_pretty(&mut writer, self).map_err(|e| {
                std::io::Error::other(format!("Failed to serialize manifest: {}", e))
            })?;

            writer.flush()?;
            writer.get_ref().sync_all()?;
        }

        // Atomic rename
        fs::rename(&tmp_path, &manifest_path)?;

        // Sync parent directory for durability
        if let Ok(dir) = File::open(data_dir) {
            let _ = dir.sync_all();
        }

        Ok(())
    }

    /// Add an SSTable entry to the manifest
    pub fn add_entry(&mut self, entry: ManifestEntry) {
        self.entries.push(entry);
    }

    /// Remove an SSTable entry by filename
    pub fn remove_entry(&mut self, filename: &str) {
        self.entries.retain(|e| e.filename != filename);
    }

    /// Get all entries for a specific level
    #[allow(dead_code)]
    pub fn entries_for_level(&self, level: usize) -> Vec<&ManifestEntry> {
        self.entries.iter().filter(|e| e.level == level).collect()
    }

    /// Get entry by filename
    pub fn get_entry(&self, filename: &str) -> Option<&ManifestEntry> {
        self.entries.iter().find(|e| e.filename == filename)
    }

    /// Update the level of an existing entry
    #[allow(dead_code)]
    pub fn update_level(&mut self, filename: &str, new_level: usize) {
        if let Some(entry) = self.entries.iter_mut().find(|e| e.filename == filename) {
            entry.level = new_level;
        }
    }

    /// Get manifest file path
    #[allow(dead_code)]
    pub fn path(data_dir: &Path) -> PathBuf {
        data_dir.join(MANIFEST_FILENAME)
    }

    /// Check if manifest exists
    #[allow(dead_code)]
    pub fn exists(data_dir: &Path) -> bool {
        Self::path(data_dir).exists()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_manifest_entry_creation() {
        let entry = ManifestEntry::new("sst_1_0.data".to_string(), 0, b"apple", b"zebra", 1024);

        assert_eq!(entry.filename, "sst_1_0.data");
        assert_eq!(entry.level, 0);
        assert_eq!(entry.min_key(), b"apple");
        assert_eq!(entry.max_key(), b"zebra");
        assert_eq!(entry.size_bytes, 1024);
    }

    #[test]
    fn test_manifest_save_and_load() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path();

        // Create manifest with entries
        let mut manifest = Manifest::new();
        manifest.add_entry(ManifestEntry::new(
            "sst_1_0.data".to_string(),
            0,
            b"a",
            b"m",
            1000,
        ));
        manifest.add_entry(ManifestEntry::new(
            "sst_2_0.data".to_string(),
            0,
            b"n",
            b"z",
            2000,
        ));
        manifest.add_entry(ManifestEntry::new(
            "sst_3_0.data".to_string(),
            1,
            b"a",
            b"z",
            5000,
        ));

        // Save
        manifest.save(data_dir).unwrap();

        // Verify file exists
        assert!(Manifest::exists(data_dir));

        // Load and verify
        let loaded = Manifest::load(data_dir).unwrap().unwrap();
        assert_eq!(loaded.version, MANIFEST_VERSION);
        assert_eq!(loaded.entries.len(), 3);

        // Check entries
        let l0_entries = loaded.entries_for_level(0);
        assert_eq!(l0_entries.len(), 2);

        let l1_entries = loaded.entries_for_level(1);
        assert_eq!(l1_entries.len(), 1);
        assert_eq!(l1_entries[0].filename, "sst_3_0.data");
    }

    #[test]
    fn test_manifest_load_nonexistent() {
        let dir = tempdir().unwrap();
        let result = Manifest::load(dir.path()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_manifest_remove_entry() {
        let mut manifest = Manifest::new();
        manifest.add_entry(ManifestEntry::new(
            "sst_1_0.data".to_string(),
            0,
            b"a",
            b"z",
            1000,
        ));
        manifest.add_entry(ManifestEntry::new(
            "sst_2_0.data".to_string(),
            0,
            b"a",
            b"z",
            2000,
        ));

        assert_eq!(manifest.entries.len(), 2);

        manifest.remove_entry("sst_1_0.data");
        assert_eq!(manifest.entries.len(), 1);
        assert_eq!(manifest.entries[0].filename, "sst_2_0.data");
    }

    #[test]
    fn test_manifest_update_level() {
        let mut manifest = Manifest::new();
        manifest.add_entry(ManifestEntry::new(
            "sst_1_0.data".to_string(),
            0,
            b"a",
            b"z",
            1000,
        ));

        assert_eq!(manifest.get_entry("sst_1_0.data").unwrap().level, 0);

        manifest.update_level("sst_1_0.data", 1);
        assert_eq!(manifest.get_entry("sst_1_0.data").unwrap().level, 1);
    }

    #[test]
    fn test_manifest_atomic_save() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path();

        // Save manifest
        let mut manifest = Manifest::new();
        manifest.add_entry(ManifestEntry::new(
            "sst_1_0.data".to_string(),
            0,
            b"test",
            b"test",
            100,
        ));
        manifest.save(data_dir).unwrap();

        // Verify no tmp file remains
        let tmp_path = data_dir.join(MANIFEST_TMP_FILENAME);
        assert!(!tmp_path.exists());

        // Verify manifest file exists
        let manifest_path = data_dir.join(MANIFEST_FILENAME);
        assert!(manifest_path.exists());
    }
}
