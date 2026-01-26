//! Page manager for disk I/O operations.
//!
//! Handles reading and writing pages to the data file,
//! page allocation, and file management.

use super::page::{MetaPage, PAGE_SIZE, Page, PageType};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

/// Manages page-level disk I/O
pub struct PageManager {
    /// Path to the data file
    path: PathBuf,
    /// File handle
    file: File,
    /// Next page ID to allocate
    next_page_id: AtomicU32,
}

impl PageManager {
    /// Open or create a B-tree data file
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file_exists = path.exists();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let mut manager = Self {
            path,
            file,
            next_page_id: AtomicU32::new(1),
        };

        if file_exists {
            // Read meta page to get page count
            let meta_page = manager.read_page(0)?;
            let meta = MetaPage::from_page(&meta_page)?;
            manager
                .next_page_id
                .store(meta.page_count, Ordering::SeqCst);
        } else {
            // Initialize with meta page
            let meta = MetaPage::default();
            let page = meta.to_page();
            manager.write_page(0, &page)?;
            manager.sync()?;
        }

        Ok(manager)
    }

    /// Get the file path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read a page from disk
    pub fn read_page(&mut self, page_id: u32) -> io::Result<Page> {
        let offset = (page_id as u64) * (PAGE_SIZE as u64);
        self.file.seek(SeekFrom::Start(offset))?;

        let mut buf = [0u8; PAGE_SIZE];
        self.file.read_exact(&mut buf)?;

        Page::from_bytes(&buf)
    }

    /// Write a page to disk
    pub fn write_page(&mut self, page_id: u32, page: &Page) -> io::Result<()> {
        let offset = (page_id as u64) * (PAGE_SIZE as u64);
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&page.to_bytes())?;
        Ok(())
    }

    /// Allocate a new page ID
    pub fn allocate_page(&self) -> u32 {
        self.next_page_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Get the current page count (next page ID)
    pub fn page_count(&self) -> u32 {
        self.next_page_id.load(Ordering::SeqCst)
    }

    /// Set page count (used during recovery)
    pub fn set_page_count(&self, count: u32) {
        self.next_page_id.store(count, Ordering::SeqCst);
    }

    /// Sync file to disk
    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    /// Read the meta page
    pub fn read_meta(&mut self) -> io::Result<MetaPage> {
        let page = self.read_page(0)?;
        MetaPage::from_page(&page)
    }

    /// Write the meta page
    pub fn write_meta(&mut self, meta: &MetaPage) -> io::Result<()> {
        let page = meta.to_page();
        self.write_page(0, &page)
    }

    /// Allocate and write a new page
    pub fn allocate_and_write(&mut self, page: &Page) -> io::Result<u32> {
        let page_id = self.allocate_page();
        let mut page = page.clone();
        page.header.page_id = page_id;
        page.update_checksum();
        self.write_page(page_id, &page)?;
        Ok(page_id)
    }

    /// Create a new internal node page
    pub fn new_internal_page(&self) -> Page {
        Page::new(PageType::Internal, self.allocate_page())
    }

    /// Create a new leaf node page
    pub fn new_leaf_page(&self) -> Page {
        Page::new(PageType::Leaf, self.allocate_page())
    }

    /// Get file size in bytes
    pub fn file_size(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    /// Truncate file (used for testing/cleanup)
    pub fn truncate(&mut self) -> io::Result<()> {
        self.file.set_len(0)?;
        self.next_page_id.store(1, Ordering::SeqCst);

        // Re-initialize with meta page
        let meta = MetaPage::default();
        let page = meta.to_page();
        self.write_page(0, &page)?;
        self.sync()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_page_manager_create() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.btree");

        let mut manager = PageManager::open(&path).unwrap();

        // Should have meta page
        assert_eq!(manager.page_count(), 1);

        let meta = manager.read_meta().unwrap();
        assert_eq!(meta.root_page_id, 0);
        assert_eq!(meta.page_count, 1);
    }

    #[test]
    fn test_page_manager_allocate() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.btree");

        let manager = PageManager::open(&path).unwrap();

        let page_id1 = manager.allocate_page();
        let page_id2 = manager.allocate_page();

        assert_eq!(page_id1, 1);
        assert_eq!(page_id2, 2);
        assert_eq!(manager.page_count(), 3);
    }

    #[test]
    fn test_page_manager_read_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.btree");

        let mut manager = PageManager::open(&path).unwrap();

        let mut page = Page::new(PageType::Leaf, 1);
        page.data[0..5].copy_from_slice(b"hello");
        page.update_checksum();

        manager.write_page(1, &page).unwrap();
        manager.sync().unwrap();

        let read_page = manager.read_page(1).unwrap();
        assert_eq!(&read_page.data[0..5], b"hello");
        assert!(read_page.verify_checksum());
    }

    #[test]
    fn test_page_manager_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.btree");

        {
            let mut manager = PageManager::open(&path).unwrap();
            let _page_id = manager.allocate_page();
            let _page_id = manager.allocate_page();

            let mut meta = manager.read_meta().unwrap();
            meta.page_count = manager.page_count();
            meta.root_page_id = 1;
            manager.write_meta(&meta).unwrap();
            manager.sync().unwrap();
        }

        // Reopen and verify state
        let mut manager = PageManager::open(&path).unwrap();
        assert_eq!(manager.page_count(), 3);

        let meta = manager.read_meta().unwrap();
        assert_eq!(meta.root_page_id, 1);
    }
}
