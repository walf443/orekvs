//! Freelist for managing deallocated pages.
//!
//! Tracks pages that have been freed and can be reused for new allocations.
//! Uses a simple linked-list approach with pages storing lists of free page IDs.

use super::page::{MAX_DATA_SIZE, Page, PageType};
use super::page_manager::PageManager;
use std::collections::VecDeque;
use std::io::{self, Read, Write};

/// Number of page IDs that can fit in a freelist page
/// Each ID is 4 bytes, header uses 4 bytes for next pointer
const IDS_PER_PAGE: usize = (MAX_DATA_SIZE - 4) / 4;

/// Freelist page format:
/// [next_page: u32][id_count: u32][page_ids: u32...]
#[derive(Debug, Clone)]
pub struct FreelistPage {
    /// Next freelist page ID (0 if none)
    pub next_page: u32,
    /// Free page IDs stored in this page
    pub page_ids: Vec<u32>,
}

impl FreelistPage {
    pub fn new() -> Self {
        Self {
            next_page: 0,
            page_ids: Vec::new(),
        }
    }

    pub fn from_page(page: &Page) -> io::Result<Self> {
        if page.header.page_type != PageType::Freelist {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Expected freelist page",
            ));
        }

        let mut cursor = std::io::Cursor::new(&page.data[..]);
        let mut buf4 = [0u8; 4];

        cursor.read_exact(&mut buf4)?;
        let next_page = u32::from_le_bytes(buf4);

        cursor.read_exact(&mut buf4)?;
        let id_count = u32::from_le_bytes(buf4) as usize;

        let mut page_ids = Vec::with_capacity(id_count);
        for _ in 0..id_count {
            cursor.read_exact(&mut buf4)?;
            page_ids.push(u32::from_le_bytes(buf4));
        }

        Ok(Self {
            next_page,
            page_ids,
        })
    }

    pub fn to_page(&self, page_id: u32) -> io::Result<Page> {
        let mut page = Page::new(PageType::Freelist, page_id);
        page.header.item_count = self.page_ids.len() as u16;

        let mut cursor = std::io::Cursor::new(&mut page.data[..]);

        cursor.write_all(&self.next_page.to_le_bytes())?;
        cursor.write_all(&(self.page_ids.len() as u32).to_le_bytes())?;

        for &id in &self.page_ids {
            cursor.write_all(&id.to_le_bytes())?;
        }

        page.update_checksum();
        Ok(page)
    }

    pub fn is_full(&self) -> bool {
        self.page_ids.len() >= IDS_PER_PAGE
    }

    pub fn is_empty(&self) -> bool {
        self.page_ids.is_empty()
    }
}

impl Default for FreelistPage {
    fn default() -> Self {
        Self::new()
    }
}

/// In-memory freelist manager
#[derive(Debug)]
pub struct Freelist {
    /// Free pages available for allocation (in-memory queue)
    free_pages: VecDeque<u32>,
    /// Head of on-disk freelist chain
    head_page_id: u32,
    /// Whether freelist has been modified
    dirty: bool,
}

impl Freelist {
    /// Create empty freelist
    pub fn new() -> Self {
        Self {
            free_pages: VecDeque::new(),
            head_page_id: 0,
            dirty: false,
        }
    }

    /// Load freelist from disk
    pub fn load(page_manager: &mut PageManager, head_page_id: u32) -> io::Result<Self> {
        let mut free_pages = VecDeque::new();
        let mut current_page_id = head_page_id;

        while current_page_id != 0 {
            let page = page_manager.read_page(current_page_id)?;
            let fl_page = FreelistPage::from_page(&page)?;

            for &id in &fl_page.page_ids {
                free_pages.push_back(id);
            }

            current_page_id = fl_page.next_page;
        }

        Ok(Self {
            free_pages,
            head_page_id,
            dirty: false,
        })
    }

    /// Get head page ID
    pub fn head_page_id(&self) -> u32 {
        self.head_page_id
    }

    /// Check if there are free pages available
    pub fn has_free_pages(&self) -> bool {
        !self.free_pages.is_empty()
    }

    /// Get number of free pages
    pub fn free_count(&self) -> usize {
        self.free_pages.len()
    }

    /// Allocate a page from freelist
    pub fn allocate(&mut self) -> Option<u32> {
        let page_id = self.free_pages.pop_front()?;
        self.dirty = true;
        Some(page_id)
    }

    /// Return a page to the freelist
    pub fn free(&mut self, page_id: u32) {
        self.free_pages.push_back(page_id);
        self.dirty = true;
    }

    /// Check if freelist has been modified
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Save freelist to disk
    pub fn save(&mut self, page_manager: &mut PageManager) -> io::Result<u32> {
        if self.free_pages.is_empty() {
            // No free pages, clear head
            self.head_page_id = 0;
            self.dirty = false;
            return Ok(0);
        }

        // Create freelist pages
        let mut pages: Vec<FreelistPage> = Vec::new();
        let mut current_page = FreelistPage::new();

        for &page_id in &self.free_pages {
            if current_page.is_full() {
                pages.push(current_page);
                current_page = FreelistPage::new();
            }
            current_page.page_ids.push(page_id);
        }

        if !current_page.is_empty() {
            pages.push(current_page);
        }

        // Allocate page IDs for freelist pages and link them
        let mut page_ids: Vec<u32> = Vec::with_capacity(pages.len());
        for _ in 0..pages.len() {
            page_ids.push(page_manager.allocate_page());
        }

        // Set next pointers and write pages
        let pages_len = pages.len();
        for (i, fl_page) in pages.iter_mut().enumerate() {
            fl_page.next_page = if i + 1 < pages_len {
                page_ids[i + 1]
            } else {
                0
            };

            let page = fl_page.to_page(page_ids[i])?;
            page_manager.write_page(page_ids[i], &page)?;
        }

        self.head_page_id = page_ids[0];
        self.dirty = false;

        Ok(self.head_page_id)
    }

    /// Clear all free pages (used during recovery)
    pub fn clear(&mut self) {
        self.free_pages.clear();
        self.head_page_id = 0;
        self.dirty = true;
    }
}

impl Default for Freelist {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_freelist_page_roundtrip() {
        let mut fl_page = FreelistPage::new();
        fl_page.next_page = 5;
        fl_page.page_ids = vec![10, 20, 30, 40];

        let page = fl_page.to_page(1).unwrap();
        let decoded = FreelistPage::from_page(&page).unwrap();

        assert_eq!(decoded.next_page, 5);
        assert_eq!(decoded.page_ids, vec![10, 20, 30, 40]);
    }

    #[test]
    fn test_freelist_allocate_free() {
        let mut fl = Freelist::new();

        fl.free(10);
        fl.free(20);
        fl.free(30);

        assert_eq!(fl.free_count(), 3);
        assert_eq!(fl.allocate(), Some(10));
        assert_eq!(fl.allocate(), Some(20));
        assert_eq!(fl.free_count(), 1);
    }

    #[test]
    fn test_freelist_save_load() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.btree");

        let head_page_id = {
            let mut pm = PageManager::open(&path).unwrap();
            let mut fl = Freelist::new();

            fl.free(10);
            fl.free(20);
            fl.free(30);

            fl.save(&mut pm).unwrap()
        };

        // Reload
        let mut pm = PageManager::open(&path).unwrap();
        let fl = Freelist::load(&mut pm, head_page_id).unwrap();

        assert_eq!(fl.free_count(), 3);
    }
}
