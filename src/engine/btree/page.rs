//! Page structure and serialization for B-Tree storage.
//!
//! Each page is a fixed 4KB block stored on disk. Pages contain:
//! - Header: page type, flags, item count
//! - Body: type-specific data (keys/values for leaf, keys/children for internal)

use std::io::{self, Read, Write};

/// Page size in bytes (4KB)
pub const PAGE_SIZE: usize = 4096;

/// Magic number for page validation
pub const PAGE_MAGIC: u32 = 0x4254_5245; // "BTRE"

/// Page version for format compatibility
pub const PAGE_VERSION: u8 = 1;

/// Page header size in bytes
/// Layout: magic(4) + page_type(1) + version(1) + flags(2) + item_count(2) + page_id(4) + checksum(4) = 18
pub const HEADER_SIZE: usize = 18;

/// Maximum usable space in a page for data
pub const MAX_DATA_SIZE: usize = PAGE_SIZE - HEADER_SIZE;

/// Page type identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    /// Metadata page (page 0, contains root pointer, freelist head, etc.)
    Meta = 0,
    /// Internal B-tree node (keys + child pointers)
    Internal = 1,
    /// Leaf B-tree node (keys + values)
    Leaf = 2,
    /// Freelist page (list of free page IDs)
    Freelist = 3,
    /// Overflow page for large values
    Overflow = 4,
}

impl TryFrom<u8> for PageType {
    type Error = io::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(PageType::Meta),
            1 => Ok(PageType::Internal),
            2 => Ok(PageType::Leaf),
            3 => Ok(PageType::Freelist),
            4 => Ok(PageType::Overflow),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid page type: {}", value),
            )),
        }
    }
}

/// Page header structure (16 bytes)
#[derive(Debug, Clone, Copy)]
pub struct PageHeader {
    /// Magic number for validation
    pub magic: u32,
    /// Page type
    pub page_type: PageType,
    /// Format version
    pub version: u8,
    /// Page flags (reserved for future use)
    pub flags: u16,
    /// Number of items in this page
    pub item_count: u16,
    /// Page ID (for validation)
    pub page_id: u32,
    /// Checksum of page data (CRC32)
    pub checksum: u32,
}

impl Default for PageHeader {
    fn default() -> Self {
        Self {
            magic: PAGE_MAGIC,
            page_type: PageType::Leaf,
            version: PAGE_VERSION,
            flags: 0,
            item_count: 0,
            page_id: 0,
            checksum: 0,
        }
    }
}

impl PageHeader {
    pub fn new(page_type: PageType, page_id: u32) -> Self {
        Self {
            magic: PAGE_MAGIC,
            page_type,
            version: PAGE_VERSION,
            flags: 0,
            item_count: 0,
            page_id,
            checksum: 0,
        }
    }

    /// Serialize header to bytes
    pub fn serialize(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4] = self.page_type as u8;
        buf[5] = self.version;
        buf[6..8].copy_from_slice(&self.flags.to_le_bytes());
        buf[8..10].copy_from_slice(&self.item_count.to_le_bytes());
        buf[10..14].copy_from_slice(&self.page_id.to_le_bytes());
        buf[14..18].copy_from_slice(&self.checksum.to_le_bytes());
        buf
    }

    /// Deserialize header from bytes
    pub fn deserialize(buf: &[u8]) -> io::Result<Self> {
        if buf.len() < HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Buffer too small for page header",
            ));
        }

        let magic = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if magic != PAGE_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid page magic: {:#x}", magic),
            ));
        }

        let page_type = PageType::try_from(buf[4])?;
        let version = buf[5];
        if version != PAGE_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unsupported page version: {}", version),
            ));
        }

        Ok(Self {
            magic,
            page_type,
            version,
            flags: u16::from_le_bytes([buf[6], buf[7]]),
            item_count: u16::from_le_bytes([buf[8], buf[9]]),
            page_id: u32::from_le_bytes([buf[10], buf[11], buf[12], buf[13]]),
            checksum: u32::from_le_bytes([buf[14], buf[15], buf[16], buf[17]]),
        })
    }
}

/// Raw page data (4KB buffer)
#[derive(Clone)]
pub struct Page {
    pub header: PageHeader,
    pub data: Vec<u8>,
}

impl Page {
    /// Create a new empty page
    pub fn new(page_type: PageType, page_id: u32) -> Self {
        Self {
            header: PageHeader::new(page_type, page_id),
            data: vec![0u8; MAX_DATA_SIZE],
        }
    }

    /// Create page from raw bytes
    pub fn from_bytes(buf: &[u8]) -> io::Result<Self> {
        if buf.len() != PAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid page size: {} (expected {})", buf.len(), PAGE_SIZE),
            ));
        }

        let header = PageHeader::deserialize(&buf[0..HEADER_SIZE])?;
        let mut data = vec![0u8; MAX_DATA_SIZE];
        data.copy_from_slice(&buf[HEADER_SIZE..]);

        Ok(Self { header, data })
    }

    /// Serialize page to bytes
    pub fn to_bytes(&self) -> [u8; PAGE_SIZE] {
        let mut buf = [0u8; PAGE_SIZE];
        buf[0..HEADER_SIZE].copy_from_slice(&self.header.serialize());
        buf[HEADER_SIZE..].copy_from_slice(&self.data[..MAX_DATA_SIZE]);
        buf
    }

    /// Calculate CRC32 checksum of data
    pub fn calculate_checksum(&self) -> u32 {
        crc32fast::hash(&self.data)
    }

    /// Update header checksum
    pub fn update_checksum(&mut self) {
        self.header.checksum = self.calculate_checksum();
    }

    /// Verify page checksum
    pub fn verify_checksum(&self) -> bool {
        // Checksum of 0 means not yet computed
        self.header.checksum == 0 || self.header.checksum == self.calculate_checksum()
    }
}

/// Metadata page structure (page 0)
/// Contains global B-tree state
#[derive(Debug, Clone, Copy)]
pub struct MetaPage {
    /// Root page ID (0 if tree is empty)
    pub root_page_id: u32,
    /// Head of freelist
    pub freelist_page_id: u32,
    /// Total number of pages allocated
    pub page_count: u32,
    /// Tree height (0 for empty tree, 1 for root-only)
    pub tree_height: u32,
    /// Total number of key-value pairs
    pub entry_count: u64,
    /// Last WAL sequence number
    pub last_wal_seq: u64,
}

impl Default for MetaPage {
    fn default() -> Self {
        Self {
            root_page_id: 0,
            freelist_page_id: 0,
            page_count: 1, // Meta page itself
            tree_height: 0,
            entry_count: 0,
            last_wal_seq: 0,
        }
    }
}

impl MetaPage {
    /// Serialize to page data
    pub fn to_page(&self) -> Page {
        let mut page = Page::new(PageType::Meta, 0);
        let mut cursor = std::io::Cursor::new(&mut page.data[..]);

        cursor.write_all(&self.root_page_id.to_le_bytes()).unwrap();
        cursor
            .write_all(&self.freelist_page_id.to_le_bytes())
            .unwrap();
        cursor.write_all(&self.page_count.to_le_bytes()).unwrap();
        cursor.write_all(&self.tree_height.to_le_bytes()).unwrap();
        cursor.write_all(&self.entry_count.to_le_bytes()).unwrap();
        cursor.write_all(&self.last_wal_seq.to_le_bytes()).unwrap();

        page.update_checksum();
        page
    }

    /// Deserialize from page
    pub fn from_page(page: &Page) -> io::Result<Self> {
        if page.header.page_type != PageType::Meta {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Expected meta page",
            ));
        }

        let mut cursor = std::io::Cursor::new(&page.data[..]);
        let mut buf4 = [0u8; 4];
        let mut buf8 = [0u8; 8];

        cursor.read_exact(&mut buf4)?;
        let root_page_id = u32::from_le_bytes(buf4);

        cursor.read_exact(&mut buf4)?;
        let freelist_page_id = u32::from_le_bytes(buf4);

        cursor.read_exact(&mut buf4)?;
        let page_count = u32::from_le_bytes(buf4);

        cursor.read_exact(&mut buf4)?;
        let tree_height = u32::from_le_bytes(buf4);

        cursor.read_exact(&mut buf8)?;
        let entry_count = u64::from_le_bytes(buf8);

        cursor.read_exact(&mut buf8)?;
        let last_wal_seq = u64::from_le_bytes(buf8);

        Ok(Self {
            root_page_id,
            freelist_page_id,
            page_count,
            tree_height,
            entry_count,
            last_wal_seq,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_header_roundtrip() {
        let header = PageHeader::new(PageType::Leaf, 42);
        let buf = header.serialize();
        let decoded = PageHeader::deserialize(&buf).unwrap();

        assert_eq!(decoded.magic, PAGE_MAGIC);
        assert_eq!(decoded.page_type, PageType::Leaf);
        assert_eq!(decoded.page_id, 42);
    }

    #[test]
    fn test_page_roundtrip() {
        let mut page = Page::new(PageType::Internal, 123);
        page.data[0..5].copy_from_slice(b"hello");
        page.update_checksum();

        let buf = page.to_bytes();
        let decoded = Page::from_bytes(&buf).unwrap();

        assert_eq!(decoded.header.page_type, PageType::Internal);
        assert_eq!(decoded.header.page_id, 123);
        assert_eq!(&decoded.data[0..5], b"hello");
        assert!(decoded.verify_checksum());
    }

    #[test]
    fn test_meta_page_roundtrip() {
        let meta = MetaPage {
            root_page_id: 1,
            freelist_page_id: 10,
            page_count: 100,
            tree_height: 3,
            entry_count: 50000,
            last_wal_seq: 12345,
        };

        let page = meta.to_page();
        let decoded = MetaPage::from_page(&page).unwrap();

        assert_eq!(decoded.root_page_id, 1);
        assert_eq!(decoded.freelist_page_id, 10);
        assert_eq!(decoded.page_count, 100);
        assert_eq!(decoded.tree_height, 3);
        assert_eq!(decoded.entry_count, 50000);
        assert_eq!(decoded.last_wal_seq, 12345);
    }
}
