//! B-Tree node structures (leaf and internal nodes).
//!
//! Node format in page data:
//!
//! **Leaf Node:**
//! ```text
//! [entry_count: u16][next_leaf: u32][prev_leaf: u32]
//! [key_len: u16][value_len: u16][expire_at: u64][key_bytes...][value_bytes...]
//! ...
//! ```
//!
//! **Internal Node:**
//! ```text
//! [entry_count: u16][first_child: u32]
//! [key_len: u16][child_page: u32][key_bytes...]
//! ...
//! ```

use super::page::{MAX_DATA_SIZE, Page, PageType};
use std::io::{self, Read, Write};

/// Minimum fill factor for nodes (for split/merge decisions)
pub const MIN_FILL_FACTOR: f32 = 0.4;

/// Leaf node header size: entry_count(2) + next_leaf(4) + prev_leaf(4)
const LEAF_HEADER_SIZE: usize = 10;

/// Internal node header size: entry_count(2) + first_child(4)
const INTERNAL_HEADER_SIZE: usize = 6;

/// Entry overhead in leaf: key_len(2) + value_len(2) + expire_at(8)
const LEAF_ENTRY_OVERHEAD: usize = 12;

/// Entry overhead in internal: key_len(2) + child_page(4)
const INTERNAL_ENTRY_OVERHEAD: usize = 6;

/// A key-value entry in a leaf node
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafEntry {
    pub key: String,
    /// None means tombstone (deleted)
    pub value: Option<String>,
    /// Expiration timestamp (Unix seconds), 0 = no expiration
    pub expire_at: u64,
}

impl LeafEntry {
    pub fn new(key: String, value: String) -> Self {
        Self {
            key,
            value: Some(value),
            expire_at: 0,
        }
    }

    pub fn new_with_ttl(key: String, value: String, expire_at: u64) -> Self {
        Self {
            key,
            value: Some(value),
            expire_at,
        }
    }

    pub fn tombstone(key: String) -> Self {
        Self {
            key,
            value: None,
            expire_at: 0,
        }
    }

    pub fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    /// Check if entry is expired at the given timestamp
    pub fn is_expired(&self, now: u64) -> bool {
        self.expire_at > 0 && now > self.expire_at
    }

    /// Check if entry is valid (not tombstone and not expired)
    pub fn is_valid(&self, now: u64) -> bool {
        !self.is_tombstone() && !self.is_expired(now)
    }

    /// Size in bytes when serialized
    pub fn serialized_size(&self) -> usize {
        LEAF_ENTRY_OVERHEAD + self.key.len() + self.value.as_ref().map_or(0, |v| v.len())
    }
}

/// A key-child pointer entry in an internal node
#[derive(Debug, Clone)]
pub struct InternalEntry {
    pub key: String,
    pub child_page_id: u32,
}

impl InternalEntry {
    pub fn new(key: String, child_page_id: u32) -> Self {
        Self { key, child_page_id }
    }

    /// Size in bytes when serialized
    pub fn serialized_size(&self) -> usize {
        INTERNAL_ENTRY_OVERHEAD + self.key.len()
    }
}

/// Leaf node in B-tree
#[derive(Debug, Clone)]
pub struct LeafNode {
    /// Key-value entries, sorted by key
    pub entries: Vec<LeafEntry>,
    /// Next leaf page ID (for range scans), 0 if none
    pub next_leaf: u32,
    /// Previous leaf page ID, 0 if none
    pub prev_leaf: u32,
}

impl Default for LeafNode {
    fn default() -> Self {
        Self::new()
    }
}

impl LeafNode {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            next_leaf: 0,
            prev_leaf: 0,
        }
    }

    /// Load from page
    pub fn from_page(page: &Page) -> io::Result<Self> {
        if page.header.page_type != PageType::Leaf {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Expected leaf page",
            ));
        }

        let data = &page.data;
        let mut cursor = std::io::Cursor::new(data);

        let mut buf2 = [0u8; 2];
        let mut buf4 = [0u8; 4];
        let mut buf8 = [0u8; 8];

        cursor.read_exact(&mut buf2)?;
        let entry_count = u16::from_le_bytes(buf2) as usize;

        cursor.read_exact(&mut buf4)?;
        let next_leaf = u32::from_le_bytes(buf4);

        cursor.read_exact(&mut buf4)?;
        let prev_leaf = u32::from_le_bytes(buf4);

        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            cursor.read_exact(&mut buf2)?;
            let key_len = u16::from_le_bytes(buf2) as usize;

            cursor.read_exact(&mut buf2)?;
            let value_len = u16::from_le_bytes(buf2);

            cursor.read_exact(&mut buf8)?;
            let expire_at = u64::from_le_bytes(buf8);

            let mut key_buf = vec![0u8; key_len];
            cursor.read_exact(&mut key_buf)?;
            let key = String::from_utf8(key_buf).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid UTF-8 key: {}", e),
                )
            })?;

            let value = if value_len == u16::MAX {
                // Tombstone
                None
            } else {
                let mut value_buf = vec![0u8; value_len as usize];
                cursor.read_exact(&mut value_buf)?;
                Some(String::from_utf8(value_buf).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Invalid UTF-8 value: {}", e),
                    )
                })?)
            };

            entries.push(LeafEntry {
                key,
                value,
                expire_at,
            });
        }

        Ok(Self {
            entries,
            next_leaf,
            prev_leaf,
        })
    }

    /// Save to page
    pub fn to_page(&self, page_id: u32) -> io::Result<Page> {
        let mut page = Page::new(PageType::Leaf, page_id);
        page.header.item_count = self.entries.len() as u16;

        let mut cursor = std::io::Cursor::new(&mut page.data[..]);

        cursor.write_all(&(self.entries.len() as u16).to_le_bytes())?;
        cursor.write_all(&self.next_leaf.to_le_bytes())?;
        cursor.write_all(&self.prev_leaf.to_le_bytes())?;

        for entry in &self.entries {
            cursor.write_all(&(entry.key.len() as u16).to_le_bytes())?;

            if let Some(value) = &entry.value {
                cursor.write_all(&(value.len() as u16).to_le_bytes())?;
                cursor.write_all(&entry.expire_at.to_le_bytes())?;
                cursor.write_all(entry.key.as_bytes())?;
                cursor.write_all(value.as_bytes())?;
            } else {
                // Tombstone marker
                cursor.write_all(&u16::MAX.to_le_bytes())?;
                cursor.write_all(&entry.expire_at.to_le_bytes())?;
                cursor.write_all(entry.key.as_bytes())?;
            }
        }

        page.update_checksum();
        Ok(page)
    }

    /// Binary search for key, returns (found, index)
    pub fn search(&self, key: &str) -> (bool, usize) {
        match self.entries.binary_search_by(|e| e.key.as_str().cmp(key)) {
            Ok(idx) => (true, idx),
            Err(idx) => (false, idx),
        }
    }

    /// Get value for key
    pub fn get(&self, key: &str) -> Option<&LeafEntry> {
        let (found, idx) = self.search(key);
        if found {
            Some(&self.entries[idx])
        } else {
            None
        }
    }

    /// Insert or update entry
    pub fn insert(&mut self, entry: LeafEntry) -> bool {
        let (found, idx) = self.search(&entry.key);
        if found {
            self.entries[idx] = entry;
            false // Updated existing
        } else {
            self.entries.insert(idx, entry);
            true // Inserted new
        }
    }

    /// Delete entry by key
    pub fn delete(&mut self, key: &str) -> bool {
        let (found, idx) = self.search(key);
        if found {
            self.entries.remove(idx);
            true
        } else {
            false
        }
    }

    /// Current serialized size
    pub fn serialized_size(&self) -> usize {
        LEAF_HEADER_SIZE
            + self
                .entries
                .iter()
                .map(|e| e.serialized_size())
                .sum::<usize>()
    }

    /// Check if node would overflow with a new entry
    pub fn would_overflow(&self, entry: &LeafEntry) -> bool {
        self.serialized_size() + entry.serialized_size() > MAX_DATA_SIZE
    }

    /// Check if node needs splitting
    pub fn needs_split(&self) -> bool {
        self.serialized_size() > MAX_DATA_SIZE
    }

    /// Check if node is underfull (needs merging)
    pub fn is_underfull(&self) -> bool {
        self.entries.is_empty()
            || (self.serialized_size() as f32) < (MAX_DATA_SIZE as f32 * MIN_FILL_FACTOR)
    }

    /// Clean up expired and tombstone entries, returns number of entries removed
    pub fn cleanup_expired(&mut self, now: u64) -> usize {
        let before = self.entries.len();
        self.entries.retain(|entry| entry.is_valid(now));
        before - self.entries.len()
    }

    /// Check if split is still needed after cleanup
    pub fn needs_split_after_cleanup(&mut self, now: u64) -> bool {
        if !self.needs_split() {
            return false;
        }

        // Try cleanup first
        let removed = self.cleanup_expired(now);
        if removed > 0 {
            // Re-check after cleanup
            return self.needs_split();
        }

        true
    }

    /// Split node, returns (left, right, split_key)
    pub fn split(&mut self) -> (LeafNode, String) {
        let mid = self.entries.len() / 2;
        let right_entries: Vec<LeafEntry> = self.entries.drain(mid..).collect();
        let split_key = right_entries[0].key.clone();

        let right = LeafNode {
            entries: right_entries,
            next_leaf: self.next_leaf,
            prev_leaf: 0, // Will be set by caller
        };

        self.next_leaf = 0; // Will be set by caller

        (right, split_key)
    }

    /// Split with cleanup: first cleanup expired entries, then split if still needed
    /// Returns None if cleanup avoided the split, Some((right, split_key)) if split occurred
    pub fn split_with_cleanup(&mut self, now: u64) -> Option<(LeafNode, String)> {
        if !self.needs_split_after_cleanup(now) {
            return None;
        }
        Some(self.split())
    }

    /// Merge with sibling (self becomes merged node)
    pub fn merge(&mut self, sibling: &LeafNode) {
        self.entries.extend(sibling.entries.clone());
        self.next_leaf = sibling.next_leaf;
    }
}

/// Internal node in B-tree
#[derive(Debug, Clone)]
pub struct InternalNode {
    /// Key-child entries, sorted by key
    /// Each entry.child_page_id points to the subtree with keys >= entry.key
    pub entries: Vec<InternalEntry>,
    /// Pointer to leftmost child (keys < entries[0].key)
    pub first_child: u32,
}

impl Default for InternalNode {
    fn default() -> Self {
        Self::new()
    }
}

impl InternalNode {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            first_child: 0,
        }
    }

    /// Create internal node with initial children
    pub fn with_children(first_child: u32, key: String, second_child: u32) -> Self {
        Self {
            entries: vec![InternalEntry::new(key, second_child)],
            first_child,
        }
    }

    /// Load from page
    pub fn from_page(page: &Page) -> io::Result<Self> {
        if page.header.page_type != PageType::Internal {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Expected internal page",
            ));
        }

        let data = &page.data;
        let mut cursor = std::io::Cursor::new(data);

        let mut buf2 = [0u8; 2];
        let mut buf4 = [0u8; 4];

        cursor.read_exact(&mut buf2)?;
        let entry_count = u16::from_le_bytes(buf2) as usize;

        cursor.read_exact(&mut buf4)?;
        let first_child = u32::from_le_bytes(buf4);

        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            cursor.read_exact(&mut buf2)?;
            let key_len = u16::from_le_bytes(buf2) as usize;

            cursor.read_exact(&mut buf4)?;
            let child_page_id = u32::from_le_bytes(buf4);

            let mut key_buf = vec![0u8; key_len];
            cursor.read_exact(&mut key_buf)?;
            let key = String::from_utf8(key_buf).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid UTF-8 key: {}", e),
                )
            })?;

            entries.push(InternalEntry { key, child_page_id });
        }

        Ok(Self {
            entries,
            first_child,
        })
    }

    /// Save to page
    pub fn to_page(&self, page_id: u32) -> io::Result<Page> {
        let mut page = Page::new(PageType::Internal, page_id);
        page.header.item_count = self.entries.len() as u16;

        let mut cursor = std::io::Cursor::new(&mut page.data[..]);

        cursor.write_all(&(self.entries.len() as u16).to_le_bytes())?;
        cursor.write_all(&self.first_child.to_le_bytes())?;

        for entry in &self.entries {
            cursor.write_all(&(entry.key.len() as u16).to_le_bytes())?;
            cursor.write_all(&entry.child_page_id.to_le_bytes())?;
            cursor.write_all(entry.key.as_bytes())?;
        }

        page.update_checksum();
        Ok(page)
    }

    /// Find child page for key
    /// In a B-tree internal node:
    /// - first_child points to keys < entries[0].key
    /// - entries[i].child_page_id points to keys >= entries[i].key
    pub fn find_child(&self, key: &str) -> u32 {
        // Find the first entry where key < entry.key
        for (i, entry) in self.entries.iter().enumerate() {
            if key < entry.key.as_str() {
                // key belongs to the child before this entry
                if i == 0 {
                    return self.first_child;
                } else {
                    return self.entries[i - 1].child_page_id;
                }
            }
        }

        // key >= all entry keys, return last entry's child
        self.entries
            .last()
            .map_or(self.first_child, |e| e.child_page_id)
    }

    /// Find child page index for key (for traversal)
    pub fn find_child_index(&self, key: &str) -> usize {
        match self.entries.binary_search_by(|e| e.key.as_str().cmp(key)) {
            Ok(idx) => idx + 1, // Key found, go to right child
            Err(idx) => idx,    // Key not found, idx is insertion point
        }
    }

    /// Get child page ID at index
    pub fn get_child(&self, index: usize) -> u32 {
        if index == 0 {
            self.first_child
        } else {
            self.entries[index - 1].child_page_id
        }
    }

    /// Set child page ID at index
    pub fn set_child(&mut self, index: usize, page_id: u32) {
        if index == 0 {
            self.first_child = page_id;
        } else {
            self.entries[index - 1].child_page_id = page_id;
        }
    }

    /// Number of children
    pub fn child_count(&self) -> usize {
        self.entries.len() + 1
    }

    /// Insert a new key and right child after a split
    pub fn insert_after_split(&mut self, key: String, right_child: u32) {
        let idx = match self
            .entries
            .binary_search_by(|e| e.key.as_str().cmp(key.as_str()))
        {
            Ok(idx) | Err(idx) => idx,
        };
        self.entries
            .insert(idx, InternalEntry::new(key, right_child));
    }

    /// Current serialized size
    pub fn serialized_size(&self) -> usize {
        INTERNAL_HEADER_SIZE
            + self
                .entries
                .iter()
                .map(|e| e.serialized_size())
                .sum::<usize>()
    }

    /// Check if node needs splitting
    pub fn needs_split(&self) -> bool {
        self.serialized_size() > MAX_DATA_SIZE
    }

    /// Check if node is underfull
    pub fn is_underfull(&self) -> bool {
        self.entries.is_empty()
            || (self.serialized_size() as f32) < (MAX_DATA_SIZE as f32 * MIN_FILL_FACTOR)
    }

    /// Split node, returns (right_node, promoted_key)
    pub fn split(&mut self) -> (InternalNode, String) {
        let mid = self.entries.len() / 2;

        // The middle key is promoted to parent
        let promoted_key = self.entries[mid].key.clone();

        // Right node gets entries after mid, with first_child = mid's child
        let right_first_child = self.entries[mid].child_page_id;
        let right_entries: Vec<InternalEntry> = self.entries.drain(mid + 1..).collect();

        // Remove the promoted entry from left
        self.entries.pop();

        let right = InternalNode {
            entries: right_entries,
            first_child: right_first_child,
        };

        (right, promoted_key)
    }

    /// Merge with sibling
    pub fn merge(&mut self, separator_key: String, sibling: &InternalNode) {
        self.entries
            .push(InternalEntry::new(separator_key, sibling.first_child));
        self.entries.extend(sibling.entries.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leaf_node_roundtrip() {
        let mut node = LeafNode::new();
        node.insert(LeafEntry::new("key1".to_string(), "value1".to_string()));
        node.insert(LeafEntry::new("key2".to_string(), "value2".to_string()));
        node.insert(LeafEntry::tombstone("key3".to_string()));
        node.next_leaf = 5;
        node.prev_leaf = 3;

        let page = node.to_page(1).unwrap();
        let decoded = LeafNode::from_page(&page).unwrap();

        assert_eq!(decoded.entries.len(), 3);
        assert_eq!(decoded.entries[0].key, "key1");
        assert_eq!(decoded.entries[0].value, Some("value1".to_string()));
        assert_eq!(decoded.entries[2].key, "key3");
        assert!(decoded.entries[2].is_tombstone());
        assert_eq!(decoded.next_leaf, 5);
        assert_eq!(decoded.prev_leaf, 3);
    }

    #[test]
    fn test_leaf_node_search() {
        let mut node = LeafNode::new();
        node.insert(LeafEntry::new("apple".to_string(), "red".to_string()));
        node.insert(LeafEntry::new("banana".to_string(), "yellow".to_string()));
        node.insert(LeafEntry::new("cherry".to_string(), "red".to_string()));

        assert_eq!(node.get("apple").unwrap().value, Some("red".to_string()));
        assert_eq!(
            node.get("banana").unwrap().value,
            Some("yellow".to_string())
        );
        assert!(node.get("date").is_none());
    }

    #[test]
    fn test_leaf_node_split() {
        let mut node = LeafNode::new();
        for i in 0..10 {
            node.insert(LeafEntry::new(
                format!("key{:02}", i),
                format!("value{}", i),
            ));
        }

        let (right, split_key) = node.split();

        assert_eq!(node.entries.len(), 5);
        assert_eq!(right.entries.len(), 5);
        assert_eq!(split_key, "key05");
        assert!(node.entries.last().unwrap().key < split_key);
        assert!(right.entries.first().unwrap().key >= split_key);
    }

    #[test]
    fn test_internal_node_roundtrip() {
        let mut node = InternalNode::new();
        node.first_child = 1;
        node.entries.push(InternalEntry::new("key1".to_string(), 2));
        node.entries.push(InternalEntry::new("key2".to_string(), 3));

        let page = node.to_page(10).unwrap();
        let decoded = InternalNode::from_page(&page).unwrap();

        assert_eq!(decoded.first_child, 1);
        assert_eq!(decoded.entries.len(), 2);
        assert_eq!(decoded.entries[0].key, "key1");
        assert_eq!(decoded.entries[0].child_page_id, 2);
    }

    #[test]
    fn test_internal_node_find_child() {
        let node = InternalNode::with_children(1, "key1".to_string(), 2);
        let mut node = node;
        node.insert_after_split("key2".to_string(), 3);
        node.insert_after_split("key3".to_string(), 4);

        // Keys structure: [first_child=1] key1 [2] key2 [3] key3 [4]
        assert_eq!(node.find_child("aaa"), 1); // < key1
        assert_eq!(node.find_child("key1"), 2); // >= key1, < key2
        assert_eq!(node.find_child("key15"), 2); // >= key1, < key2
        assert_eq!(node.find_child("key2"), 3); // >= key2, < key3
        assert_eq!(node.find_child("key3"), 4); // >= key3
        assert_eq!(node.find_child("zzz"), 4); // >= key3
    }

    #[test]
    fn test_internal_node_split() {
        let mut node = InternalNode::new();
        node.first_child = 0;
        for i in 0..10 {
            node.entries
                .push(InternalEntry::new(format!("key{:02}", i), i as u32 + 1));
        }

        let (right, promoted) = node.split();

        // For 10 entries [0..9], mid = 5:
        // - promoted = key05
        // - left keeps entries [0..4] after drain(6..) and pop() = 5 entries
        // - right gets entries [6..9] = 4 entries, with first_child from entry[5]
        assert_eq!(node.entries.len(), 5);
        assert_eq!(right.entries.len(), 4);
        assert_eq!(promoted, "key05");
    }
}
