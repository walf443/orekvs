//! Safe wrapper around memory-mapped files.
//!
//! This module encapsulates all unsafe mmap operations and provides a safe public API.
//! The underlying mmap is read-only and the file must not be modified while mapped.

use memmap2::Advice;
use std::fs::File;
use std::io;
use std::ops::Deref;
use std::path::Path;

/// A read-only memory-mapped file.
///
/// This struct provides safe access to memory-mapped file contents.
/// The mapping is read-only and will remain valid as long as this struct exists.
pub struct MappedFile {
    // The inner mmap handle. This is the only place where unsafe code is used.
    inner: memmap2::Mmap,
    // Keep the file handle alive to ensure the mapping remains valid
    _file: File,
}

impl MappedFile {
    /// Open a file and create a read-only memory mapping.
    ///
    /// # Errors
    /// Returns an error if the file cannot be opened or mapped.
    ///
    /// # Safety Guarantees
    /// - The file is opened in read-only mode
    /// - The mapping is read-only
    /// - The file handle is kept alive for the lifetime of the mapping
    ///
    /// # Caveats
    /// - If the file is modified or truncated by another process while mapped,
    ///   accessing the mapped memory may cause undefined behavior (SIGBUS).
    /// - This is generally safe for SSTable files which are immutable after creation.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path)?;

        // SAFETY: The file is opened read-only and will not be modified.
        // SSTable files are immutable after creation.
        // We keep the File handle alive to ensure the mapping remains valid.
        let inner = unsafe { memmap2::Mmap::map(&file)? };

        Ok(Self { inner, _file: file })
    }

    /// Returns the length of the mapped region in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if the mapped region is empty.
    #[inline]
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Get a slice of bytes from the mapped file.
    ///
    /// # Panics
    /// Panics if the range is out of bounds.
    #[inline]
    pub fn slice(&self, start: usize, end: usize) -> &[u8] {
        &self.inner[start..end]
    }

    /// Get a slice of bytes from the mapped file, with bounds checking.
    ///
    /// Returns `None` if the range is out of bounds.
    #[inline]
    pub fn get_slice(&self, start: usize, end: usize) -> Option<&[u8]> {
        if end <= self.inner.len() && start <= end {
            Some(&self.inner[start..end])
        } else {
            None
        }
    }

    /// Read bytes at the given offset with the specified length.
    ///
    /// Returns `None` if the range would exceed the file size.
    #[inline]
    pub fn read_at(&self, offset: usize, len: usize) -> Option<&[u8]> {
        let end = offset.checked_add(len)?;
        self.get_slice(offset, end)
    }

    /// Advise the OS that the file will be accessed randomly.
    /// This disables read-ahead which may be wasteful for point lookups.
    pub fn advise_random(&self) -> io::Result<()> {
        self.inner.advise(Advice::Random)
    }
}

/// Allow treating MappedFile as a byte slice
impl Deref for MappedFile {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl AsRef<[u8]> for MappedFile {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_mmap_basic() {
        let mut temp = NamedTempFile::new().unwrap();
        temp.write_all(b"Hello, World!").unwrap();
        temp.flush().unwrap();

        let mapped = MappedFile::open(temp.path()).unwrap();

        assert_eq!(mapped.len(), 13);
        assert!(!mapped.is_empty());
        assert_eq!(&*mapped, b"Hello, World!");
    }

    #[test]
    fn test_mmap_slice() {
        let mut temp = NamedTempFile::new().unwrap();
        temp.write_all(b"0123456789").unwrap();
        temp.flush().unwrap();

        let mapped = MappedFile::open(temp.path()).unwrap();

        assert_eq!(mapped.slice(0, 5), b"01234");
        assert_eq!(mapped.slice(5, 10), b"56789");
        assert_eq!(mapped.read_at(3, 4), Some(&b"3456"[..]));
    }

    #[test]
    fn test_mmap_get_slice_bounds() {
        let mut temp = NamedTempFile::new().unwrap();
        temp.write_all(b"test").unwrap();
        temp.flush().unwrap();

        let mapped = MappedFile::open(temp.path()).unwrap();

        assert!(mapped.get_slice(0, 4).is_some());
        assert!(mapped.get_slice(0, 5).is_none()); // out of bounds
        assert!(mapped.get_slice(5, 10).is_none()); // out of bounds
        assert!(mapped.read_at(2, 10).is_none()); // would overflow
    }

    #[test]
    fn test_mmap_empty_file() {
        let temp = NamedTempFile::new().unwrap();

        let mapped = MappedFile::open(temp.path()).unwrap();

        assert_eq!(mapped.len(), 0);
        assert!(mapped.is_empty());
    }

    #[test]
    fn test_mmap_deref() {
        let mut temp = NamedTempFile::new().unwrap();
        temp.write_all(b"deref test").unwrap();
        temp.flush().unwrap();

        let mapped = MappedFile::open(temp.path()).unwrap();

        // Test Deref trait
        let slice: &[u8] = &mapped;
        assert_eq!(slice, b"deref test");

        // Test AsRef trait
        let as_ref: &[u8] = mapped.as_ref();
        assert_eq!(as_ref, b"deref test");
    }
}
