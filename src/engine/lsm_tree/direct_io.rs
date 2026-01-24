//! Platform-abstracted Direct I/O utilities.
//!
//! This module provides a unified interface for bypassing the OS page cache
//! across different platforms:
//! - macOS: Uses F_NOCACHE via fcntl
//! - Linux: Uses O_DIRECT (requires aligned buffers)
//! - Other platforms: No-op (graceful fallback)

use std::fs::File;
use std::io;

/// Disable page cache for the given file.
///
/// This tells the OS not to cache file data in the page cache,
/// which is useful for:
/// - Reducing memory pressure from large writes (e.g., WAL)
/// - Avoiding double-buffering when the application has its own cache
/// - More predictable I/O latency
///
/// # Platform behavior
/// - **macOS**: Uses `F_NOCACHE` fcntl flag
/// - **Linux**: Uses `fadvise` with `POSIX_FADV_DONTNEED` (O_DIRECT requires aligned I/O)
/// - **Other**: No-op, returns Ok(())
///
/// # Example
/// ```ignore
/// let file = File::create("wal.log")?;
/// direct_io::disable_page_cache(&file)?;
/// ```
pub fn disable_page_cache(file: &File) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        disable_page_cache_macos(file)
    }

    #[cfg(target_os = "linux")]
    {
        disable_page_cache_linux(file)
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        // Graceful fallback for unsupported platforms
        let _ = file;
        Ok(())
    }
}

/// macOS implementation using F_NOCACHE
#[cfg(target_os = "macos")]
fn disable_page_cache_macos(file: &File) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;

    // F_NOCACHE: Turns off data caching for this file
    // The file data is not cached in the unified buffer cache
    let ret = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };

    if ret == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Linux implementation using fadvise
/// Note: O_DIRECT requires aligned buffers which is more complex.
/// fadvise with DONTNEED provides similar benefit without alignment requirements.
#[cfg(target_os = "linux")]
fn disable_page_cache_linux(file: &File) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;

    // POSIX_FADV_DONTNEED: The specified data will not be accessed in the near future
    // This hints the kernel to free the pages from cache
    let ret = unsafe {
        libc::posix_fadvise(
            file.as_raw_fd(),
            0,
            0, // 0 means entire file
            libc::POSIX_FADV_DONTNEED,
        )
    };

    if ret != 0 {
        Err(io::Error::from_raw_os_error(ret))
    } else {
        Ok(())
    }
}

/// Check if direct I/O is supported on this platform
#[allow(dead_code)]
pub fn is_supported() -> bool {
    cfg!(any(target_os = "macos", target_os = "linux"))
}

/// Get the name of the direct I/O method used on this platform
#[allow(dead_code)]
pub fn method_name() -> &'static str {
    #[cfg(target_os = "macos")]
    {
        "F_NOCACHE"
    }

    #[cfg(target_os = "linux")]
    {
        "fadvise(DONTNEED)"
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        "none"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_disable_page_cache() {
        let mut temp = NamedTempFile::new().unwrap();
        temp.write_all(b"test data").unwrap();

        // Should not fail on any platform
        let result = disable_page_cache(temp.as_file());
        assert!(result.is_ok(), "disable_page_cache failed: {:?}", result);
    }

    #[test]
    fn test_is_supported() {
        let supported = is_supported();
        #[cfg(any(target_os = "macos", target_os = "linux"))]
        assert!(supported);

        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        assert!(!supported);
    }

    #[test]
    fn test_method_name() {
        let name = method_name();
        assert!(!name.is_empty());

        #[cfg(target_os = "macos")]
        assert_eq!(name, "F_NOCACHE");

        #[cfg(target_os = "linux")]
        assert_eq!(name, "fadvise(DONTNEED)");
    }
}
