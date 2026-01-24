//! Thread-local buffer pool for reducing allocation overhead.
//!
//! This module provides a simple buffer pooling mechanism that reuses
//! Vec<u8> buffers to reduce memory allocation overhead during
//! high-frequency operations like WAL writes and SSTable reads.

use std::cell::RefCell;

/// Maximum number of buffers to keep in the pool per thread
const MAX_POOL_SIZE: usize = 8;

/// Maximum buffer size to pool (larger buffers are not pooled)
const MAX_BUFFER_SIZE: usize = 1024 * 1024; // 1MB

thread_local! {
    static BUFFER_POOL: RefCell<Vec<Vec<u8>>> = const { RefCell::new(Vec::new()) };
}

/// A pooled buffer that automatically returns to the pool when dropped.
pub struct PooledBuffer {
    buf: Option<Vec<u8>>,
}

impl PooledBuffer {
    /// Create a new pooled buffer with at least the specified capacity.
    pub fn new(capacity: usize) -> Self {
        let buf = acquire_buffer(capacity);
        Self { buf: Some(buf) }
    }

    /// Create a new pooled buffer with the specified size, filled with zeros.
    /// Use this when you need a buffer of a specific length for reading data.
    pub fn new_zeroed(size: usize) -> Self {
        let mut buf = acquire_buffer(size);
        buf.resize(size, 0);
        Self { buf: Some(buf) }
    }

    /// Take ownership of the buffer without returning it to the pool.
    /// Use this when you need to pass the buffer to another owner.
    #[cfg(test)]
    pub fn take(mut self) -> Vec<u8> {
        self.buf.take().unwrap()
    }
}

impl std::ops::Deref for PooledBuffer {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        self.buf.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PooledBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buf.as_mut().unwrap()
    }
}

impl AsRef<[u8]> for PooledBuffer {
    fn as_ref(&self) -> &[u8] {
        self.buf.as_ref().unwrap()
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let Some(buf) = self.buf.take() {
            release_buffer(buf);
        }
    }
}

/// Acquire a buffer from the pool or create a new one.
fn acquire_buffer(capacity: usize) -> Vec<u8> {
    BUFFER_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        // Try to find a buffer with sufficient capacity
        if let Some(idx) = pool.iter().position(|b| b.capacity() >= capacity) {
            let mut buf = pool.swap_remove(idx);
            buf.clear();
            buf
        } else if let Some(mut buf) = pool.pop() {
            // Reuse any buffer and reserve more capacity
            buf.clear();
            buf.reserve(capacity.saturating_sub(buf.capacity()));
            buf
        } else {
            // Create new buffer
            Vec::with_capacity(capacity)
        }
    })
}

/// Release a buffer back to the pool.
fn release_buffer(buf: Vec<u8>) {
    // Don't pool very large buffers
    if buf.capacity() > MAX_BUFFER_SIZE {
        return;
    }

    BUFFER_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        if pool.len() < MAX_POOL_SIZE {
            pool.push(buf);
        }
        // If pool is full, buffer is dropped
    });
}

/// Get the current pool size (for testing/metrics).
#[cfg(test)]
pub fn pool_size() -> usize {
    BUFFER_POOL.with(|pool| pool.borrow().len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pooled_buffer_basic() {
        // Pool should start empty
        assert_eq!(pool_size(), 0);

        {
            let mut buf = PooledBuffer::new(1024);
            buf.extend_from_slice(b"hello");
            assert_eq!(&buf[..], b"hello");
        }

        // Buffer should be returned to pool
        assert_eq!(pool_size(), 1);

        {
            let buf = PooledBuffer::new(512);
            // Should reuse the pooled buffer
            assert!(buf.capacity() >= 1024);
        }

        assert_eq!(pool_size(), 1);
    }

    #[test]
    fn test_pooled_buffer_take() {
        {
            let mut buf = PooledBuffer::new(1024);
            buf.extend_from_slice(b"data");
            let _taken = buf.take();
            // Buffer is taken, not returned to pool
        }
        assert_eq!(pool_size(), 0);
    }

    #[test]
    fn test_pool_size_limit() {
        // Fill the pool
        for _ in 0..MAX_POOL_SIZE + 5 {
            let _ = PooledBuffer::new(64);
        }

        // Pool should not exceed MAX_POOL_SIZE
        assert!(pool_size() <= MAX_POOL_SIZE);
    }

    #[test]
    fn test_large_buffer_not_pooled() {
        {
            let _ = PooledBuffer::new(MAX_BUFFER_SIZE + 1);
        }
        // Large buffer should not be pooled
        assert_eq!(pool_size(), 0);
    }
}
