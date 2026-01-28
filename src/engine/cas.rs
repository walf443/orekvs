//! Striped lock for Compare-and-Set (CAS) operations.
//!
//! Provides concurrent CAS operations on different keys while serializing
//! operations on the same key (or keys that hash to the same stripe).

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Mutex;

/// Number of stripes for CAS lock (power of 2 for efficient modulo)
const CAS_LOCK_STRIPES: usize = 256;

/// Striped lock for CAS operations.
///
/// Allows concurrent CAS operations on different keys while serializing
/// operations on the same key (or keys that hash to the same stripe).
pub struct CasLockStripe {
    locks: Vec<Mutex<()>>,
}

impl CasLockStripe {
    /// Create a new striped lock with the default number of stripes.
    pub fn new() -> Self {
        Self::with_stripes(CAS_LOCK_STRIPES)
    }

    /// Create a new striped lock with a custom number of stripes.
    pub fn with_stripes(num_stripes: usize) -> Self {
        let locks = (0..num_stripes).map(|_| Mutex::new(())).collect();
        Self { locks }
    }

    /// Get the lock for a given key.
    ///
    /// Keys are hashed and mapped to stripes, so different keys may share
    /// the same lock if they hash to the same stripe.
    pub fn get_lock(&self, key: &str) -> &Mutex<()> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        &self.locks[hash % self.locks.len()]
    }
}

impl Default for CasLockStripe {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cas_lock_stripe_same_key() {
        let stripe = CasLockStripe::new();
        let lock1 = stripe.get_lock("key1");
        let lock2 = stripe.get_lock("key1");
        // Same key should return the same lock
        assert!(std::ptr::eq(lock1, lock2));
    }

    #[test]
    fn test_cas_lock_stripe_different_keys() {
        let stripe = CasLockStripe::new();
        // Different keys may or may not return the same lock (depends on hash)
        let _lock1 = stripe.get_lock("key1");
        let _lock2 = stripe.get_lock("key2");
        // Just verify we can get locks for different keys without panic
    }

    #[test]
    fn test_cas_lock_stripe_custom_stripes() {
        let stripe = CasLockStripe::with_stripes(4);
        let _lock = stripe.get_lock("test");
        // Verify custom stripe count works
    }
}
