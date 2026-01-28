//! Composite key format for LSM-tree.
//!
//! Format: `logical_key + \x00 + expire_at (8 bytes, big-endian)`
//!
//! This format allows:
//! - expire_at to be visible in the index without reading data blocks
//! - Efficient compaction by checking expiration from keys alone
//! - Natural sorting: same logical key entries are grouped together

/// Separator between logical key and expire_at
const SEPARATOR: u8 = 0x00;

/// Size of expire_at field in bytes
const EXPIRE_AT_SIZE: usize = 8;

/// Encode a logical key and expire_at into a composite key.
///
/// Format: `logical_key + \x00 + expire_at (8 bytes big-endian)`
pub fn encode(logical_key: &str, expire_at: u64) -> String {
    let mut bytes = Vec::with_capacity(logical_key.len() + 1 + EXPIRE_AT_SIZE);
    bytes.extend_from_slice(logical_key.as_bytes());
    bytes.push(SEPARATOR);
    bytes.extend_from_slice(&expire_at.to_be_bytes());
    // Safety: We control the format - logical_key is valid UTF-8, separator and expire_at
    // are treated as raw bytes. We use String for compatibility but it may contain
    // non-UTF-8 bytes after the separator.
    unsafe { String::from_utf8_unchecked(bytes) }
}

/// Decode a composite key into (logical_key, expire_at).
///
/// Returns None if the key is malformed.
pub fn decode(composite_key: &str) -> Option<(&str, u64)> {
    let bytes = composite_key.as_bytes();
    decode_bytes(bytes)
}

/// Decode composite key bytes directly into (logical_key, expire_at).
///
/// This is used when reading from SSTable to avoid UTF-8 validation issues
/// with the binary expire_at portion.
///
/// Returns None if the key is malformed.
fn decode_bytes(bytes: &[u8]) -> Option<(&str, u64)> {
    if bytes.len() < 1 + EXPIRE_AT_SIZE {
        return None;
    }

    let separator_pos = bytes.len() - 1 - EXPIRE_AT_SIZE;
    if bytes[separator_pos] != SEPARATOR {
        return None;
    }

    let logical_key = std::str::from_utf8(&bytes[..separator_pos]).ok()?;
    let expire_at_bytes: [u8; 8] = bytes[separator_pos + 1..].try_into().ok()?;
    let expire_at = u64::from_be_bytes(expire_at_bytes);

    Some((logical_key, expire_at))
}

/// Decode composite key bytes into (logical_key as owned String, expire_at).
///
/// This is the primary function for reading composite keys from SSTable.
/// It handles the binary format safely without going through String intermediates.
///
/// Returns None if the key is malformed.
pub fn decode_from_bytes(bytes: &[u8]) -> Option<(String, u64)> {
    decode_bytes(bytes).map(|(k, e)| (k.to_string(), e))
}

/// Create a composite key String from raw bytes.
///
/// This encapsulates the unsafe conversion needed because composite keys
/// contain non-UTF-8 bytes in the expire_at portion.
///
/// Safety: The caller must ensure bytes represent a valid composite key
/// (produced by `encode` or read from SSTable).
pub fn string_from_bytes(bytes: Vec<u8>) -> String {
    // Safety: Composite key format is controlled by this module.
    // The logical_key portion is valid UTF-8, and the separator + expire_at
    // portion is treated as raw bytes.
    unsafe { String::from_utf8_unchecked(bytes) }
}

/// Extract just the logical key from a composite key.
pub fn logical_key(composite_key: &str) -> Option<&str> {
    decode(composite_key).map(|(k, _)| k)
}

/// Extract just the expire_at from a composite key.
pub fn expire_at(composite_key: &str) -> Option<u64> {
    decode(composite_key).map(|(_, e)| e)
}

/// Check if a composite key matches a logical key prefix.
pub fn matches_logical_key(composite_key: &str, logical_key: &str) -> bool {
    let bytes = composite_key.as_bytes();
    let key_bytes = logical_key.as_bytes();

    if bytes.len() < key_bytes.len() + 1 + EXPIRE_AT_SIZE {
        return false;
    }

    let separator_pos = bytes.len() - 1 - EXPIRE_AT_SIZE;
    if separator_pos != key_bytes.len() {
        return false;
    }

    &bytes[..separator_pos] == key_bytes && bytes[separator_pos] == SEPARATOR
}

/// Check if a composite key is expired.
pub fn is_expired(composite_key: &str, now: u64) -> bool {
    match expire_at(composite_key) {
        Some(0) => false, // No expiration
        Some(exp) => exp <= now,
        None => false, // Malformed key, treat as not expired
    }
}

/// Create a prefix for searching composite keys with a logical key.
/// This is the minimum composite key for the given logical key.
pub fn search_prefix(logical_key: &str) -> String {
    encode(logical_key, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode() {
        let key = encode("user:123", 1234567890);
        let (logical, expire) = decode(&key).unwrap();
        assert_eq!(logical, "user:123");
        assert_eq!(expire, 1234567890);
    }

    #[test]
    fn test_encode_decode_no_ttl() {
        let key = encode("user:123", 0);
        let (logical, expire) = decode(&key).unwrap();
        assert_eq!(logical, "user:123");
        assert_eq!(expire, 0);
    }

    #[test]
    fn test_logical_key() {
        let key = encode("test", 100);
        assert_eq!(logical_key(&key), Some("test"));
    }

    #[test]
    fn test_expire_at() {
        let key = encode("test", 999);
        assert_eq!(expire_at(&key), Some(999));
    }

    #[test]
    fn test_matches_logical_key() {
        let composite = encode("user:123", 100);
        assert!(matches_logical_key(&composite, "user:123"));
        assert!(!matches_logical_key(&composite, "user:12"));
        assert!(!matches_logical_key(&composite, "user:1234"));
    }

    #[test]
    fn test_is_expired() {
        let key_with_ttl = encode("test", 100);
        assert!(is_expired(&key_with_ttl, 101));
        assert!(is_expired(&key_with_ttl, 100));
        assert!(!is_expired(&key_with_ttl, 99));

        let key_no_ttl = encode("test", 0);
        assert!(!is_expired(&key_no_ttl, 1000000));
    }

    #[test]
    fn test_sorting() {
        // Same logical key, different expire_at
        let k1 = encode("a", 100);
        let k2 = encode("a", 200);
        let k3 = encode("b", 50);

        let mut keys = vec![k3.clone(), k1.clone(), k2.clone()];
        keys.sort();

        // Should sort by logical key first, then by expire_at
        assert_eq!(keys[0], k1); // "a" + 100
        assert_eq!(keys[1], k2); // "a" + 200
        assert_eq!(keys[2], k3); // "b" + 50
    }

    #[test]
    fn test_search_prefix() {
        let prefix = search_prefix("user:123");
        let k1 = encode("user:123", 100);
        let k2 = encode("user:123", 200);
        let k3 = encode("user:124", 50);

        // prefix should be <= all keys with same logical key
        assert!(prefix <= k1);
        assert!(prefix <= k2);
        // And < keys with greater logical key
        assert!(prefix < k3);
    }
}
