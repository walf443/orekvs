use std::collections::BTreeMap;

// Type alias for MemTable entries
pub type MemTable = BTreeMap<String, Option<String>>;
