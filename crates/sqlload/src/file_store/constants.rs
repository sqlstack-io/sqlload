/// Folders and files whose name starts with one of these are never listed as data (`_sqlload/`, `_SUCCESS`, `.tmp`).
pub const HIDDEN_PREFIXES: &[char] = &['_', '.'];

/// The OAuth scope a GCS connection signed in with Google's application default credentials asks for: reads, writes and deletes.
pub const GCS_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";
