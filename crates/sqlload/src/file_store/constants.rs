/// Folders and files whose name starts with one of these are never listed as data (`_sqlload/`, `_SUCCESS`, `.tmp`).
pub const HIDDEN_PREFIXES: &[char] = &['_', '.'];
