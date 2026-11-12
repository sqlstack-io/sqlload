/// Folders and files whose name starts with one of these are never listed as data (`_sqlload/`, `_SUCCESS`, `.tmp`).
pub const HIDDEN_PREFIXES: &[char] = &['_', '.'];

/// Parts of a file a move streams through SQLLoad (across local volumes) that are in memory at once; object_store's parts are about 5 MB.
pub const MOVE_PARTS_IN_FLIGHT: usize = 8;
