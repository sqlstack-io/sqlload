//! Shapes of the files destination's records.

use std::collections::BTreeMap;

/// Swaps two existing folders in one rename, or fails with `Unsupported` where the system or filesystem can't.
pub type Exchange = fn(&std::path::Path, &std::path::Path) -> std::io::Result<()>;

/// Source files (name and version) by the commit that loaded them and the folder (below the table folder, ending in `/`, or empty) its files holding their rows are in.
pub type CommitFiles = BTreeMap<String, BTreeMap<String, Vec<(String, String)>>>;
