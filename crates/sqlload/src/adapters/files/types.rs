//! Shapes of the files destination's records.

use std::collections::BTreeMap;

/// Source files (name and version) by the commit that loaded them and the folder (below the table folder, ending in `/`, or empty) its files holding their rows are in.
pub type CommitFiles = BTreeMap<String, BTreeMap<String, Vec<(String, String)>>>;
