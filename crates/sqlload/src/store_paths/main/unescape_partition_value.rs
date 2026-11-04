//! Read a value back from a Hive-style partition folder name.

use crate::store_paths::_helpers::escapes::percent_decode;

/// `escaped` with its `%XX` escapes undone, as Hive and Spark read partition folders.
pub fn unescape_partition_value(escaped: &str) -> String {
    percent_decode(escaped)
}
