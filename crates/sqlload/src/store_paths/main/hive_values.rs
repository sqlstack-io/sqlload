//! Read Hive-style `key=value` folders from an object key.

use crate::store_paths::_helpers::escapes::percent_decode;

/// Every `key=value` folder in `relative_key` (the key below the source's prefix), in path order.
pub fn hive_values(relative_key: &str) -> Vec<(String, String)> {
    let folders = relative_key
        .rsplit_once('/')
        .map_or("", |(folders, _)| folders);
    folders
        .split('/')
        .filter_map(|folder| folder.split_once('='))
        .map(|(key, value)| (key.to_owned(), percent_decode(value)))
        .collect()
}
