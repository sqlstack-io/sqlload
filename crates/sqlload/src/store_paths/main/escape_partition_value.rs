//! Escape a value for a Hive-style partition folder name.

use crate::store_paths::constants::HIVE_ESCAPED;

/// `value` with `%XX` escapes for the characters Hive and Spark escape (and `/`), and for a leading `_` or `.`, which would hide the folder.
pub fn escape_partition_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for (index, character) in value.chars().enumerate() {
        let hidden = index == 0 && (character == '_' || character == '.');
        match hidden || character.is_ascii_control() || HIVE_ESCAPED.contains(&character) {
            true => escaped.push_str(&format!("%{:02X}", u32::from(character))),
            false => escaped.push(character),
        }
    }
    escaped
}
