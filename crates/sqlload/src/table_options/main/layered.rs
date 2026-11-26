//! A setting of a table over its source's.

use crate::loader::models::SettingOrigin;

/// A table's value if set, otherwise its source's.
pub fn layered<T: Clone>(table: &Option<T>, source: &Option<T>) -> Option<(T, SettingOrigin)> {
    match (table, source) {
        (Some(value), _) => Some((value.clone(), SettingOrigin::Table)),
        (None, Some(value)) => Some((value.clone(), SettingOrigin::Source)),
        (None, None) => None,
    }
}
