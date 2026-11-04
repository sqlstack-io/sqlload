//! Tell a loader project from an exporter configuration.

use crate::project::constants::PROJECT_MARKER_KEYS;

/// Whether TOML `text` is a loader project (it has `[connections]`, `[targets]` or `[sources]`).
pub fn is_project(text: &str) -> bool {
    toml::from_str::<toml::Table>(text).is_ok_and(|table| {
        PROJECT_MARKER_KEYS
            .iter()
            .any(|key| table.contains_key(*key))
    })
}
