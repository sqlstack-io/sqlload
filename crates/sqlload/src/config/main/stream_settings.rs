//! Per-stream settings after topic overrides.

use crate::config::_helpers::patterns::pattern_matches;
use crate::config::models::{Config, StreamSettings};

/// The settings for `topic`: global batching values, then every matching `[[topics]]` entry in order.
pub fn stream_settings(config: &Config, topic: &str) -> StreamSettings {
    let mut settings = StreamSettings {
        start_from: config.source.start_from,
        max_bytes: config.batching.max_bytes.0,
        max_rows: config.batching.max_rows,
        max_age: config.batching.max_age,
        oversized: config.batching.oversized,
    };
    let matching = config
        .topics
        .iter()
        .filter(|entry| pattern_matches(&entry.pattern, topic));
    for entry in matching {
        settings.start_from = entry.start_from.unwrap_or(settings.start_from);
        settings.max_bytes = entry.max_bytes.map_or(settings.max_bytes, |size| size.0);
        settings.max_rows = entry.max_rows.unwrap_or(settings.max_rows);
        settings.max_age = entry.max_age.unwrap_or(settings.max_age);
        settings.oversized = entry.oversized.unwrap_or(settings.oversized);
    }
    settings
}
