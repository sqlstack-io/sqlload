use std::collections::BTreeMap;

use crate::config::models::Config;
use crate::naming::_helpers::patterns::fill_template;
use crate::naming::models::Stream;

/// Topics whose names differ only in case or punctuation would share a streaming channel and its offsets.
fn channel_collision(table: &str, topics: &[String]) -> Result<(), String> {
    let mut seen: BTreeMap<String, &str> = BTreeMap::new();
    for topic in topics {
        let channel = channel_prefix(topic);
        if let Some(other) = seen.insert(channel.clone(), topic) {
            return Err(format!(
                "topics {other:?} and {topic:?} both export to {table} and would share channel {channel}; map them to different tables"
            ));
        }
    }
    Ok(())
}

/// The topic part of a channel name: upper-case letters and digits, everything else `_`.
pub(crate) fn channel_prefix(topic: &str) -> String {
    topic
        .chars()
        .map(|c| match c.is_ascii_alphanumeric() {
            true => c.to_ascii_uppercase(),
            false => '_',
        })
        .collect()
}

/// The stream for `table`, with names from the configured templates.
pub(crate) fn build_stream(
    config: &Config,
    table: String,
    topics: Vec<String>,
) -> Result<Stream, String> {
    channel_collision(&table, &topics)?;
    let name = table.to_ascii_lowercase();
    let values = BTreeMap::from([
        ("table".to_owned(), table.clone()),
        ("table_flat".to_owned(), table.replace("__", "_")),
        ("stream".to_owned(), name.clone()),
    ]);
    Ok(Stream {
        consumer_group: fill_template(&config.source.consumer_group, &values)?,
        stage: fill_template(&config.naming.stage, &values)?,
        pipe: fill_template(&config.naming.pipe, &values)?,
        streaming_pipe: fill_template(&config.naming.streaming_pipe, &values)?,
        table,
        name,
        topics,
    })
}
