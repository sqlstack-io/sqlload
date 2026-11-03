//! Group topics into streams.

use std::collections::BTreeMap;

use crate::config::models::Config;
use crate::naming::_helpers::streams::build_stream;
use crate::naming::main::resolve_tables::resolve_tables;
use crate::naming::models::Stream;

/// Group topics by table and derive every object name each stream uses.
pub fn resolve_streams(config: &Config, topics: &[String]) -> Result<Vec<Stream>, Vec<String>> {
    let tables = resolve_tables(&config.naming, topics)?;
    let mut grouped: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for entry in tables {
        grouped.entry(entry.table).or_default().push(entry.topic);
    }
    let mut problems: Vec<String> = Vec::new();
    let mut streams: Vec<Stream> = Vec::new();
    for (table, mut stream_topics) in grouped {
        stream_topics.sort();
        match build_stream(config, table, stream_topics) {
            Ok(stream) => streams.push(stream),
            Err(problem) => problems.push(problem),
        }
    }
    match problems.is_empty() {
        true => Ok(streams),
        false => Err(problems),
    }
}
