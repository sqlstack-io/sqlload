//! Map topics to tables.

use crate::config::models::NamingConfig;
use crate::naming::_helpers::identifiers::identifier_problem;
use crate::naming::_helpers::tables::{compile_rules, default_collisions, table_for};
use crate::naming::models::TopicTable;

/// The table for every topic, checked for invalid names and accidental collisions.
pub fn resolve_tables(
    naming: &NamingConfig,
    topics: &[String],
) -> Result<Vec<TopicTable>, Vec<String>> {
    let rules = compile_rules(naming)?;
    let mut problems: Vec<String> = Vec::new();
    let mut resolved: Vec<TopicTable> = Vec::new();
    for topic in topics {
        match table_for(naming, &rules, topic) {
            Ok(Some(entry)) => resolved.push(entry),
            Ok(None) => problems.push(format!("topic {topic:?} matches no naming rule")),
            Err(problem) => problems.push(problem),
        }
    }
    problems.extend(
        resolved
            .iter()
            .filter_map(|entry| identifier_problem(&entry.table)),
    );
    problems.extend(default_collisions(&resolved));
    match problems.is_empty() {
        true => Ok(resolved),
        false => Err(problems),
    }
}
