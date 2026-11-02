use std::collections::BTreeMap;

use crate::config::models::{NamingConfig, UnmatchedPolicy};
use crate::naming::_helpers::identifiers::to_identifier;
use crate::naming::_helpers::patterns::{TopicPattern, fill_template};
use crate::naming::models::{NameRule, NameSource, TopicTable};

/// Compile every naming rule, reporting each invalid pattern.
pub(crate) fn compile_rules(naming: &NamingConfig) -> Result<Vec<NameRule>, Vec<String>> {
    let mut problems: Vec<String> = Vec::new();
    let mut rules: Vec<NameRule> = Vec::new();
    for rule in &naming.rules {
        match TopicPattern::compile(&rule.pattern) {
            Ok(pattern) => rules.push(NameRule {
                pattern,
                template: rule.table.clone(),
            }),
            Err(problem) => problems.push(problem),
        }
    }
    match problems.is_empty() {
        true => Ok(rules),
        false => Err(problems),
    }
}

/// The table for one topic: the map first, then rules in order, then the unmatched policy.
pub(crate) fn table_for(
    naming: &NamingConfig,
    rules: &[NameRule],
    topic: &str,
) -> Result<Option<TopicTable>, String> {
    if let Some(table) = naming.map.get(topic) {
        return Ok(Some(entry(topic, table.clone(), NameSource::Map)));
    }
    for rule in rules {
        if let Some(values) = rule.pattern.matches(topic) {
            let filled = fill_template(&rule.template, &values)?;
            return Ok(Some(entry(
                topic,
                to_identifier(&filled),
                NameSource::Rule(rule.template.clone()),
            )));
        }
    }
    if naming.unmatched == UnmatchedPolicy::Fail {
        return Ok(None);
    }
    let stripped = naming
        .strip_prefix
        .as_deref()
        .and_then(|prefix| topic.strip_prefix(prefix))
        .unwrap_or(topic);
    Ok(Some(entry(
        topic,
        to_identifier(stripped),
        NameSource::Default,
    )))
}

fn entry(topic: &str, table: String, source: NameSource) -> TopicTable {
    TopicTable {
        topic: topic.to_owned(),
        table,
        source,
    }
}

/// Tables that several topics reach through the default rule, which is never intentional grouping.
pub(crate) fn default_collisions(resolved: &[TopicTable]) -> Vec<String> {
    let mut by_table: BTreeMap<&str, Vec<&TopicTable>> = BTreeMap::new();
    for entry in resolved {
        by_table
            .entry(entry.table.as_str())
            .or_default()
            .push(entry);
    }
    by_table
        .into_iter()
        .filter(|(_, entries)| is_default_collision(entries))
        .map(|(table, entries)| collision_message(table, &entries))
        .collect()
}

fn is_default_collision(entries: &[&TopicTable]) -> bool {
    entries.len() > 1
        && entries
            .iter()
            .any(|entry| entry.source == NameSource::Default)
}

fn collision_message(table: &str, entries: &[&TopicTable]) -> String {
    let topics: Vec<&str> = entries.iter().map(|entry| entry.topic.as_str()).collect();
    format!(
        "topics {topics:?} all map to {table} through the default rule; add a naming rule or map entry to group them on purpose"
    )
}
