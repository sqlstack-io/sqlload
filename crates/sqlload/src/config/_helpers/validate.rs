use std::collections::BTreeSet;

use crate::config::constants::{MAX_NAME_LENGTH, STREAMING_REQUEST_LIMIT};
use crate::config::errors::ConfigError;
use crate::config::models::{Config, StagingKind};

/// Reject settings that parse but cannot work together.
pub(crate) fn validate(config: &Config) -> Result<(), ConfigError> {
    let mut problems: Vec<String> = Vec::new();
    problems.extend(source_problems(config));
    problems.extend(destination_problems(config));
    problems.extend(column_problems(config));
    problems.extend(template_problems(config));
    problems.extend(staging_problems(config));
    problems.extend(batching_problems(config));
    match problems.is_empty() {
        true => Ok(()),
        false => Err(ConfigError::Invalid(problems)),
    }
}

fn destination_problems(config: &Config) -> Vec<String> {
    let destination = &config.destination;
    [
        ("destination.database", &destination.database),
        ("destination.schema", &destination.schema),
    ]
    .into_iter()
    .filter(|(_, name)| !is_identifier(name))
    .map(|(key, name)| {
        format!("{key} {name:?} must be an unquoted identifier (letters, digits, _ and $)")
    })
    .collect()
}

fn source_problems(config: &Config) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    if config.source.include.is_empty() {
        problems.push("source.include must list at least one topic pattern".to_owned());
    }
    for pattern in config.source.include.iter().chain(&config.source.exclude) {
        if let Err(error) = globset::Glob::new(pattern) {
            problems.push(format!("invalid topic pattern {pattern:?}: {error}"));
        }
    }
    if !config.source.consumer_group.contains("{stream}") {
        problems.push("source.consumer_group must contain {stream}".to_owned());
    }
    problems
}

fn batching_problems(config: &Config) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    if config.batching.max_request_bytes.0 > STREAMING_REQUEST_LIMIT {
        problems.push("batching.max_request_bytes cannot exceed 4MiB".to_owned());
    }
    if config.batching.max_rows == 0 || config.batching.max_bytes.0 == 0 {
        problems.push("batching.max_rows and batching.max_bytes must be positive".to_owned());
    }
    problems
}

fn column_problems(config: &Config) -> Vec<String> {
    let columns = &config.envelope.columns;
    let mut problems: Vec<String> = Vec::new();
    if columns.is_empty() {
        problems.push("envelope.columns must list at least one column".to_owned());
    }
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for column in columns {
        if !is_identifier(&column.name) {
            problems.push(format!(
                "column name {:?} is not a plain identifier",
                column.name
            ));
        }
        if !seen.insert(column.name.to_ascii_uppercase()) {
            problems.push(format!("column {:?} appears twice", column.name));
        }
        if column.sql_type.trim().is_empty() {
            problems.push(format!("column {:?} has no type", column.name));
        }
    }
    problems
}

fn template_problems(config: &Config) -> Vec<String> {
    let naming = &config.naming;
    let templates = [
        ("naming.stage", &naming.stage),
        ("naming.pipe", &naming.pipe),
        ("naming.streaming_pipe", &naming.streaming_pipe),
    ];
    templates
        .into_iter()
        .filter(|(_, template)| !template.contains("{table}") && !template.contains("{table_flat}"))
        .map(|(key, _)| format!("{key} must contain {{table}} or {{table_flat}}"))
        .collect()
}

fn staging_problems(config: &Config) -> Vec<String> {
    let is_s3 = config.staging.kind == StagingKind::S3;
    match (is_s3, config.staging.s3.is_some()) {
        (true, false) => vec!["staging.kind = \"s3\" needs a [staging.s3] section".to_owned()],
        (false, true) => vec!["[staging.s3] is only used when staging.kind = \"s3\"".to_owned()],
        _ => Vec::new(),
    }
}

/// True for names Snowflake accepts without quotes.
fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    let first_ok = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
    first_ok
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
        && name.len() <= MAX_NAME_LENGTH
}
