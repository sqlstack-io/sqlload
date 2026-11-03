use std::collections::BTreeSet;

use crate::config::constants::{MAX_NAME_LENGTH, S3_PATH_PLACEHOLDERS, STREAMING_REQUEST_LIMIT};
use crate::config::errors::ConfigError;
use crate::config::models::{Config, S3Config, StagingKind};

/// Reject settings that parse but cannot work together.
pub(crate) fn validate(config: &Config) -> Result<(), ConfigError> {
    let mut problems: Vec<String> = Vec::new();
    problems.extend(source_problems(config));
    problems.extend(destination_problems(config));
    problems.extend(column_problems(config));
    problems.extend(template_problems(config));
    problems.extend(staging_problems(config));
    problems.extend(batching_problems(config));
    problems.extend(unwrap_problems(config));
    if config.envelope.time_zone.parse::<chrono_tz::Tz>().is_err() {
        problems.push(format!(
            "envelope.time_zone {:?} is not an IANA time zone",
            config.envelope.time_zone
        ));
    }
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
    if config.source.shards == 0 || config.source.shard >= config.source.shards {
        problems.push(format!(
            "source.shard {} must be below source.shards {}",
            config.source.shard, config.source.shards
        ));
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

fn unwrap_problems(config: &Config) -> Vec<String> {
    let columns: BTreeSet<&str> = config
        .envelope
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    let mut problems: Vec<String> = Vec::new();
    for entry in &config.topics {
        for column in entry.unwrap.keys() {
            if !columns.contains(column.as_str()) {
                problems.push(format!(
                    "topics {:?} unwraps {column:?}, which is not an envelope column",
                    entry.pattern
                ));
            }
        }
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
        _ => config
            .staging
            .s3
            .as_ref()
            .map(s3_problems)
            .unwrap_or_default(),
    }
}

fn s3_problems(s3: &S3Config) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    let path = &s3.path;
    let mut rest = path.as_str();
    while let Some(start) = rest.find('{') {
        let Some(length) = rest[start..].find('}') else {
            problems.push(format!("staging.s3.path {path:?} has an unclosed {{"));
            break;
        };
        let name = &rest[start + 1..start + length];
        if !S3_PATH_PLACEHOLDERS.contains(&name) {
            problems.push(format!(
                "staging.s3.path has an unknown placeholder {{{name}}}"
            ));
        }
        rest = &rest[start + length + 1..];
    }
    if !path.contains("{date}") {
        problems.push("staging.s3.path must contain {date}".to_owned());
    }
    if !path.contains("{topic}") && !path.contains("{table}") {
        problems.push("staging.s3.path must contain {topic} or {table}".to_owned());
    }
    if s3.auto_ingest && !path.trim_start_matches('/').starts_with("{table}/") {
        problems.push(
            "staging.s3.auto_ingest needs a path starting with {table}/, so each pipe watches one folder"
                .to_owned(),
        );
    }
    let stage_parts: Vec<&str> = s3.stage.split('.').collect();
    match stage_parts.as_slice() {
        [_] | [_, _, _] => {}
        _ => problems.push("staging.s3.stage must be NAME or DATABASE.SCHEMA.NAME".to_owned()),
    }
    problems
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
