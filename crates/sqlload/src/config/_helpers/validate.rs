use std::collections::BTreeSet;

use crate::config::constants::{
    BIGQUERY_COLUMN_TYPES, MAX_NAME_LENGTH, S3_PATH_PLACEHOLDERS, STREAMING_REQUEST_LIMIT,
};
use crate::config::errors::ConfigError;
use crate::config::models::{
    BigQueryConfig, Config, DatabricksConfig, Destination, EnvelopeField, FileFormat, LoadMethod,
    S3Config, StagingKind,
};

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
    let names: Vec<(&str, &String)> = match &config.destination {
        Destination::Snowflake(snowflake) => vec![
            ("destination.database", &snowflake.database),
            ("destination.schema", &snowflake.schema),
        ],
        Destination::Databricks(databricks) => vec![
            ("destination.catalog", &databricks.catalog),
            ("destination.schema", &databricks.schema),
        ],
        Destination::BigQuery(bigquery) => vec![("destination.dataset", &bigquery.dataset)],
    };
    let mut problems: Vec<String> = names
        .into_iter()
        .filter(|(_, name)| !is_identifier(name))
        .map(|(key, name)| {
            format!("{key} {name:?} must be an unquoted identifier (letters, digits, _ and $)")
        })
        .collect();
    match &config.destination {
        Destination::Snowflake(_) => {}
        Destination::Databricks(databricks) => {
            problems.extend(databricks_problems(config, databricks));
        }
        Destination::BigQuery(bigquery) => problems.extend(bigquery_problems(config, bigquery)),
    }
    problems
}

/// BigQuery streams through the Storage Write API, or loads Parquet files uploaded with the load job or from a GCS bucket.
fn bigquery_problems(config: &Config, bigquery: &BigQueryConfig) -> Vec<String> {
    let staging = &config.staging;
    let mut problems: Vec<String> = config
        .envelope
        .columns
        .iter()
        .filter(|column| {
            let upper = column.sql_type.trim().to_ascii_uppercase();
            let family = upper.split(['(', '<', ' ']).next().unwrap_or_default();
            !BIGQUERY_COLUMN_TYPES.contains(&family)
        })
        .map(|column| {
            format!(
                "envelope column {} is typed {}; the BigQuery exporter writes STRING, INT64 (or another integer name), TIMESTAMP and DATE columns",
                column.name, column.sql_type
            )
        })
        .collect();
    let project_ok = bigquery
        .project
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '-' | '.' | ':'));
    if bigquery.project.is_empty() || !project_ok {
        problems.push(format!(
            "destination.project {:?} must be a Google Cloud project ID",
            bigquery.project
        ));
    }
    if staging.kind == StagingKind::S3 {
        problems.push("staging.kind = \"s3\" is not a BigQuery route; files for a load job go to \"internal\" (sent with the job) or \"gcs\" (a Cloud Storage bucket)".to_owned());
    }
    if staging.kind != StagingKind::None && staging.format == FileFormat::Json {
        problems.push(
            "BigQuery loads Parquet files; remove staging.format or set it to \"parquet\""
                .to_owned(),
        );
    }
    if staging.load == LoadMethod::Snowpipe {
        problems.push(
            "staging.load = \"snowpipe\" is for Snowflake; BigQuery loads with load jobs"
                .to_owned(),
        );
    }
    problems
}

/// Databricks loads Parquet files with COPY INTO, or streams through Zerobus Ingest.
fn databricks_problems(config: &Config, databricks: &DatabricksConfig) -> Vec<String> {
    let staging = &config.staging;
    let mut problems: Vec<String> = config
        .envelope
        .columns
        .iter()
        .filter(|column| {
            let upper = column.sql_type.trim().to_ascii_uppercase();
            column.field == EnvelopeField::Timestamp
                && (upper.starts_with("TIMESTAMP") || upper.starts_with("DATE"))
        })
        .map(|column| format!("envelope column {} holds the Kafka timestamp, milliseconds since the epoch, which Databricks would read as another unit; type it BIGINT", column.name))
        .collect();
    if staging.kind == StagingKind::None && databricks.zerobus_endpoint.is_none() {
        problems.push("staging.kind = \"none\" on Databricks streams through Zerobus Ingest and needs destination.zerobus_endpoint (https://<workspace-id>.zerobus.<region>.cloud.databricks.com)".to_owned());
    }
    if staging.kind != StagingKind::None && staging.format == FileFormat::Json {
        problems.push(
            "Databricks loads Parquet files; remove staging.format or set it to \"parquet\""
                .to_owned(),
        );
    }
    if staging.load == LoadMethod::Snowpipe {
        problems.push(
            "staging.load = \"snowpipe\" is for Snowflake; Databricks loads with COPY INTO"
                .to_owned(),
        );
    }
    if let Some(s3) = &staging.s3 {
        if s3.auto_ingest {
            problems.push(
                "staging.s3.auto_ingest is for Snowflake; Databricks loads with COPY INTO"
                    .to_owned(),
            );
        }
        if !s3.stage.is_empty() {
            problems.push("staging.s3.stage is for Snowflake; Databricks reads the bucket through a Unity Catalog external location, so remove it".to_owned());
        }
    }
    problems
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
    let snowflake = matches!(config.destination, Destination::Snowflake(_));
    let mut problems: Vec<String> = Vec::new();
    if snowflake && config.staging.format == FileFormat::Parquet && !is_s3 {
        problems.push("staging.format = \"parquet\" needs staging.kind = \"s3\"".to_owned());
    }
    if config.staging.max_in_flight == 0 {
        problems.push("staging.max_in_flight must be at least 1".to_owned());
    }
    if config.staging.max_pending_files == 0 {
        problems.push("staging.max_pending_files must be at least 1".to_owned());
    }
    if config.staging.pending_check_after.is_zero()
        || config.staging.pending_recheck_every.is_zero()
    {
        problems.push(
            "staging.pending_check_after and staging.pending_recheck_every must be positive"
                .to_owned(),
        );
    }
    let is_gcs = config.staging.kind == StagingKind::Gcs;
    let bigquery = matches!(config.destination, Destination::BigQuery(_));
    if is_gcs && !bigquery {
        problems.push("staging.kind = \"gcs\" is for BigQuery".to_owned());
    }
    problems.extend(match (is_gcs, config.staging.gcs.as_ref()) {
        (true, None) => vec!["staging.kind = \"gcs\" needs a [staging.gcs] section".to_owned()],
        (false, Some(_)) => {
            vec!["[staging.gcs] is only used when staging.kind = \"gcs\"".to_owned()]
        }
        (_, Some(gcs)) => path_problems("staging.gcs.path", &gcs.path),
        (false, None) => Vec::new(),
    });
    problems.extend(match (is_s3, config.staging.s3.is_some()) {
        (true, false) => vec!["staging.kind = \"s3\" needs a [staging.s3] section".to_owned()],
        (false, true) => vec!["[staging.s3] is only used when staging.kind = \"s3\"".to_owned()],
        _ => config
            .staging
            .s3
            .as_ref()
            .map(|s3| s3_problems(s3, snowflake))
            .unwrap_or_default(),
    });
    problems
}

fn s3_problems(s3: &S3Config, snowflake: bool) -> Vec<String> {
    let mut problems = path_problems("staging.s3.path", &s3.path);
    if s3.auto_ingest && !s3.path.trim_start_matches('/').starts_with("{table}/") {
        problems.push(
            "staging.s3.auto_ingest needs a path starting with {table}/, so each pipe watches one folder"
                .to_owned(),
        );
    }
    let stage_parts: Vec<&str> = s3.stage.split('.').collect();
    match stage_parts.as_slice() {
        _ if !snowflake => {}
        [""] => problems.push("staging.s3.stage must name the external stage".to_owned()),
        [_] | [_, _, _] => {}
        _ => problems.push("staging.s3.stage must be NAME or DATABASE.SCHEMA.NAME".to_owned()),
    }
    problems
}

/// A bucket folder template's problems: unknown placeholders, and a missing `{date}` or `{topic}`/`{table}`.
fn path_problems(key: &str, path: &str) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    let mut rest = path;
    while let Some(start) = rest.find('{') {
        let Some(length) = rest[start..].find('}') else {
            problems.push(format!("{key} {path:?} has an unclosed {{"));
            break;
        };
        let name = &rest[start + 1..start + length];
        if !S3_PATH_PLACEHOLDERS.contains(&name) {
            problems.push(format!("{key} has an unknown placeholder {{{name}}}"));
        }
        rest = &rest[start + length + 1..];
    }
    if !path.contains("{date}") {
        problems.push(format!("{key} must contain {{date}}"));
    }
    if !path.contains("{topic}") && !path.contains("{table}") {
        problems.push(format!("{key} must contain {{topic}} or {{table}}"));
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
