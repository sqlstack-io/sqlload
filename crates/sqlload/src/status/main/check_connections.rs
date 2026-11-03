//! The `check` command.

use crate::config::models::{Config, StagingKind};
use crate::exporter::models::Clients;
use crate::kafka::main::list_topics::list_topics;
use crate::naming::main::resolve_streams::resolve_streams;
use crate::planning::main::load_path::load_path;
use crate::sql_api::main::execute_sql::execute_sql;
use crate::streaming_api::main::ingest_host::ingest_host;

/// Confirm Kafka and Snowflake are reachable with this configuration; returns one line per finding.
pub async fn check_connections(config: &Config, clients: &Clients) -> Result<Vec<String>, String> {
    let mut findings: Vec<String> = Vec::new();
    let topics = list_topics(config).await?;
    findings.push(format!("Kafka: {} matching topics", topics.len()));
    let streams = resolve_streams(config, &topics).map_err(|problems| problems.join("; "))?;
    findings.push(format!("naming: {} streams", streams.len()));
    let identity = execute_sql(
        &clients.sql,
        "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()",
    )
    .await
    .map_err(|error| format!("Snowflake SQL API: {error}"))?;
    let row = identity.rows.first().cloned().unwrap_or_default();
    let cell = |index: usize| {
        row.get(index)
            .cloned()
            .flatten()
            .unwrap_or_else(|| "-".to_owned())
    };
    findings.push(format!(
        "Snowflake: user {}, role {}, warehouse {}",
        cell(0),
        cell(1),
        cell(2)
    ));
    let path = load_path(config, clients.auth.uses_key_pair());
    findings.push(format!("delivery: {path:?}"));
    if config.staging.kind == StagingKind::None {
        let host = ingest_host(&clients.streaming)
            .await
            .map_err(|error| format!("Snowpipe Streaming: {error}"))?;
        findings.push(format!("Snowpipe Streaming: ingest host {host}"));
    }
    Ok(findings)
}
