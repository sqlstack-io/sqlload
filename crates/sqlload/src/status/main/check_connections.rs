//! The `check` command.

use crate::bigquery_api::main::run_query::run_query;
use crate::config::models::{Config, StagingKind};
use crate::databricks_api::main::execute_statement::execute_statement;
use crate::delivery::models::Warehouse;
use crate::exporter::models::Clients;
use crate::kafka::main::list_topics::list_topics;
use crate::naming::main::resolve_streams::resolve_streams;
use crate::planning::main::load_path::load_path;
use crate::sql_api::main::execute_sql::execute_sql;
use crate::streaming_api::main::ingest_host::ingest_host;

/// Confirm Kafka and the warehouse are reachable with this configuration; returns one line per finding.
pub async fn check_connections(config: &Config, clients: &Clients) -> Result<Vec<String>, String> {
    let mut findings: Vec<String> = Vec::new();
    let topics = list_topics(config).await?;
    findings.push(format!("Kafka: {} matching topics", topics.len()));
    let streams = resolve_streams(config, &topics).map_err(|problems| problems.join("; "))?;
    findings.push(format!("naming: {} streams", streams.len()));
    let path = load_path(config, clients.warehouse.uses_key_pair());
    match &clients.warehouse {
        Warehouse::Snowflake(snowflake) => {
            let identity = execute_sql(
                &snowflake.sql,
                "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()",
            )
            .await
            .map_err(|error| format!("Snowflake SQL API: {error}"))?;
            let row = identity.rows.first().cloned().unwrap_or_default();
            findings.push(format!(
                "Snowflake: user {}, role {}, warehouse {}",
                cell(&row, 0),
                cell(&row, 1),
                cell(&row, 2)
            ));
            findings.push(format!("delivery: {path:?}"));
            if config.staging.kind == StagingKind::None {
                let host = ingest_host(&snowflake.streaming)
                    .await
                    .map_err(|error| format!("Snowpipe Streaming: {error}"))?;
                findings.push(format!("Snowpipe Streaming: ingest host {host}"));
            }
        }
        Warehouse::Databricks(databricks) => {
            let identity = execute_statement(&databricks.api, "SELECT current_user()")
                .await
                .map_err(|error| format!("Databricks SQL warehouse: {error}"))?;
            let row = identity.rows.first().cloned().unwrap_or_default();
            findings.push(format!("Databricks: user {}", cell(&row, 0)));
            findings.push(format!("delivery: {path:?}"));
        }
        Warehouse::BigQuery(bigquery) => {
            let identity = run_query(bigquery, "SELECT SESSION_USER()")
                .await
                .map_err(|error| format!("BigQuery: {error}"))?;
            let row = identity.rows.first().cloned().unwrap_or_default();
            findings.push(format!("BigQuery: user {}", cell(&row, 0)));
            findings.push(format!("delivery: {path:?}"));
        }
    }
    Ok(findings)
}

fn cell(row: &[Option<String>], index: usize) -> String {
    row.get(index)
        .cloned()
        .flatten()
        .unwrap_or_else(|| "-".to_owned())
}
