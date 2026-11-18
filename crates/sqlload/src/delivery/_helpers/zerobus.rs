use databricks_zerobus_ingest_sdk::{
    GrpcCode, JsonString, ZerobusError, ZerobusSdk, ZerobusStream,
};
use serde_json::{Map, Value};

use crate::batching::models::{Batch, BatchRow};
use crate::config::models::{ColumnConfig, OversizedPolicy};
use crate::delivery::errors::DeliveryError;
use crate::delivery::models::{DeliveryReport, ZerobusDelivery, ZerobusLink};
use crate::time_text::main::parse_date::parse_date;
use crate::time_text::main::parse_instant::parse_instant;

/// How Zerobus Ingest's JSON records carry one column's value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ZerobusKind {
    /// Microseconds since the epoch.
    Timestamp,
    /// Days since the epoch.
    Date,
    /// The JSON value as text.
    Variant,
    AsIs,
}

impl ZerobusDelivery {
    /// Send the batch's rows in requests under `batching.max_request_bytes`, then wait until Zerobus Ingest has made every one durable; a failure drops the stream so the retry opens a new one.
    pub(crate) async fn deliver(&mut self, batch: &Batch) -> Result<DeliveryReport, DeliveryError> {
        let kinds: Vec<ZerobusKind> = self.columns.iter().map(zerobus_kind).collect();
        let mut records: Vec<(u64, String)> = Vec::with_capacity(batch.rows.len());
        let mut skipped_rows = 0;
        for row in &batch.rows {
            let record = zerobus_record(&self.columns, &kinds, &row.line).map_err(|error| {
                DeliveryError::Fatal(format!(
                    "{}/{} offset {}: {error}",
                    row.topic, row.partition, row.offset
                ))
            })?;
            match record.len() as u64 > self.max_request_bytes {
                false => records.push((record.len() as u64, record)),
                true if self.oversized == OversizedPolicy::Skip => {
                    tracing::warn!(topic = %row.topic, partition = row.partition, offset = row.offset, bytes = record.len(), "skipping a row too large for one Zerobus Ingest request");
                    skipped_rows += 1;
                }
                true => {
                    return Err(DeliveryError::Fatal(oversized_message(
                        row,
                        record.len(),
                        self.max_request_bytes,
                    )));
                }
            }
        }
        let sent_bytes: u64 = records.iter().map(|(bytes, _)| bytes).sum();
        let requests = chunk_records(records, self.max_request_bytes);
        let count = requests.len() as u64;
        let sent = self.send(requests).await;
        if let Err(error) = sent {
            self.link = ZerobusLink::default();
            return Err(error);
        }
        Ok(DeliveryReport {
            rows: batch.rows.len() as u64 - skipped_rows,
            uncompressed_bytes: sent_bytes,
            sent_bytes,
            requests: count,
            skipped_rows,
            confirmed: batch.last_offsets(),
        })
    }

    async fn send(&mut self, requests: Vec<Vec<String>>) -> Result<(), DeliveryError> {
        let stream = self.stream().await?;
        for request in requests {
            stream
                .ingest_records_offset(request.into_iter().map(JsonString))
                .await
                .map_err(|error| zerobus_error("ingest", &error))?;
        }
        stream
            .flush()
            .await
            .map_err(|error| zerobus_error("flush", &error))
    }

    /// The open stream, opening the SDK and the stream when there is none.
    async fn stream(&mut self) -> Result<&ZerobusStream, DeliveryError> {
        if self.link.sdk.is_none() {
            let sdk = ZerobusSdk::builder()
                .endpoint(self.settings.endpoint.clone())
                .unity_catalog_url(self.settings.workspace_url.clone())
                .build()
                .map_err(|error| zerobus_error("connect", &error))?;
            self.link.sdk = Some(sdk);
        }
        if self.link.stream.is_none()
            && let Some(sdk) = &self.link.sdk
        {
            let stream = sdk
                .stream_builder()
                .table(self.table.clone())
                .oauth(
                    self.settings.client_id.clone(),
                    self.settings.client_secret.expose().to_owned(),
                )
                .json()
                .build()
                .await
                .map_err(|error| open_error(&self.table, &self.settings.client_id, &error))?;
            self.link.stream = Some(stream);
        }
        self.link
            .stream
            .as_ref()
            .ok_or_else(|| DeliveryError::Retry("no Zerobus Ingest stream".to_owned()))
    }
}

pub(crate) fn zerobus_kind(column: &ColumnConfig) -> ZerobusKind {
    let upper = column.sql_type.trim().to_ascii_uppercase();
    match upper.split(['(', '<', ' ']).next().unwrap_or_default() {
        "TIMESTAMP" | "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" => ZerobusKind::Timestamp,
        "DATE" => ZerobusKind::Date,
        "VARIANT" => ZerobusKind::Variant,
        _ => ZerobusKind::AsIs,
    }
}

/// One encoded row as Zerobus Ingest's JSON: timestamps in microseconds, dates in days, VARIANT values as JSON text; the error names a value no timestamp or date column can take.
pub(crate) fn zerobus_record(
    columns: &[ColumnConfig],
    kinds: &[ZerobusKind],
    line: &str,
) -> Result<String, String> {
    let mut row: Map<String, Value> =
        serde_json::from_str(line).map_err(|error| format!("row is not a JSON object: {error}"))?;
    for (column, kind) in columns.iter().zip(kinds) {
        let Some(value) = row.get_mut(&column.name) else {
            continue;
        };
        if value.is_null() {
            continue;
        }
        *value = match (kind, value.as_str()) {
            (ZerobusKind::Timestamp, Some(text)) => match parse_instant(text) {
                Some(instant) => Value::from(instant.timestamp_micros()),
                None => return Err(unreadable(&column.name, text, "timestamp")),
            },
            (ZerobusKind::Date, Some(text)) => match parse_date(text) {
                Some(date) => Value::from((date - chrono::NaiveDate::default()).num_days()),
                None => return Err(unreadable(&column.name, text, "date")),
            },
            (ZerobusKind::Variant, _) => Value::from(value.to_string()),
            _ => continue,
        };
    }
    Ok(Value::Object(row).to_string())
}

fn unreadable(column: &str, text: &str, kind: &str) -> String {
    format!(
        "column {column} holds {text:?}, which Zerobus Ingest cannot take as a {kind}; fix the producer, or type the column STRING to keep such values as text"
    )
}

/// Records grouped into requests of at most `limit` bytes each, in order.
pub(crate) fn chunk_records(records: Vec<(u64, String)>, limit: u64) -> Vec<Vec<String>> {
    let mut requests: Vec<Vec<String>> = Vec::new();
    let mut size = 0;
    for (bytes, record) in records {
        match requests.last_mut() {
            Some(request) if size + bytes <= limit => request.push(record),
            _ => {
                requests.push(vec![record]);
                size = 0;
            }
        }
        size += bytes;
    }
    requests
}

fn oversized_message(row: &BatchRow, bytes: usize, limit: u64) -> String {
    format!(
        "row {}/{}@{} is {bytes} bytes, over the {limit}-byte request limit; set oversized = \"skip\" to drop such rows",
        row.topic, row.partition, row.offset
    )
}

/// Opening a stream retries what the SDK calls transient, and also an authorisation refusal: Zerobus Ingest refuses some first token requests for a table it grants seconds later. The exporter's batch retry gives up after its usual limit, naming the grants a lasting refusal needs; every other open failure stops the exporter.
pub(crate) fn open_error(table: &str, client_id: &str, error: &ZerobusError) -> DeliveryError {
    let refused = match error {
        ZerobusError::InvalidUCTokenError(message) => {
            (message.starts_with("Client error (401)") || message.starts_with("Client error (403)"))
                && !message.contains("invalid_client")
        }
        ZerobusError::CreateStreamError(status) => {
            matches!(
                status.code(),
                GrpcCode::Unauthenticated | GrpcCode::PermissionDenied
            )
        }
        _ => false,
    };
    match refused {
        true => DeliveryError::Retry(format!(
            "Zerobus Ingest open stream for {table} was refused: {error}; if this lasts, the service principal {client_id} needs USE CATALOG, USE SCHEMA, and MODIFY and SELECT on the table, granted explicitly"
        )),
        false => zerobus_error("open stream", error),
    }
}

fn zerobus_error(action: &str, error: &ZerobusError) -> DeliveryError {
    match error.is_retryable() {
        true => DeliveryError::Retry(format!("Zerobus Ingest {action} failed: {error}")),
        false => DeliveryError::Fatal(format!("Zerobus Ingest {action} failed: {error}")),
    }
}
