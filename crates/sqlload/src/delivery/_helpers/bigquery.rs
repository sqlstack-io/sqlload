use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use prost::Message;
use serde_json::{Value, json};

use crate::batching::models::{Batch, BatchRow};
use crate::bigquery_api::main::append_answer::append_answer;
use crate::bigquery_api::main::append_rows::append_rows;
use crate::bigquery_api::main::arrow_request::arrow_request;
use crate::bigquery_api::main::load_job::load_job;
use crate::bigquery_api::main::put_object_gcs::put_object_gcs;
use crate::bigquery_api::main::table_path::table_path;
use crate::bigquery_api::models::AppendRowsRequest;
use crate::delivery::_helpers::stage::{retry, rows_digest};
use crate::delivery::_helpers::stage_paths::{FolderValues, render_folder};
use crate::delivery::constants::{LOAD_JOB_INTERVAL, LOAD_JOB_WAIT};
use crate::delivery::errors::DeliveryError;
use crate::delivery::models::{BigQueryDelivery, DeliveryReport, StagedLoad};
use crate::file_format::main::batch_parquet::batch_parquet;
use crate::file_format::main::bigquery_rows::bigquery_rows;
use crate::http::errors::ApiError;
use crate::planning::models::LoadPath;
use crate::time_text::main::now_ms::now_ms;

/// gRPC codes an append error may clear on its own: deadline exceeded, resource exhausted, aborted, internal, unavailable.
const RETRYABLE_APPEND_CODES: &[i32] = &[4, 8, 10, 13, 14];
/// Load job error reasons a later job may not hit.
const RETRYABLE_JOB_REASONS: &[&str] = &[
    "backendError",
    "internalError",
    "rateLimitExceeded",
    "jobBackendError",
    "jobInternalError",
];

impl BigQueryDelivery {
    /// Write the batch to the table: appended to its default stream, where each acknowledged append is committed, or loaded by one load job that has finished; the offsets are confirmed only then.
    pub(crate) async fn deliver(&mut self, batch: &Batch) -> Result<DeliveryReport, DeliveryError> {
        let lines: Vec<&str> = batch.rows.iter().map(|row| row.line.as_str()).collect();
        let uncompressed_bytes: u64 = lines.iter().map(|line| line.len() as u64 + 1).sum();
        let (sent_bytes, requests) = match self.path {
            LoadPath::WriteApi => {
                let all: Vec<&BatchRow> = batch.rows.iter().collect();
                self.append(&self.rows(&all)?).await?
            }
            _ => self.load(batch).await?,
        };
        Ok(DeliveryReport {
            rows: batch.rows.len() as u64,
            uncompressed_bytes,
            sent_bytes,
            requests,
            skipped_rows: 0,
            confirmed: batch.last_offsets(),
        })
    }

    fn rows(&self, rows: &[&BatchRow]) -> Result<RecordBatch, DeliveryError> {
        let lines: Vec<&str> = rows.iter().map(|row| row.line.as_str()).collect();
        bigquery_rows(&self.config.envelope.columns, &lines).map_err(|error| {
            DeliveryError::Fatal(format!(
                "stream {} has a row BigQuery cannot take: {error}",
                self.stream.name
            ))
        })
    }

    /// `projects/p/datasets/d/tables/t`.
    fn table(&self) -> String {
        table_path(
            self.config.destination.database(),
            self.config.destination.schema(),
            &self.stream.table,
        )
    }

    /// Append `rows` to the table's default stream in requests under `batching.max_request_bytes` and wait for every answer; returns the bytes and requests sent.
    async fn append(&self, rows: &RecordBatch) -> Result<(u64, u64), DeliveryError> {
        let rows = write_api_rows(rows).map_err(DeliveryError::Fatal)?;
        let limit = usize::try_from(self.config.batching.max_request_bytes.0).unwrap_or(usize::MAX);
        let stream = format!("{}/streams/_default", self.table());
        let mut requests = append_chunks(&rows, limit, &stream)
            .map_err(|error| DeliveryError::Fatal(error.to_string()))?
            .into_iter();
        let Some(first) = requests.next() else {
            return Ok((0, 0));
        };
        let mut sent = first.encoded_len() as u64;
        let mut count = 1;
        let opened = append_rows(&self.api, first)
            .await
            .map_err(|error| retry("Storage Write API append", &error))?;
        let answers = tokio::spawn(read_answers(opened.responses));
        for mut request in requests {
            if let Some(arrow) = request.arrow_rows.as_mut() {
                arrow.writer_schema = None;
            }
            sent += request.encoded_len() as u64;
            count += 1;
            if opened.requests.send(request).await.is_err() {
                break;
            }
        }
        drop(opened.requests);
        let answered = answers.await.map_err(|error| {
            DeliveryError::Retry(format!("the append reader failed: {error}"))
        })??;
        match answered == count {
            true => Ok((sent, count as u64)),
            false => Err(DeliveryError::Retry(format!(
                "BigQuery answered {answered} of {count} appends before closing the stream"
            ))),
        }
    }

    /// Load the batch with one load job, after the table's previous job by [`LOAD_JOB_INTERVAL`]; the job is named after this delivery and the batch's offsets, so a retry reads a job that already ran for them instead of running it again.
    async fn load(&mut self, batch: &Batch) -> Result<(u64, u64), DeliveryError> {
        let digest = rows_digest(&batch.rows.iter().collect::<Vec<_>>());
        let mut staged = match self.staged.take() {
            Some(staged) if staged.digest == digest => staged,
            _ => StagedLoad {
                digest,
                attempt: 0,
                uris: Vec::new(),
            },
        };
        let mut sent_bytes = 0;
        let mut upload = None;
        match self.path {
            LoadPath::GcsLoad if staged.uris.is_empty() => {
                let uploaded = self.upload_files(batch, &staged.digest).await;
                match uploaded {
                    Ok((uris, bytes)) => {
                        staged.uris = uris;
                        sent_bytes = bytes;
                    }
                    Err(error) => {
                        self.staged = Some(staged);
                        return Err(error);
                    }
                }
            }
            LoadPath::GcsLoad => {}
            _ => {
                let all: Vec<&BatchRow> = batch.rows.iter().collect();
                let body = batch_parquet(&self.rows(&all)?).map_err(DeliveryError::Fatal)?;
                sent_bytes = body.len() as u64;
                upload = Some(bytes::Bytes::from(body));
            }
        }
        match self.run_job(&staged, upload).await {
            Ok(JobEnd::Loaded) => Ok((sent_bytes, 1)),
            Ok(JobEnd::Failed(error)) => {
                staged.attempt += 1;
                self.staged = Some(staged);
                Err(error)
            }
            Err(error) => {
                self.staged = Some(staged);
                Err(error)
            }
        }
    }

    /// Upload the batch to the bucket as Parquet, one file per topic when the path holds `{topic}`; returns the files' `gs://` URIs and their bytes.
    async fn upload_files(
        &self,
        batch: &Batch,
        digest: &str,
    ) -> Result<(Vec<String>, u64), DeliveryError> {
        let Some(gcs) = self.config.staging.gcs.as_ref() else {
            return Err(DeliveryError::Fatal("staging.gcs is missing".to_owned()));
        };
        let by_topic = gcs.path.contains("{topic}");
        let mut parts: std::collections::BTreeMap<&str, Vec<&BatchRow>> = Default::default();
        for row in &batch.rows {
            let topic = match by_topic {
                true => row.topic.as_str(),
                false => batch.rows[0].topic.as_str(),
            };
            parts.entry(topic).or_default().push(row);
        }
        let now = now_ms();
        let prefix = gcs.prefix.trim_matches('/');
        let mut uris: Vec<String> = Vec::with_capacity(parts.len());
        let mut bytes = 0;
        for (topic, rows) in parts {
            let body = batch_parquet(&self.rows(&rows)?).map_err(DeliveryError::Fatal)?;
            bytes += body.len() as u64;
            let folder = render_folder(
                &gcs.path,
                &FolderValues {
                    topic,
                    table: &self.stream.table,
                    stream: &self.stream.name,
                    now_ms: now,
                },
            );
            let name = match prefix.is_empty() {
                true => format!("{folder}{}_{}_{digest}.parquet", self.stream.name, self.run),
                false => format!(
                    "{prefix}/{folder}{}_{}_{digest}.parquet",
                    self.stream.name, self.run
                ),
            };
            put_object_gcs(&self.api, (&gcs.bucket, &name), body.into())
                .await
                .map_err(|error| retry("Cloud Storage upload", &error))?;
            uris.push(format!("gs://{}/{name}", gcs.bucket));
        }
        Ok((uris, bytes))
    }

    /// Run the batch's load job, after the table's previous one by [`LOAD_JOB_INTERVAL`], and say how it ended; a job of that name that already ran is read instead of run again.
    async fn run_job(
        &mut self,
        staged: &StagedLoad,
        upload: Option<bytes::Bytes>,
    ) -> Result<JobEnd, DeliveryError> {
        let job_id = load_job_id(
            &self.stream.table,
            &self.run,
            &staged.digest,
            staged.attempt,
        );
        if let Some(last) = self.last_load {
            tokio::time::sleep_until(last + LOAD_JOB_INTERVAL).await;
        }
        self.last_load = Some(tokio::time::Instant::now());
        let job = self.job(&job_id, &staged.uris);
        let Ok(started) =
            tokio::time::timeout(LOAD_JOB_WAIT, load_job(&self.api, &job, upload)).await
        else {
            return Err(DeliveryError::Retry(format!(
                "load job {job_id} did not finish within {} minutes",
                LOAD_JOB_WAIT.as_secs() / 60
            )));
        };
        let resource = started.map_err(|error| retry("the load job", &error))?;
        let error = &resource["status"]["errorResult"];
        if error.is_null() {
            return Ok(JobEnd::Loaded);
        }
        let reason = error["reason"].as_str().unwrap_or_default();
        let text = format!(
            "load job {job_id} failed: {reason}: {}",
            error["message"].as_str().unwrap_or_default()
        );
        Ok(JobEnd::Failed(
            match RETRYABLE_JOB_REASONS.contains(&reason) {
                true => DeliveryError::Retry(text),
                false => DeliveryError::Fatal(text),
            },
        ))
    }

    /// The `jobs.insert` body: a Parquet load appending to the table, from `uris` or, without, from the file sent with it.
    fn job(&self, job_id: &str, uris: &[String]) -> Value {
        let project = self.config.destination.database();
        let mut load = json!({
            "destinationTable": {
                "projectId": project,
                "datasetId": self.config.destination.schema(),
                "tableId": self.stream.table,
            },
            "sourceFormat": "PARQUET",
            "writeDisposition": "WRITE_APPEND",
            "createDisposition": "CREATE_NEVER",
        });
        if !uris.is_empty() {
            load["sourceUris"] = json!(uris);
        }
        json!({
            "jobReference": {"projectId": project, "jobId": job_id, "location": self.api.location()},
            "configuration": {"load": load},
        })
    }
}

/// How a load job ended: loaded, or failed with the error to retry or stop on.
enum JobEnd {
    Loaded,
    Failed(DeliveryError),
}

/// The most characters of the table name a load job's name holds, well inside BigQuery's 1,024 for a job id.
const MAX_JOB_TABLE_CHARS: usize = 200;

/// `sqlload_<table>_<run>_<digest>_<attempt>`, with characters a job id cannot hold replaced by `_`.
pub(crate) fn load_job_id(table: &str, run: &str, digest: &str, attempt: u32) -> String {
    let table: String = table
        .chars()
        .take(MAX_JOB_TABLE_CHARS)
        .map(|c| match c.is_ascii_alphanumeric() || c == '-' {
            true => c,
            false => '_',
        })
        .collect();
    format!("sqlload_{table}_{run}_{digest}_{attempt}")
}

/// `rows` with timestamps in microseconds, as the Storage Write API reads them.
fn write_api_rows(rows: &RecordBatch) -> Result<RecordBatch, String> {
    let microseconds = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let mut fields: Vec<Field> = Vec::with_capacity(rows.num_columns());
    let mut arrays = Vec::with_capacity(rows.num_columns());
    for (field, array) in rows.schema().fields().iter().zip(rows.columns()) {
        let array = match field.data_type() {
            DataType::Timestamp(_, _) => {
                arrow_cast::cast(array, &microseconds).map_err(|error| error.to_string())?
            }
            _ => array.clone(),
        };
        fields.push(Field::new(field.name(), array.data_type().clone(), true));
        arrays.push(array);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|error| error.to_string())
}

/// `rows` cut into append requests to `stream` of at most `limit` encoded bytes each, every one with the writer schema; a row larger than `limit` goes alone, for BigQuery to refuse.
pub(crate) fn append_chunks(
    rows: &RecordBatch,
    limit: usize,
    stream: &str,
) -> Result<Vec<AppendRowsRequest>, ApiError> {
    let total = rows.num_rows();
    let memory = rows.get_array_memory_size().max(1);
    let mut step = (total.saturating_mul(limit) / memory).clamp(1, total.max(1));
    let mut requests: Vec<AppendRowsRequest> = Vec::new();
    let mut start = 0;
    while start < total {
        let length = step.min(total - start);
        let mut request = arrow_request(stream, 0, &rows.slice(start, length), true)?;
        if request.encoded_len() > limit && length > 1 {
            step = length / 2;
            continue;
        }
        request.offset = None;
        requests.push(request);
        start += length;
    }
    Ok(requests)
}

/// Read a stream's answers until it ends: how many appends it acknowledged, or the first error or refused row.
async fn read_answers(
    mut responses: tonic::Streaming<crate::bigquery_api::models::AppendRowsResponse>,
) -> Result<usize, DeliveryError> {
    let mut answered = 0;
    loop {
        let answer = append_answer(&mut responses)
            .await
            .map_err(|error| retry("Storage Write API append", &error))?;
        let Some(answer) = answer else {
            return Ok(answered);
        };
        if let Some(refused) = answer.row_errors.first() {
            return Err(DeliveryError::Fatal(format!(
                "BigQuery refused {} row(s) of an append, the first at index {}: {}",
                answer.row_errors.len(),
                refused.index,
                refused.message
            )));
        }
        if let Some(error) = answer.error {
            let text = format!(
                "Storage Write API append failed: {} (code {})",
                error.message, error.code
            );
            return Err(match RETRYABLE_APPEND_CODES.contains(&error.code) {
                true => DeliveryError::Retry(text),
                false => DeliveryError::Fatal(text),
            });
        }
        answered += 1;
    }
}
