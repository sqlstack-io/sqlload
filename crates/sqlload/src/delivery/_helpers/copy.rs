use std::collections::BTreeMap;

use crate::batching::models::Batch;
use crate::config::models::FileFormat;
use crate::databricks_api::main::delete_path::delete_path;
use crate::databricks_api::main::execute_statement::execute_statement;
use crate::databricks_api::main::list_directory::list_directory;
use crate::databricks_api::main::put_file::put_file;
use crate::delivery::_helpers::stage::{Part, by_topic, retry, rows_digest, split_parts};
use crate::delivery::_helpers::stage_paths::{
    FolderValues, expired_dates, render_folder, s3_target,
};
use crate::delivery::constants::PRUNE_LOOKBACK;
use crate::delivery::errors::DeliveryError;
use crate::delivery::models::{CopyDelivery, DeliveryReport, StagedBatch, StagedFile};
use crate::file_format::main::databricks_copy::databricks_copy;
use crate::file_format::main::encode_file::encode_file;
use crate::file_format::main::file_extension::file_extension;
use crate::http::errors::ApiError;
use crate::naming::main::quoted_table::quoted_table;
use crate::stage_api::main::put_object::put_object;
use crate::stage_api::models::{ObjectUpload, PutOutcome};
use crate::time_text::main::now_ms::now_ms;
use crate::time_text::main::utc_date::utc_date;

impl CopyDelivery {
    /// Upload the batch as Parquet, one file per folder, then load the files with `COPY INTO`; the offsets are confirmed once the load returns.
    pub(crate) async fn deliver(&mut self, batch: &Batch) -> Result<DeliveryReport, DeliveryError> {
        let parts = split_parts(batch, self.s3.is_some() && by_topic(&self.config));
        let mut staged = self.staged_batch(batch, &parts);
        let mut uncompressed_bytes = 0;
        let mut sent_bytes = 0;
        for (index, part) in parts.iter().enumerate() {
            let lines: Vec<&str> = part.rows.iter().map(|row| row.line.as_str()).collect();
            let (body, plain) =
                encode_file(FileFormat::Parquet, &self.config.envelope.columns, &lines)
                    .map_err(DeliveryError::Fatal)?;
            uncompressed_bytes += plain;
            sent_bytes += body.len() as u64;
            if staged.files[index].uploaded {
                continue;
            }
            let key = staged.files[index].key.clone();
            let uploaded = self.upload(&key, body).await;
            if let Err(error) = uploaded {
                self.staged = Some(staged);
                return Err(error);
            }
            staged.files[index].uploaded = true;
        }
        self.staged = Some(staged.clone());
        let keys: Vec<&str> = staged.files.iter().map(|file| file.key.as_str()).collect();
        self.load(&keys).await?;
        self.staged = None;
        Ok(DeliveryReport {
            rows: batch.rows.len() as u64,
            uncompressed_bytes,
            sent_bytes,
            requests: keys.len() as u64,
            skipped_rows: 0,
            confirmed: batch.last_offsets(),
        })
    }

    /// The batch's file keys, kept from an earlier failed attempt at the same rows so a retry reuses its names and uploads.
    fn staged_batch(&self, batch: &Batch, parts: &[Part<'_>]) -> StagedBatch {
        let digest = rows_digest(&batch.rows.iter().collect::<Vec<_>>());
        if let Some(staged) = self
            .staged
            .as_ref()
            .filter(|staged| staged.digest == digest)
        {
            return staged.clone();
        }
        let now = now_ms();
        let files: Vec<StagedFile> = parts
            .iter()
            .map(|part| StagedFile {
                key: self.file_key(part, now),
                uploaded: false,
            })
            .collect();
        StagedBatch { digest, files }
    }

    /// `<folder>/<stream>_<digest>.parquet`: the `s3` path template, or the UTC date in the volume.
    fn file_key(&self, part: &Part<'_>, now: i64) -> String {
        let file = format!(
            "{}_{}.{}",
            self.stream.name,
            rows_digest(&part.rows),
            file_extension(FileFormat::Parquet)
        );
        let folder = match (&self.s3, self.config.staging.s3.as_ref()) {
            (Some(_), Some(s3)) => render_folder(
                &s3.path,
                &FolderValues {
                    topic: part.topic,
                    table: &self.stream.table,
                    stream: &self.stream.name,
                    now_ms: now,
                },
            ),
            _ => format!("{}/", utc_date(now)),
        };
        format!("{folder}{file}")
    }

    async fn upload(&mut self, key: &str, body: Vec<u8>) -> Result<(), DeliveryError> {
        let _slot = self.in_flight.upload().await;
        let Some(target) = &self.s3 else {
            return put_file(&self.api, &self.volume_path(key), body.into())
                .await
                .map_err(|error| retry("volume upload", &error));
        };
        let headers: BTreeMap<String, String> = BTreeMap::new();
        let upload = ObjectUpload {
            target,
            key,
            headers: &headers,
        };
        match put_object(&self.http, upload, body)
            .await
            .map_err(|error| retry("upload", &error))?
        {
            PutOutcome::Uploaded => Ok(()),
            PutOutcome::CredentialsExpired => {
                self.s3 = Some(s3_target(&self.config).map_err(DeliveryError::Fatal)?);
                Err(DeliveryError::Retry(
                    "the S3 credentials expired; reloaded them from the environment".to_owned(),
                ))
            }
        }
    }

    /// One `COPY INTO` per folder; each is one Delta commit.
    async fn load(&self, keys: &[&str]) -> Result<(), DeliveryError> {
        let _slot = self.in_flight.load().await;
        let mut folders: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for key in keys {
            let (folder, file) = key.rsplit_once('/').unwrap_or(("", key));
            folders.entry(folder).or_default().push(file);
        }
        let table = quoted_table(&self.config, &self.stream.table);
        for (folder, files) in folders {
            let location = match &self.s3 {
                Some(target) => format!("s3://{}/{}{folder}/", target.bucket, target.prefix),
                None => format!("{}/", self.volume_path(folder)),
            };
            let statement =
                databricks_copy(&table, &self.config.envelope.columns, &location, &files);
            execute_statement(&self.api, &statement)
                .await
                .map_err(copy_error)?;
        }
        Ok(())
    }

    /// `/Volumes/<catalog>/<schema>/<volume>/<key>`.
    fn volume_path(&self, key: &str) -> String {
        format!(
            "/Volumes/{}/{}/{}/{key}",
            self.config.destination.database(),
            self.config.destination.schema(),
            self.stream.stage
        )
    }

    /// Remove the volume's dated folders older than the retention; files in a bucket are left to its lifecycle rules.
    pub(crate) async fn prune(&self) -> Result<u64, DeliveryError> {
        if self.s3.is_some() {
            return Ok(0);
        }
        let mut removed = 0;
        for date in expired_dates(now_ms(), self.config.staging.retain, PRUNE_LOOKBACK) {
            let folder = self.volume_path(&date);
            let entries = list_directory(&self.api, &folder)
                .await
                .map_err(|error| retry("volume cleanup", &error))?;
            let Some(entries) = entries else {
                continue;
            };
            for entry in entries.iter().filter(|entry| !entry.is_directory) {
                delete_path(&self.api, &entry.path, false)
                    .await
                    .map_err(|error| retry("volume cleanup", &error))?;
                removed += 1;
            }
            delete_path(&self.api, &folder, true)
                .await
                .map_err(|error| retry("volume cleanup", &error))?;
        }
        Ok(removed)
    }
}

/// A `COPY INTO` that failed is retried, within the batch time limit, unless sign-in was refused: a warehouse restart cancels statements and concurrent Delta commits conflict, and a retry loads only the files not loaded yet.
fn copy_error(error: ApiError) -> DeliveryError {
    match error {
        ApiError::Auth(_) => DeliveryError::Fatal(format!("COPY INTO failed: {error}")),
        error => DeliveryError::Retry(format!("COPY INTO failed: {error}")),
    }
}
