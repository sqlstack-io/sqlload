use crate::batching::main::gzip_bytes::gzip_bytes;
use crate::batching::models::Batch;
use crate::config::models::Config;
use crate::delivery::_helpers::stage_paths::{expired_dates, s3_target};
use crate::delivery::errors::DeliveryError;
use crate::delivery::models::{DeliveryReport, StageClients, StageDelivery};
use crate::http::errors::ApiError;
use crate::naming::main::quote_name::quote_name;
use crate::naming::models::Stream;
use crate::planning::models::LoadPath;
use crate::stage_api::constants::CREDENTIAL_MARGIN_MS;
use crate::stage_api::main::insert_files::insert_files;
use crate::stage_api::main::put_object::put_object;
use crate::stage_api::main::put_stage_file::put_stage_file;
use crate::stage_api::models::{PutOutcome, StageInfo};
use crate::time_text::main::now_ms::now_ms;
use crate::time_text::main::utc_date::utc_date;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

impl StageDelivery {
    pub(crate) fn new(
        clients: StageClients,
        config: Arc<Config>,
        stream: Stream,
        path: LoadPath,
    ) -> Result<Self, DeliveryError> {
        let s3 = match path {
            LoadPath::S3AutoIngest | LoadPath::S3Copy => {
                Some(s3_target(&config, &stream).map_err(DeliveryError::Fatal)?)
            }
            _ => None,
        };
        Ok(Self {
            clients,
            config,
            stream,
            path,
            stage_info: None,
            s3,
        })
    }

    /// Upload the batch as one file and load it; the offsets are confirmed once the load is accepted.
    pub(crate) async fn deliver(&mut self, batch: &Batch) -> Result<DeliveryReport, DeliveryError> {
        let plain = batch.ndjson();
        let body = gzip_bytes(&plain);
        let date = utc_date(now_ms());
        let file = format!("{}_{}.json.gz", self.stream.name, batch_digest(batch));
        let key = format!("{date}/{file}");
        self.upload(&key, &body).await?;
        self.load(&date, &file).await?;
        Ok(DeliveryReport {
            rows: batch.rows.len() as u64,
            uncompressed_bytes: plain.len() as u64,
            sent_bytes: body.len() as u64,
            requests: 1,
            skipped_rows: 0,
            confirmed: batch.last_offsets(),
        })
    }

    async fn upload(&mut self, key: &str, body: &[u8]) -> Result<(), DeliveryError> {
        if let Some(target) = &self.s3 {
            let outcome = put_object(
                &self.clients.http,
                target,
                key,
                body.to_vec(),
                BTreeMap::new(),
            )
            .await
            .map_err(|error| retry("upload", &error))?;
            return match outcome {
                PutOutcome::Uploaded => Ok(()),
                PutOutcome::CredentialsExpired => Err(DeliveryError::Retry(
                    "the S3 credentials expired".to_owned(),
                )),
            };
        }
        for _ in 0..2 {
            let stage = self.stage_info().await?;
            match put_stage_file(&self.clients.http, &stage, key, body).await {
                Ok(PutOutcome::Uploaded) => return Ok(()),
                Ok(PutOutcome::CredentialsExpired) => self.stage_info = None,
                Err(error) => return Err(retry("stage upload", &error)),
            }
        }
        Err(DeliveryError::Retry(
            "stage credentials expired twice in a row".to_owned(),
        ))
    }

    async fn stage_info(&mut self) -> Result<StageInfo, DeliveryError> {
        let fresh = self.stage_info.as_ref().is_some_and(|info| {
            info.credentials_expire_ms
                .is_none_or(|expires| expires - CREDENTIAL_MARGIN_MS > now_ms())
        });
        if !fresh {
            let stage = self.qualified(&self.stream.stage);
            let info = self
                .clients
                .driver
                .stage_info(&stage)
                .await
                .map_err(|error| retry("stage credentials", &error))?;
            self.stage_info = Some(info);
        }
        self.stage_info
            .clone()
            .ok_or_else(|| DeliveryError::Retry("no stage credentials".to_owned()))
    }

    async fn load(&self, date: &str, file: &str) -> Result<(), DeliveryError> {
        let relative = format!("{date}/{file}");
        let copy_from = match self.path {
            LoadPath::S3AutoIngest => return Ok(()),
            LoadPath::StageInsertFiles => {
                let pipe = self.qualified(&self.stream.pipe);
                return insert_files(
                    &self.clients.http,
                    &self.clients.account_url,
                    &self.clients.auth,
                    &pipe,
                    &[relative],
                )
                .await
                .map_err(|error| retry("insertFiles", &error));
            }
            LoadPath::StageCopy => format!("@{}/{date}/", self.qualified(&self.stream.stage)),
            LoadPath::S3Copy => {
                let stage = self
                    .config
                    .staging
                    .s3
                    .as_ref()
                    .map(|s3| s3.stage.as_str())
                    .unwrap_or_default();
                format!("@{}/{}/{date}/", self.qualified(stage), self.stream.table)
            }
            LoadPath::Streaming => {
                return Err(DeliveryError::Fatal(
                    "streaming is not a stage load".to_owned(),
                ));
            }
        };
        let statement = format!(
            "COPY INTO {} FROM {copy_from} FILES = ('{file}') FILE_FORMAT = (TYPE = JSON COMPRESSION = AUTO) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
            self.qualified(&self.stream.table)
        );
        self.clients
            .sql
            .execute(&statement)
            .await
            .map(|_| ())
            .map_err(|error| retry("COPY", &error))
    }

    /// Remove dated stage folders older than the retention; only for internal stages.
    pub(crate) async fn prune(&self) -> Result<u64, DeliveryError> {
        if !matches!(self.path, LoadPath::StageCopy | LoadPath::StageInsertFiles) {
            return Ok(0);
        }
        let stage = self.qualified(&self.stream.stage);
        let mut removed = 0;
        for date in expired_dates(
            now_ms(),
            self.config.staging.retain,
            Duration::from_secs(30 * 86_400),
        ) {
            let rows = self
                .clients
                .sql
                .execute(&format!("REMOVE @{stage}/{date}/"))
                .await
                .map_err(|error| retry("stage cleanup", &error))?;
            removed += rows.rows.len() as u64;
        }
        Ok(removed)
    }

    fn qualified(&self, name: &str) -> String {
        format!(
            "{}.{}.{}",
            quote_name(&self.config.destination.database),
            quote_name(&self.config.destination.schema),
            quote_name(name)
        )
    }
}

/// A stable name for the batch's content, so a retried upload and load of the same rows is deduplicated by Snowflake's load history.
fn batch_digest(batch: &Batch) -> String {
    let mut hasher = Sha256::new();
    for (partition, rows) in batch.by_partition() {
        let first = rows.first().map_or(0, |row| row.offset);
        let last = rows.last().map_or(0, |row| row.offset);
        hasher.update(
            format!(
                "{}:{}:{first}-{last};",
                partition.topic, partition.partition
            )
            .as_bytes(),
        );
    }
    hex::encode(&hasher.finalize()[..12])
}

fn retry(action: &str, error: &ApiError) -> DeliveryError {
    DeliveryError::Retry(format!("{action} failed: {error}"))
}
