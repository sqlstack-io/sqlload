use crate::batching::models::{Batch, BatchRow};
use crate::config::models::Config;
use crate::delivery::_helpers::stage_paths::{
    FolderValues, expired_dates, render_folder, s3_target,
};
use crate::delivery::constants::{MAX_HISTORY_PAGES, PRUNE_LOOKBACK};
use crate::delivery::errors::DeliveryError;
use crate::delivery::models::{
    Confirmation, DeliveryReport, PendingLoads, StageClients, StageDelivery, StagedBatch,
    StagedFile,
};
use crate::file_format::main::copy_into::copy_into;
use crate::file_format::main::encode_file::encode_file;
use crate::file_format::main::file_extension::file_extension;
use crate::http::errors::ApiError;
use crate::naming::main::qualify_stage::qualify_stage;
use crate::naming::main::quote_name::quote_name;
use crate::naming::models::Stream;
use crate::planning::models::LoadPath;
use crate::sql_api::main::execute_sql::execute_sql;
use crate::stage_api::constants::CREDENTIAL_MARGIN_MS;
use crate::stage_api::main::head_object::head_object;
use crate::stage_api::main::insert_files::insert_files;
use crate::stage_api::main::insert_report::insert_report;
use crate::stage_api::main::load_history_scan::load_history_scan;
use crate::stage_api::main::put_object::put_object;
use crate::stage_api::main::put_stage_file::put_stage_file;
use crate::stage_api::models::{ObjectPresence, ObjectUpload, PipeTarget, PutOutcome, StageInfo};
use crate::time_text::main::iso_utc::iso_utc;
use crate::time_text::main::now_ms::now_ms;
use crate::time_text::main::utc_date::utc_date;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;

impl StageDelivery {
    pub(crate) fn new(
        clients: StageClients,
        config: Arc<Config>,
        stream: Stream,
        path: LoadPath,
    ) -> Result<Self, DeliveryError> {
        let s3 = match path {
            LoadPath::S3AutoIngest | LoadPath::S3Copy | LoadPath::S3InsertFiles => {
                Some(s3_target(&config).map_err(DeliveryError::Fatal)?)
            }
            _ => None,
        };
        let pending = PendingLoads::new(config.staging.on_load_error);
        Ok(Self {
            clients,
            config,
            stream,
            path,
            stage_info: None,
            s3,
            staged: None,
            pending,
            begin_mark: None,
        })
    }

    /// Upload the batch as one file per folder and load them; COPY loads confirm the offsets at once, `insertFiles` loads after [`Self::poll`] sees them loaded.
    pub(crate) async fn deliver(&mut self, batch: &Batch) -> Result<DeliveryReport, DeliveryError> {
        let parts = self.parts(batch);
        let mut staged = self.staged_batch(batch, &parts);
        let mut uncompressed_bytes = 0;
        let mut sent_bytes = 0;
        for (index, part) in parts.iter().enumerate() {
            let lines: Vec<&str> = part.rows.iter().map(|row| row.line.as_str()).collect();
            let (body, plain) = encode_file(
                self.config.staging.format,
                &self.config.envelope.columns,
                &lines,
            )
            .map_err(DeliveryError::Fatal)?;
            uncompressed_bytes += plain;
            sent_bytes += body.len() as u64;
            if !staged.files[index].uploaded {
                let key = staged.files[index].key.clone();
                let uploaded = self.upload(&key, &body).await;
                if let Err(error) = uploaded {
                    self.staged = Some(staged);
                    return Err(error);
                }
                staged.files[index].uploaded = true;
                self.staged = Some(staged.clone());
            }
        }
        self.staged = Some(staged.clone());
        let keys: Vec<String> = staged.files.iter().map(|file| file.key.clone()).collect();
        let skipped = self.load(&keys).await?;
        self.staged = None;
        let confirmed = match self.confirms_later() {
            true => {
                self.pending
                    .add(&keys, &skipped, batch.last_offsets(), now_ms());
                BTreeMap::new()
            }
            false => batch.last_offsets(),
        };
        Ok(DeliveryReport {
            rows: batch.rows.len() as u64,
            uncompressed_bytes,
            sent_bytes,
            requests: keys.len() as u64,
            skipped_rows: 0,
            confirmed,
        })
    }

    fn confirms_later(&self) -> bool {
        matches!(
            self.path,
            LoadPath::StageInsertFiles | LoadPath::S3InsertFiles
        )
    }

    pub(crate) fn is_full(&self) -> bool {
        self.pending.file_count() >= self.config.staging.max_in_flight
    }

    /// Read the pipe's load report and return the offsets of the oldest batches whose files are all loaded.
    pub(crate) async fn poll(&mut self) -> Result<Confirmation, DeliveryError> {
        if self.pending.is_empty() {
            return Ok(Confirmation::default());
        }
        let pipe = self.qualified(&self.stream.pipe);
        let report = insert_report(&self.pipe_target(&pipe), self.begin_mark.as_deref())
            .await
            .map_err(|error| retry("insertReport", &error))?;
        if report.next_begin_mark.is_some() {
            self.begin_mark = report.next_begin_mark;
        }
        self.pending.apply(&report.files);
        self.check_overdue(&pipe, now_ms()).await?;
        let settled = self.pending.settle();
        for (key, reason) in &settled.skipped {
            tracing::warn!(stream = %self.stream.name, file = %key, %reason, "Snowpipe did not load a file; skipping it as on_load_error = \"skip\"");
        }
        if let Some(reason) = settled.halted.filter(|_| settled.confirmed.is_empty()) {
            return Err(DeliveryError::Fatal(format!(
                "Snowpipe did not load {reason}; set on_load_error = \"skip\" to commit past failed files"
            )));
        }
        Ok(Confirmation {
            confirmed: settled.confirmed,
            error_rows: 0,
            skipped_files: settled.skipped.len() as u64,
        })
    }

    /// Look up files with no report in the load history; send those still in the stage once more and fail the rest.
    async fn check_overdue(&mut self, pipe: &str, now: i64) -> Result<(), DeliveryError> {
        let overdue = self.pending.overdue(now);
        let Some(start_ms) = self.pending.history_start(&overdue) else {
            return Ok(());
        };
        let mut start = iso_utc(start_ms);
        for _ in 0..MAX_HISTORY_PAGES {
            let page = load_history_scan(&self.pipe_target(pipe), &start)
                .await
                .map_err(|error| retry("loadHistoryScan", &error))?;
            self.pending.apply(&page.files);
            match page.range_end {
                Some(next) if !page.complete && next != start => start = next,
                _ => break,
            }
        }
        let mut resend: Vec<String> = Vec::new();
        for key in self.pending.overdue(now) {
            if self.pending.was_resent(&key) {
                self.pending
                    .fail(&key, "not reported by Snowpipe after being sent twice");
            } else if self.exists(&key).await {
                resend.push(key);
            } else {
                self.pending.fail(&key, "no longer in the stage");
            }
        }
        if resend.is_empty() {
            return Ok(());
        }
        tracing::warn!(stream = %self.stream.name, files = ?resend, "Snowpipe has not reported files; sending them again");
        let skipped = insert_files(&self.pipe_target(pipe), &resend)
            .await
            .map_err(|error| retry("insertFiles", &error))?;
        for key in &resend {
            let was_skipped = skipped
                .iter()
                .any(|path| path.trim_start_matches('/') == key);
            self.pending.resend(key, was_skipped, now);
        }
        Ok(())
    }

    /// Whether a staged file still exists; true when that cannot be told, so it is sent again.
    async fn exists(&mut self, key: &str) -> bool {
        let found = match self.s3.clone() {
            Some(target) => head_object(&self.clients.http, &target, key).await,
            None => match self.stage_info().await {
                Ok(stage) => head_object(&self.clients.http, &stage.target, key).await,
                Err(error) => {
                    tracing::warn!(stream = %self.stream.name, file = %key, %error, "cannot check whether a staged file exists");
                    return true;
                }
            },
        };
        match found {
            Ok(ObjectPresence::Missing) => false,
            Ok(ObjectPresence::Found) => true,
            Ok(ObjectPresence::CredentialsExpired) => {
                self.stage_info = None;
                if let (Some(_), Ok(target)) = (&self.s3, s3_target(&self.config)) {
                    self.s3 = Some(target);
                }
                true
            }
            Err(error) => {
                tracing::warn!(stream = %self.stream.name, file = %key, %error, "cannot check whether a staged file exists");
                true
            }
        }
    }

    fn pipe_target<'a>(&'a self, pipe: &'a str) -> PipeTarget<'a> {
        PipeTarget {
            http: &self.clients.http,
            account_url: &self.clients.account_url,
            auth: &self.clients.auth,
            pipe,
        }
    }

    /// One part per topic when the `s3` folder names the topic, otherwise the whole batch.
    fn parts<'a>(&self, batch: &'a Batch) -> Vec<Part<'a>> {
        let by_topic = self.s3.is_some()
            && self
                .config
                .staging
                .s3
                .as_ref()
                .is_some_and(|s3| s3.path.contains("{topic}"));
        let mut parts: BTreeMap<&str, Vec<&BatchRow>> = BTreeMap::new();
        for row in &batch.rows {
            let topic = match by_topic {
                true => row.topic.as_str(),
                false => "",
            };
            parts.entry(topic).or_default().push(row);
        }
        parts
            .into_iter()
            .map(|(topic, rows)| Part {
                topic: match topic.is_empty() {
                    true => batch.rows.first().map_or("", |row| row.topic.as_str()),
                    false => topic,
                },
                rows,
            })
            .collect()
    }

    /// `<folder>/<stream>_<digest>.<json.gz|parquet>`: the `s3` path template, or the UTC date on an internal stage.
    fn file_key(&self, part: &Part<'_>, now: i64) -> String {
        let file = format!(
            "{}_{}.{}",
            self.stream.name,
            rows_digest(&part.rows),
            file_extension(self.config.staging.format)
        );
        let template = match &self.s3 {
            Some(_) => self.config.staging.s3.as_ref().map(|s3| s3.path.as_str()),
            None => None,
        };
        let folder = match template {
            Some(template) => render_folder(
                template,
                &FolderValues {
                    topic: part.topic,
                    table: &self.stream.table,
                    stream: &self.stream.name,
                    now_ms: now,
                },
            ),
            None => format!("{}/", utc_date(now)),
        };
        format!("{folder}{file}")
    }

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

    async fn upload(&mut self, key: &str, body: &[u8]) -> Result<(), DeliveryError> {
        let _slot = self.clients.in_flight.upload().await;
        if let Some(target) = &self.s3 {
            let headers: BTreeMap<String, String> = BTreeMap::new();
            let upload = ObjectUpload {
                target,
                key,
                headers: &headers,
            };
            let outcome = put_object(&self.clients.http, upload, body.to_vec())
                .await
                .map_err(|error| retry("upload", &error))?;
            return match outcome {
                PutOutcome::Uploaded => Ok(()),
                PutOutcome::CredentialsExpired => {
                    self.s3 = Some(s3_target(&self.config).map_err(DeliveryError::Fatal)?);
                    Err(DeliveryError::Retry(
                        "the S3 credentials expired; reloaded them from the environment".to_owned(),
                    ))
                }
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
                .map_err(|error| {
                    DeliveryError::Retry(format!("stage credentials failed: {error}"))
                })?;
            self.stage_info = Some(info);
        }
        self.stage_info
            .clone()
            .ok_or_else(|| DeliveryError::Retry("no stage credentials".to_owned()))
    }

    /// Load the files; returns the paths an `insertFiles` pipe skipped because it received them before.
    async fn load(&self, keys: &[String]) -> Result<Vec<String>, DeliveryError> {
        let _slot = self.clients.in_flight.load().await;
        let location = match self.path {
            LoadPath::S3AutoIngest => return Ok(Vec::new()),
            LoadPath::StageInsertFiles | LoadPath::S3InsertFiles => {
                let pipe = self.qualified(&self.stream.pipe);
                return insert_files(&self.pipe_target(&pipe), keys)
                    .await
                    .map_err(|error| retry("insertFiles", &error));
            }
            LoadPath::StageCopy => format!("@{}/", self.qualified(&self.stream.stage)),
            LoadPath::S3Copy => {
                let prefix = self.s3.as_ref().map_or("", |target| target.prefix.as_str());
                format!("@{}/{prefix}", self.external_stage())
            }
            LoadPath::Streaming => {
                return Err(DeliveryError::Fatal(
                    "streaming is not a stage load".to_owned(),
                ));
            }
        };
        let mut folders: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for key in keys {
            let (folder, file) = key.rsplit_once('/').unwrap_or(("", key.as_str()));
            folders.entry(folder).or_default().push(file);
        }
        for (folder, files) in folders {
            let names: Vec<String> = files.iter().map(|file| format!("'{file}'")).collect();
            let from = match folder.is_empty() {
                true => location.clone(),
                false => format!("{location}{folder}/"),
            };
            let statement = copy_into(
                &self.config,
                &self.qualified(&self.stream.table),
                &from,
                &format!(" FILES = ({})", names.join(", ")),
            );
            execute_sql(&self.clients.sql, &statement)
                .await
                .map_err(|error| retry("COPY", &error))?;
        }
        Ok(Vec::new())
    }

    fn external_stage(&self) -> String {
        let stage = self
            .config
            .staging
            .s3
            .as_ref()
            .map(|s3| s3.stage.as_str())
            .unwrap_or_default();
        qualify_stage(
            &self.config.destination.database,
            &self.config.destination.schema,
            stage,
        )
    }

    /// Remove dated stage folders older than the retention; only for internal stages.
    pub(crate) async fn prune(&self) -> Result<u64, DeliveryError> {
        if !matches!(self.path, LoadPath::StageCopy | LoadPath::StageInsertFiles) {
            return Ok(0);
        }
        let stage = self.qualified(&self.stream.stage);
        let mut removed = 0;
        for date in expired_dates(now_ms(), self.config.staging.retain, PRUNE_LOOKBACK) {
            let rows = execute_sql(&self.clients.sql, &format!("REMOVE @{stage}/{date}/"))
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

/// One file's rows.
struct Part<'a> {
    topic: &'a str,
    rows: Vec<&'a BatchRow>,
}

/// A stable name for rows' content, so a retried upload and load of the same rows is deduplicated by Snowflake's load history.
fn rows_digest(rows: &[&BatchRow]) -> String {
    let mut ranges: BTreeMap<(&str, i32), (i64, i64)> = BTreeMap::new();
    for row in rows {
        let range = ranges
            .entry((row.topic.as_str(), row.partition))
            .or_insert((row.offset, row.offset));
        range.0 = range.0.min(row.offset);
        range.1 = range.1.max(row.offset);
    }
    let mut hasher = Sha256::new();
    for ((topic, partition), (first, last)) in ranges {
        hasher.update(format!("{topic}:{partition}:{first}-{last};").as_bytes());
    }
    hex::encode(&hasher.finalize()[..12])
}

fn retry(action: &str, error: &ApiError) -> DeliveryError {
    match error.is_retryable() {
        true => DeliveryError::Retry(format!("{action} failed: {error}")),
        false => DeliveryError::Fatal(format!("{action} failed: {error}")),
    }
}
