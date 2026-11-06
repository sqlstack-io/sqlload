//! One `postgres_cdc` source's load: its slot (created with a snapshot of every table when missing), then its changes up to where the load started, written batch by batch of whole transactions, the slot confirming a batch only once every table it touched has committed.

use std::collections::BTreeMap;
use std::time::Duration;

use pgwire_replication::client::ReplicationEvent;
use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, TlsConfig};
use tokio_postgres::Client;

use crate::adapters::contract::models::{ChangeApply, ChangeWrite, WriteMode};
use crate::adapters::dispatch::models::Destination;
use crate::cdc::constants::{BATCH_CHANGES, REPLICATION_OPTIONS, STOP_MESSAGE_PREFIX};
use crate::cdc::main::buffer_change::{StreamStep, buffer_change};
use crate::cdc::main::changes_batch::changes_batch;
use crate::cdc::main::metadata_read_columns::metadata_read_columns;
use crate::cdc::models::ChangeBuffer;
use crate::loader::_helpers::cdc_state::SnapshotLog;
use crate::loader::_helpers::publish::publish_table;
use crate::loader::_helpers::sources::connect_source;
use crate::loader::errors::LoaderError;
use crate::loader::models::{
    CdcSpec, CommitRule, LoadRun, Publisher, SourceReader, TablePlan, WholeReason,
};
use crate::postgres_source::errors::PostgresError;
use crate::postgres_source::models::ReadColumn;
use crate::project::models::{ConnectionConfig, SourceConfig};
use crate::schema_plan::constants::{CDC_PREFIX, CDC_UNCHANGED_COLUMN};

/// How often the replication session reports the confirmed position.
const STATUS_INTERVAL: Duration = Duration::from_millis(100);
/// How long a finished load waits for the slot to show its confirmed position.
const CONFIRM_WAIT: Duration = Duration::from_secs(10);

/// One `postgres_cdc` source as a load sees it.
pub(crate) struct CdcSource<'a> {
    pub(crate) run: &'a LoadRun<'a>,
    pub(crate) name: &'a str,
    pub(crate) source: &'a SourceConfig,
    pub(crate) cdc: &'a CdcSpec,
    pub(crate) tables: Vec<&'a TablePlan>,
    pub(crate) destination: &'a Destination,
}

/// Stop the process at `point` when a crash test asks for it there (`SQLLOAD_TEST_CRASH_AT`); compiled into test builds only.
fn crash_point(point: &str) {
    #[cfg(feature = "container-tests")]
    if std::env::var("SQLLOAD_TEST_CRASH_AT").is_ok_and(|wanted| wanted == point) {
        std::process::abort();
    }
    #[cfg(not(feature = "container-tests"))]
    let _ = point;
}

fn postgres(error: tokio_postgres::Error) -> LoaderError {
    LoaderError::Postgres(PostgresError::from(error))
}

fn replication(error: impl std::fmt::Display) -> LoaderError {
    LoaderError::Invalid(format!("logical replication: {error}"))
}

/// A Postgres identifier, quoted.
fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

impl CdcSource<'_> {
    /// The replication session's settings, starting where the slot confirmed.
    fn replication_config(&self) -> Result<ReplicationConfig, LoaderError> {
        let Some(ConnectionConfig::Postgres {
            host,
            port,
            user,
            password,
            database,
        }) = self.run.project.connections.get(&self.source.connection)
        else {
            return Err(LoaderError::Invalid(format!(
                "connection {:?} is not a Postgres connection",
                self.source.connection
            )));
        };
        let password = password
            .as_ref()
            .map(|secret| secret.expose().to_owned())
            .unwrap_or_default();
        Ok(ReplicationConfig::new(
            host.clone(),
            user.clone(),
            password,
            database.clone(),
            self.cdc.slot.clone(),
            self.cdc.publication.clone(),
        )
        .with_port(*port)
        .with_tls(TlsConfig::disabled())
        .with_status_interval(STATUS_INTERVAL)
        .with_options(REPLICATION_OPTIONS)
        .with_application_name("sqlload"))
    }

    /// The source's snapshot records, beside its first table.
    fn log(&self) -> Result<SnapshotLog<'_>, LoaderError> {
        let first = self
            .tables
            .first()
            .map(|table| &table.spec)
            .ok_or_else(|| {
                LoaderError::Invalid(format!("source {} selects no tables", self.name))
            })?;
        Ok(SnapshotLog {
            destination: self.destination,
            warehouse: first.warehouse,
            beside: &first.destination,
            source: self.name,
            slot: &self.cdc.slot,
        })
    }

    /// Create the publication for the selected tables when it is missing; when it exists, every selected table must be in it.
    async fn ensure_publication(&self, control: &Client) -> Result<(), LoaderError> {
        let publication = &self.cdc.publication;
        let exists = control
            .query_opt(
                "SELECT 1 FROM pg_publication WHERE pubname = $1",
                &[publication],
            )
            .await
            .map_err(postgres)?
            .is_some();
        let names: Vec<String> = self
            .tables
            .iter()
            .map(|table| {
                let source = &table.spec.source_table;
                format!("{}.{}", quoted(&source.schema), quoted(&source.name))
            })
            .collect();
        if !exists {
            control
                .batch_execute(&format!(
                    "CREATE PUBLICATION {} FOR TABLE {}",
                    quoted(publication),
                    names.join(", ")
                ))
                .await
                .map_err(postgres)?;
            return Ok(());
        }
        let published: Vec<(String, String)> = control
            .query(
                "SELECT schemaname::text, tablename::text FROM pg_publication_tables WHERE pubname = $1",
                &[publication],
            )
            .await
            .map_err(postgres)?
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        let missing: Vec<String> = self
            .tables
            .iter()
            .map(|table| &table.spec.source_table)
            .filter(|source| {
                !published
                    .iter()
                    .any(|(schema, name)| *schema == source.schema && *name == source.name)
            })
            .map(|source| format!("{}.{}", quoted(&source.schema), quoted(&source.name)))
            .collect();
        match missing.is_empty() {
            true => Ok(()),
            false => Err(LoaderError::Invalid(format!(
                "publication {publication} does not publish {}; add them with ALTER PUBLICATION {} ADD TABLE {}",
                missing.join(", "),
                quoted(publication),
                missing.join(", ")
            ))),
        }
    }

    /// The slot's confirmed position and WAL status, or `None` when it does not exist.
    async fn slot_state(&self, control: &Client) -> Result<Option<(String, String)>, LoaderError> {
        Ok(control
            .query_opt(
                "SELECT confirmed_flush_lsn::text, COALESCE(wal_status, '') FROM pg_replication_slots WHERE slot_name = $1 AND database = current_database()",
                &[&self.cdc.slot],
            )
            .await
            .map_err(postgres)?
            .map(|row| (row.get::<_, Option<String>>(0).unwrap_or_default(), row.get(1))))
    }

    /// Why the slot cannot resume, if it cannot: SQLLoad did not create it, or some table has no snapshot from it (a snapshot stopped part-way, or the table joined since).
    async fn resume_problem(&self, wal_status: &str) -> Result<Option<String>, LoaderError> {
        let slot = &self.cdc.slot;
        if wal_status == "lost" {
            return Ok(Some(format!(
                "slot {slot} lost WAL it had not confirmed, so changes are missing; run `sqlload load --resnapshot` to drop and recreate it and copy every table again"
            )));
        }
        let Some((_, snapshotted)) = self.log()?.current().await? else {
            return Ok(Some(format!(
                "slot {slot} exists but SQLLoad has no record of creating it, so it cannot tell which changes the tables already hold; drop it, or run `sqlload load --resnapshot` to drop and recreate it and copy every table again"
            )));
        };
        let unfinished: Vec<&str> = self
            .tables
            .iter()
            .map(|table| table.spec.destination.table.as_str())
            .filter(|name| !snapshotted.iter().any(|done| done == name))
            .collect();
        Ok((!unfinished.is_empty()).then(|| {
            format!(
                "{} {} no snapshot from slot {slot} (a snapshot stopped part-way, or the table joined since); run `sqlload load --resnapshot` to drop and recreate the slot and copy every table again",
                unfinished.join(", "),
                match unfinished.len() {
                    1 => "has",
                    _ => "have",
                }
            )
        }))
    }

    /// Load the source: rows written per destination table name.
    pub(crate) async fn load(&self) -> Result<BTreeMap<String, u64>, LoaderError> {
        let control = connect_source(self.run.project, self.source).await?;
        self.ensure_publication(&control).await?;
        self.log()?.ensure().await?;
        let mut rows: BTreeMap<String, u64> = BTreeMap::new();
        let mut slot = self.slot_state(&control).await?;
        if self.run.options.resnapshot && slot.is_some() {
            control
                .execute("SELECT pg_drop_replication_slot($1)", &[&self.cdc.slot])
                .await
                .map_err(postgres)?;
            slot = None;
        }
        let mut stream = match slot {
            Some((_, wal_status)) => {
                if let Some(problem) = self.resume_problem(&wal_status).await? {
                    return Err(LoaderError::Invalid(problem));
                }
                ReplicationClient::connect(self.replication_config()?)
                    .await
                    .map_err(replication)?
            }
            None => self.snapshot(&mut rows).await?,
        };
        let stop: String = control
            .query_one(
                "SELECT pg_logical_emit_message(false, $1, '')::text",
                &[&STOP_MESSAGE_PREFIX],
            )
            .await
            .map_err(postgres)?
            .get(0);
        let stop = Lsn::parse(&stop).map_err(replication)?;
        let streamed = self.stream(&mut stream, stop, &mut rows).await;
        let confirmed = match streamed {
            Ok(Some(end)) => self.await_confirmed(&control, end).await,
            Ok(None) => Ok(()),
            Err(error) => Err(error),
        };
        stream.stop();
        let _ = stream.shutdown().await;
        confirmed.map(|()| rows)
    }

    /// Create the slot with an exported snapshot and copy every table under it, each table's rows getting the snapshot's LSN; returns the session, which streams on from the snapshot.
    async fn snapshot(
        &self,
        rows: &mut BTreeMap<String, u64>,
    ) -> Result<ReplicationClient, LoaderError> {
        let mut session = ReplicationClient::connect_gated(self.replication_config()?)
            .await
            .map_err(replication)?;
        let created = session
            .simple_query(&format!(
                "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput (SNAPSHOT 'export')",
                quoted(&self.cdc.slot)
            ))
            .await
            .map_err(replication)?;
        let field = |index: usize| {
            created
                .first()
                .and_then(|row| row.get(index))
                .and_then(Option::as_ref)
                .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                .ok_or_else(|| replication("CREATE_REPLICATION_SLOT returned no snapshot"))
        };
        let consistent = Lsn::parse(&field(1)?).map_err(replication)?;
        let snapshot_name = field(2)?;
        let snapshot_lsn = i64::try_from(consistent.as_u64()).map_err(replication)?;
        let log = self.log()?;
        log.record("", snapshot_lsn).await?;
        let client = connect_source(self.run.project, self.source).await?;
        client
            .batch_execute(&format!(
                "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT '{}'",
                snapshot_name.replace('\'', "''")
            ))
            .await
            .map_err(postgres)?;
        let reader = SourceReader::Postgres(client, Vec::new());
        for (index, table) in self.tables.iter().enumerate() {
            if index == 1 {
                crash_point("snapshot-after-first-table");
            }
            let mut plan = (*table).clone();
            plan.mode = WriteMode::Replace;
            plan.commits = CommitRule::Whole(WholeReason::Chosen);
            plan.read_columns = with_snapshot_columns(&plan.read_columns, self.cdc, snapshot_lsn);
            let written = publish_table(&reader, &plan, (self.destination, self.run)).await?;
            log.record(&table.spec.destination.table, snapshot_lsn)
                .await?;
            rows.insert(table.spec.destination.table.clone(), written);
        }
        if let SourceReader::Postgres(client, _) = &reader {
            client.batch_execute("COMMIT").await.map_err(postgres)?;
        }
        session.start().await.map_err(replication)?;
        session.wait_started().await.map_err(replication)?;
        Ok(session)
    }

    /// Read changes until the stop message, writing a batch whenever it holds enough changes at a transaction's end, and the rest at the stop; returns the end LSN of the last transaction read, confirmed to the slot, or `None` when there was none.
    async fn stream(
        &self,
        stream: &mut ReplicationClient,
        stop: Lsn,
        rows: &mut BTreeMap<String, u64>,
    ) -> Result<Option<u64>, LoaderError> {
        let mut buffer = ChangeBuffer::default();
        let mut confirmed: Option<u64> = None;
        loop {
            let Some(event) = stream.recv().await.map_err(replication)? else {
                return Err(replication("the stream ended before the load caught up"));
            };
            let step = match &event {
                ReplicationEvent::Begin { .. } => Some(StreamStep::Begin),
                ReplicationEvent::XLogData { data, .. } => Some(StreamStep::Message(data)),
                ReplicationEvent::Commit {
                    lsn,
                    end_lsn,
                    commit_time_micros,
                } => Some(StreamStep::Commit {
                    lsn: lsn.as_u64(),
                    end_lsn: end_lsn.as_u64(),
                    time: *commit_time_micros,
                }),
                _ => None,
            };
            if let Some(step) = step {
                buffer_change(&mut buffer, step).map_err(LoaderError::Invalid)?;
            }
            let at_stop = matches!(
                &event,
                ReplicationEvent::Message { prefix, lsn, .. }
                    if prefix == STOP_MESSAGE_PREFIX && *lsn >= stop
            );
            let full =
                matches!(event, ReplicationEvent::Commit { .. }) && buffer.count >= BATCH_CHANGES;
            if (full || at_stop)
                && let Some(end) = self.flush(&mut buffer, rows).await?
            {
                stream.update_applied_lsn(Lsn::from_u64(end));
                confirmed = Some(end);
            }
            if at_stop {
                return Ok(confirmed);
            }
        }
    }

    /// Write the buffered transactions, one commit per touched table, then empty the buffer; returns the end LSN the slot may now confirm.
    async fn flush(
        &self,
        buffer: &mut ChangeBuffer,
        rows: &mut BTreeMap<String, u64>,
    ) -> Result<Option<u64>, LoaderError> {
        let tables = std::mem::take(&mut buffer.tables);
        buffer.count = 0;
        let mut written = 0;
        for changes in tables.values() {
            let relation = &changes.relation;
            let Some(table) = self.tables.iter().find(|table| {
                let source = &table.spec.source_table;
                source.schema == relation.schema && source.name == relation.table
            }) else {
                continue;
            };
            let (batch, truncated) = changes_batch(
                changes,
                &table.read_columns,
                (self.cdc.apply, &table.spec.unique_key),
            )
            .map_err(LoaderError::Invalid)?;
            let mut plan = (*table).clone();
            plan.mode = WriteMode::Changes(ChangeWrite {
                apply: self.cdc.apply,
                unique_key: table.spec.unique_key.clone(),
                truncated,
            });
            let mut publisher =
                Publisher::open(plan, (self.destination, self.run.options), None).await?;
            let result = async {
                publisher.stage(batch, None).await?;
                if written == 0 {
                    crash_point("batch-before-commit");
                }
                publisher.finish(None).await
            }
            .await;
            publisher.close().await;
            let count = result?;
            written += 1;
            if written == 1 {
                crash_point("batch-after-first-table");
            }
            *rows
                .entry(table.spec.destination.table.clone())
                .or_default() += count;
        }
        crash_point("batch-before-confirm");
        Ok(buffer.end_lsn)
    }

    /// Wait until the slot shows `end` as confirmed, so the next load starts after it.
    async fn await_confirmed(&self, control: &Client, end: u64) -> Result<(), LoaderError> {
        let wanted = Lsn::from_u64(end).to_pg_string();
        let started = std::time::Instant::now();
        loop {
            let confirmed: bool = control
                .query_one(
                    "SELECT COALESCE(confirmed_flush_lsn >= $2::text::pg_lsn, false) FROM pg_replication_slots WHERE slot_name = $1",
                    &[&self.cdc.slot, &wanted],
                )
                .await
                .map_err(postgres)?
                .get(0);
            if confirmed {
                return Ok(());
            }
            if started.elapsed() > CONFIRM_WAIT {
                return Err(replication(format!(
                    "slot {} did not confirm {wanted}; the changes are written and the next load skips them",
                    self.cdc.slot
                )));
            }
            tokio::time::sleep(STATUS_INTERVAL).await;
        }
    }
}

/// The read columns with the change columns read as the snapshot's constants, the columns the table has (a merged table keeps no unchanged-value flags).
fn with_snapshot_columns(read: &[ReadColumn], cdc: &CdcSpec, snapshot_lsn: i64) -> Vec<ReadColumn> {
    let mut columns: Vec<ReadColumn> = read
        .iter()
        .filter(|column| !column.name.starts_with(CDC_PREFIX))
        .cloned()
        .collect();
    let merged = cdc.apply != ChangeApply::Changelog;
    columns.extend(
        metadata_read_columns(cdc.apply, snapshot_lsn)
            .into_iter()
            .filter(|column| !(merged && column.name == CDC_UNCHANGED_COLUMN)),
    );
    columns
}
