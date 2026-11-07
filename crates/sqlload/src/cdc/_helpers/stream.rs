//! One `postgres_cdc` source's load: its slot (created with a snapshot of every table when missing), then its changes up to where the load started, written batch by batch of whole transactions, the slot confirming a batch only once every table it touched has committed.

use std::collections::BTreeMap;
use std::time::Duration;

use pgwire_replication::client::ReplicationEvent;
use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, TlsConfig};
use tokio_postgres::Client;

use crate::adapters::contract::models::{ChangeApply, ChangeWrite, WriteMode};
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::models::Destination;
use crate::cdc::_helpers::batch::changes_batch;
use crate::cdc::_helpers::buffer::buffer_change;
use crate::cdc::_helpers::snapshot_log::SnapshotLog;
use crate::cdc::constants::{
    BATCH_CHANGES, LOST_WAL_STATUS, REPLICATION_OPTIONS, SNAPSHOT_SETTINGS, STOP_MESSAGE_PREFIX,
};
use crate::cdc::main::metadata_read_columns::metadata_read_columns;
use crate::cdc::models::{ChangeBuffer, StreamStep, TableChanges};
use crate::loader::constants::SOURCE_BATCH_ROWS;
use crate::loader::errors::LoaderError;
use crate::loader::models::{
    CdcSpec, CommitRule, LoadRun, ProjectPlan, Publisher, TableLoaded, TableOutcome, TablePlan,
    WholeReason,
};
use crate::postgres_source::constants::DEFAULT_BATCH_BYTES;
use crate::postgres_source::errors::PostgresError;
use crate::postgres_source::main::connect_postgres::connect_postgres;
use crate::postgres_source::main::copy_table::copy_table;
use crate::postgres_source::models::{CopyRequest, PostgresAddress, ReadColumn};
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
    /// The source's Postgres connection settings.
    fn address(&self) -> Result<PostgresAddress<'_>, LoaderError> {
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
        Ok(PostgresAddress {
            host,
            port: *port,
            user,
            password: password.as_ref().map(|secret| secret.expose()),
            database,
        })
    }

    /// A control connection to the source.
    async fn connect(&self) -> Result<Client, LoaderError> {
        Ok(connect_postgres(self.address()?).await?)
    }

    /// The replication session's settings, starting where the slot confirmed.
    fn replication_config(&self) -> Result<ReplicationConfig, LoaderError> {
        let address = self.address()?;
        Ok(ReplicationConfig::new(
            address.host,
            address.user,
            address.password.unwrap_or_default(),
            address.database,
            self.cdc.slot.clone(),
            self.cdc.publication.clone(),
        )
        .with_port(address.port)
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
        let mut missing: Vec<String> = Vec::new();
        for table in &self.tables {
            let source = &table.spec.source_table;
            let wanted = (source.schema.clone(), source.name.clone());
            if !published.contains(&wanted) {
                missing.push(format!(
                    "{}.{}",
                    quoted(&source.schema),
                    quoted(&source.name)
                ));
            }
        }
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

    /// Why the slot cannot resume, if it cannot: SQLLoad did not create it; the load leaves out a table the slot captured (its changes would be confirmed unwritten); some table has no snapshot from it (a snapshot stopped part-way, or the table joined since); or a table's columns changed since its snapshot.
    async fn resume_problem(&self, wal_status: &str) -> Result<Option<String>, LoaderError> {
        let slot = &self.cdc.slot;
        if wal_status == LOST_WAL_STATUS {
            return Ok(Some(format!(
                "slot {slot} lost WAL it had not confirmed, so changes are missing; run `sqlload load --resnapshot` to drop and recreate it and copy every table again"
            )));
        }
        let Some((_, snapshotted)) = self.log()?.current().await? else {
            return Ok(Some(format!(
                "slot {slot} exists but SQLLoad has no record of creating it, so it cannot tell which changes the tables already hold; drop it, or run `sqlload load --resnapshot` to drop and recreate it and copy every table again"
            )));
        };
        let loading: Vec<&str> = self
            .tables
            .iter()
            .map(|table| table.spec.destination.table.as_str())
            .collect();
        let left_out: Vec<&str> = snapshotted
            .iter()
            .map(|(name, _)| name.as_str())
            .filter(|name| !loading.contains(name))
            .collect();
        if !left_out.is_empty() {
            return Ok(Some(format!(
                "this load leaves out {} of source {}, whose tables share slot {slot}; load the whole source (no --select or --exclude of its tables, and none blocked), or their changes would be skipped",
                left_out.join(", "),
                self.name
            )));
        }
        let reshaped: Vec<&str> = self
            .tables
            .iter()
            .filter(|table| {
                snapshotted.iter().any(|(name, columns)| {
                    *name == table.spec.destination.table
                        && *columns != data_columns(&table.read_columns)
                })
            })
            .map(|table| table.spec.destination.table.as_str())
            .collect();
        if !reshaped.is_empty() {
            return Ok(Some(format!(
                "the columns of {} changed since their snapshot from slot {slot}; schema changes come in a later release, so run `sqlload load --resnapshot` to copy every table again",
                reshaped.join(", ")
            )));
        }
        let unfinished: Vec<&str> = loading
            .into_iter()
            .filter(|name| !snapshotted.iter().any(|(done, _)| done == name))
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
        let control = self.connect().await?;
        self.ensure_publication(&control).await?;
        self.log()?.ensure().await?;
        let mut slot = self.slot_state(&control).await?;
        if self.run.options.resnapshot && slot.is_some() {
            control
                .execute("SELECT pg_drop_replication_slot($1)", &[&self.cdc.slot])
                .await
                .map_err(postgres)?;
            slot = None;
        }
        let (mut stream, mut rows) = match slot {
            Some((_, wal_status)) => {
                if let Some(problem) = self.resume_problem(&wal_status).await? {
                    return Err(LoaderError::Invalid(problem));
                }
                let session = ReplicationClient::connect(self.replication_config()?)
                    .await
                    .map_err(replication)?;
                (session, BTreeMap::new())
            }
            None => {
                let recreated = self.run.options.resnapshot;
                if !recreated && self.log()?.current().await?.is_some() {
                    return Err(LoaderError::Invalid(format!(
                        "slot {} is gone (dropped, or never kept after a failover), so the changes since the last load are lost; run `sqlload load --resnapshot` to recreate it and copy every table again",
                        self.cdc.slot
                    )));
                }
                self.snapshot().await?
            }
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
        let streamed = self.stream(&mut stream, stop).await;
        let confirmed = match streamed {
            Ok((end, written)) => {
                for (table, count) in written {
                    *rows.entry(table).or_default() += count;
                }
                match end {
                    Some(end) => self.await_confirmed(&control, end).await,
                    None => Ok(()),
                }
            }
            Err(error) => Err(error),
        };
        stream.stop();
        let _ = stream.shutdown().await;
        confirmed.map(|()| rows)
    }

    /// Create the slot with an exported snapshot and copy every table under it, each table's rows getting the snapshot's LSN; returns the session, which streams on from the snapshot.
    async fn snapshot(&self) -> Result<(ReplicationClient, BTreeMap<String, u64>), LoaderError> {
        let mut rows: BTreeMap<String, u64> = BTreeMap::new();
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
        log.record("", snapshot_lsn, "").await?;
        let client = self.connect().await?;
        client
            .batch_execute(&format!(
                "{SNAPSHOT_SETTINGS}; BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT '{}'",
                snapshot_name.replace('\'', "''")
            ))
            .await
            .map_err(postgres)?;
        for (index, table) in self.tables.iter().enumerate() {
            if index == 1 {
                crash_point("snapshot-after-first-table");
            }
            let mut plan = (*table).clone();
            plan.mode = WriteMode::Replace;
            plan.commits = CommitRule::Whole(WholeReason::Chosen);
            plan.read_columns =
                with_snapshot_columns(&plan.read_columns, self.cdc, snapshot_lsn - 1);
            let written = self.copy_table(&client, plan).await?;
            log.record(
                &table.spec.destination.table,
                snapshot_lsn,
                &data_columns(&table.read_columns),
            )
            .await?;
            rows.insert(table.spec.destination.table.clone(), written);
        }
        client.batch_execute("COMMIT").await.map_err(postgres)?;
        session.start().await.map_err(replication)?;
        session.wait_started().await.map_err(replication)?;
        Ok((session, rows))
    }

    /// Copy one table under the open snapshot into a build table swapped in when done; returns its rows.
    async fn copy_table(&self, client: &Client, plan: TablePlan) -> Result<u64, LoaderError> {
        let request = CopyRequest {
            table: &plan.spec.source_table,
            columns: plan.read_columns.clone(),
            filter: None,
            order_by: None,
            batch_rows: SOURCE_BATCH_ROWS,
            batch_bytes: DEFAULT_BATCH_BYTES,
            helpers: &[],
        };
        let mut publisher =
            Publisher::open(plan.clone(), (self.destination, self.run.options), None).await?;
        let copied = async {
            copy_table(client, request, async |batch| {
                publisher
                    .stage(batch, None)
                    .await
                    .map_err(|error| error.to_string())
            })
            .await?;
            publisher.finish(None).await
        }
        .await;
        publisher.close().await;
        copied
    }

    /// Read changes until the stop message, writing a batch whenever it holds enough changes at a transaction's end, and the rest at the stop; returns the end LSN of the last transaction read, confirmed to the slot, or `None` when there was none.
    async fn stream(
        &self,
        stream: &mut ReplicationClient,
        stop: Lsn,
    ) -> Result<(Option<u64>, BTreeMap<String, u64>), LoaderError> {
        let mut rows: BTreeMap<String, u64> = BTreeMap::new();
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
                buffer = buffer_change(buffer, step).map_err(LoaderError::Invalid)?;
            }
            let at_stop = matches!(
                &event,
                ReplicationEvent::Message { prefix, lsn, .. }
                    if prefix == STOP_MESSAGE_PREFIX && *lsn >= stop
            );
            let full =
                matches!(event, ReplicationEvent::Commit { .. }) && buffer.count >= BATCH_CHANGES;
            if full || at_stop {
                let tables = std::mem::take(&mut buffer.tables);
                buffer.count = 0;
                for (table, count) in self.flush(tables).await? {
                    *rows.entry(table).or_default() += count;
                }
                if let Some(end) = buffer.end_lsn {
                    stream.update_applied_lsn(Lsn::from_u64(end));
                    confirmed = Some(end);
                }
            }
            if at_stop {
                stream.update_applied_lsn(stop);
                return Ok((Some(stop.as_u64()).max(confirmed), rows));
            }
        }
    }

    /// Write a batch's transactions, one commit per touched table; returns the rows each table wrote. The slot may confirm the batch once this returns.
    async fn flush(
        &self,
        tables: BTreeMap<u32, TableChanges>,
    ) -> Result<BTreeMap<String, u64>, LoaderError> {
        let mut rows: BTreeMap<String, u64> = BTreeMap::new();
        let mut written = 0;
        for changes in tables.values() {
            let relation = &changes.relation;
            let Some(table) = self.tables.iter().find(|table| {
                let source = &table.spec.source_table;
                source.schema == relation.schema && source.name == relation.table
            }) else {
                return Err(LoaderError::Invalid(format!(
                    "publication {} sends changes of {}.{}, which source {} does not load; its changes would be skipped, so add the table to include or drop it from the publication",
                    self.cdc.publication, relation.schema, relation.table, self.name
                )));
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
            rows.insert(table.spec.destination.table.clone(), count);
        }
        crash_point("batch-before-confirm");
        Ok(rows)
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

/// The plan's `postgres_cdc` tables by source: the unblocked ones, and the names of the blocked ones.
pub(crate) fn captured_sources(plan: &ProjectPlan) -> BTreeMap<&str, CapturedTables<'_>> {
    let mut sources: BTreeMap<&str, CapturedTables<'_>> = BTreeMap::new();
    for table in plan.tables.iter().filter(|table| table.spec.cdc.is_some()) {
        let entry = sources.entry(table.spec.source_name.as_str()).or_default();
        match table.schema.blocked {
            None => entry.0.push(table),
            Some(_) => entry.1.push(table.spec.destination.table.clone()),
        }
    }
    sources
}

/// One source's unblocked tables, and its blocked tables' names.
pub(crate) type CapturedTables<'a> = (Vec<&'a TablePlan>, Vec<String>);

/// Load source `name` with `tables`: rows written per destination table name, or why it failed.
pub(crate) async fn load_source(
    run: &LoadRun<'_>,
    name: &str,
    tables: &[&TablePlan],
) -> Result<BTreeMap<String, u64>, String> {
    let first = tables.first().map(|table| &table.spec);
    let (Some(source), Some(cdc), Some(destination)) = (
        run.project.sources.get(name),
        first.and_then(|spec| spec.cdc.as_ref()),
        first.and_then(|spec| run.destinations.get(&spec.target)),
    ) else {
        return Err(format!(
            "source {name} is not a connected postgres_cdc source"
        ));
    };
    let unit = CdcSource {
        run,
        name,
        source,
        cdc,
        tables: tables.to_vec(),
        destination,
    };
    unit.load().await.map_err(|error| error.to_string())
}

/// Each table's outcome from its source's load.
pub(crate) fn source_outcomes(
    tables: &[&TablePlan],
    loaded: &Result<BTreeMap<String, u64>, String>,
    elapsed: Duration,
) -> Vec<TableOutcome> {
    let mut outcomes: Vec<TableOutcome> = Vec::with_capacity(tables.len());
    for table in tables {
        let spec = &table.spec;
        let rows = loaded.as_ref().map(|rows| {
            rows.get(&spec.destination.table)
                .copied()
                .unwrap_or_default()
        });
        outcomes.push(TableOutcome {
            table: sql_adapter(spec.warehouse).render_display_name(&spec.destination),
            loaded: rows
                .map(|rows| TableLoaded {
                    rows,
                    ..TableLoaded::default()
                })
                .map_err(Clone::clone),
            elapsed,
        });
    }
    outcomes
}

/// The data columns of a table's read, as `,a,b,`, the shape its snapshot was copied with.
fn data_columns(read: &[ReadColumn]) -> String {
    let mut columns = String::from(",");
    for column in read
        .iter()
        .filter(|column| !column.name.starts_with(CDC_PREFIX))
    {
        columns.push_str(&column.name);
        columns.push(',');
    }
    columns
}
