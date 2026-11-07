//! One `postgres_cdc` source's load: its slot (created with a snapshot of every table when missing), then its changes up to where the load started, written batch by batch of whole transactions, the slot confirming a batch only once every table it touched has committed.

use std::collections::BTreeMap;
use std::time::Duration;

use pgwire_replication::client::ReplicationEvent;
use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, TlsConfig};
use tokio_postgres::Client;

use crate::adapters::contract::models::{ChangeApply, ChangeWrite, WriteMode};
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::models::Destination;
use crate::cdc::_helpers::batch::{change_parts, refuse_renamed, rows_batch, unplanned_columns};
use crate::cdc::_helpers::buffer::buffer_change;
use crate::cdc::_helpers::reshape::reshape_problem;
use crate::cdc::_helpers::snapshot_log::SnapshotLog;
use crate::cdc::_helpers::spill::SpillFile;
use crate::cdc::constants::{
    BATCH_CHANGES, COLUMN_LISTS_VERSION, GENERATED_COLUMNS_VERSION, LOST_WAL_STATUS,
    REPLICATION_OPTIONS, SNAPSHOT_SETTINGS, SPILL_BYTES, STOP_MESSAGE_PREFIX,
};
use crate::cdc::main::metadata_read_columns::metadata_read_columns;
use crate::cdc::models::{
    CatalogColumn, ChangeBuffer, PlannedShape, SnapshotRecord, StreamStep, TableChanges,
};
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
use crate::postgres_source::models::{CopyRequest, PostgresAddress, ReadColumn, SourceTable};
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

/// How many bytes of changes a load keeps in memory before spilling them to local disk; tests set it lower.
fn spill_bytes() -> usize {
    #[cfg(feature = "container-tests")]
    if let Ok(bytes) = std::env::var("SQLLOAD_TEST_SPILL_BYTES")
        .unwrap_or_default()
        .parse::<usize>()
    {
        return bytes;
    }
    SPILL_BYTES
}

fn postgres(error: tokio_postgres::Error) -> LoaderError {
    LoaderError::Postgres(PostgresError::from(error))
}

fn replication(error: impl std::fmt::Display) -> LoaderError {
    LoaderError::Invalid(format!("logical replication: {error}"))
}

/// `names`, comma-separated.
fn list(names: &[&String]) -> String {
    names
        .iter()
        .map(|name| name.as_str())
        .collect::<Vec<&str>>()
        .join(", ")
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
        let names: Vec<String> = self
            .tables
            .iter()
            .map(|table| {
                let source = &table.spec.source_table;
                format!("{}.{}", quoted(&source.schema), quoted(&source.name))
            })
            .collect();
        self.refuse_unpublishable(control, &names).await?;
        let exists = control
            .query_opt(
                "SELECT 1 FROM pg_publication WHERE pubname = $1",
                &[publication],
            )
            .await
            .map_err(postgres)?
            .is_some();
        let made_here = *publication == format!("sqlload_{}", self.name.to_lowercase());
        let (name, tables) = (quoted(publication), names.join(", "));
        let statement = match (exists, made_here && self.run.options.resnapshot) {
            (false, _) => Some(format!("CREATE PUBLICATION {name} FOR TABLE {tables}")),
            (true, true) => Some(format!("ALTER PUBLICATION {name} SET TABLE {tables}")),
            (true, false) => None,
        };
        if let Some(statement) = statement {
            control.batch_execute(&statement).await.map_err(postgres)?;
            return Ok(());
        }
        let published: Vec<String> = control
            .query(
                "SELECT '\"' || replace(schemaname, '\"', '\"\"') || '\".\"' || replace(tablename, '\"', '\"\"') || '\"' FROM pg_publication_tables WHERE pubname = $1",
                &[publication],
            )
            .await
            .map_err(postgres)?
            .iter()
            .map(|row| row.get(0))
            .collect();
        let missing: Vec<&String> = names
            .iter()
            .filter(|name| !published.contains(name))
            .collect();
        let extra: Vec<&String> = published
            .iter()
            .filter(|name| !names.contains(name))
            .collect();
        match (missing.is_empty(), extra.is_empty()) {
            (true, true) => Ok(()),
            (false, _) => Err(LoaderError::Invalid(format!(
                "publication {publication} does not publish {}; add them with ALTER PUBLICATION {} ADD TABLE {}",
                list(&missing),
                quoted(publication),
                list(&missing)
            ))),
            (true, false) => Err(LoaderError::Invalid(format!(
                "publication {publication} also publishes {}, which source {} does not load, so the slot would stop at their first change; publish only the source's tables (ALTER PUBLICATION {} DROP TABLE {}), then run `sqlload load --resnapshot`",
                list(&extra),
                self.name,
                quoted(publication),
                list(&extra)
            ))),
        }
    }

    /// Refuse a table that loads a column the stream never sends: a generated column (before Postgres 18, which can publish them), or one left out of the publication's column list (Postgres 15 and later). Its changes would leave the column out, so it would keep its snapshot value forever.
    async fn refuse_unstreamed(&self, control: &Client) -> Result<(), LoaderError> {
        let version: i32 = control
            .query_one("SELECT current_setting('server_version_num')::int", &[])
            .await
            .map_err(postgres)?
            .get(0);
        let mut problems: Vec<String> = Vec::new();
        for table in &self.tables {
            let source = &table.spec.source_table;
            let read: Vec<String> = table
                .read_columns
                .iter()
                .filter(|column| !column.name.starts_with(CDC_PREFIX))
                .map(|column| column.name.clone())
                .collect();
            let mut unstreamed: Vec<String> = control
                .query(
                    "SELECT a.attname::text FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid = a.attrelid JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated <> '' AND a.attname = ANY($3) AND $4::bool ORDER BY a.attnum",
                    &[
                        &source.schema,
                        &source.name,
                        &read,
                        &(version < GENERATED_COLUMNS_VERSION),
                    ],
                )
                .await
                .map_err(postgres)?
                .iter()
                .map(|row| row.get(0))
                .collect();
            if version >= COLUMN_LISTS_VERSION {
                let listed: Option<Vec<String>> = control
                    .query_opt(
                        "SELECT attnames::text[] FROM pg_catalog.pg_publication_tables WHERE pubname = $1 AND schemaname = $2 AND tablename = $3",
                        &[&self.cdc.publication, &source.schema, &source.name],
                    )
                    .await
                    .map_err(postgres)?
                    .and_then(|row| row.get(0));
                if let Some(listed) = listed {
                    let unlisted: Vec<String> = read
                        .iter()
                        .filter(|name| !listed.contains(name) && !unstreamed.contains(name))
                        .cloned()
                        .collect();
                    unstreamed.extend(unlisted);
                }
            }
            if !unstreamed.is_empty() {
                problems.push(format!(
                    "{}.{} loads {}, which logical replication does not send (a generated column, or one outside publication {}'s column list), so its changes would never reach the destination; add it to exclude_columns",
                    source.schema,
                    source.name,
                    unstreamed.join(", "),
                    self.cdc.publication
                ));
            }
        }
        match problems.is_empty() {
            true => Ok(()),
            false => Err(LoaderError::Invalid(problems.join("; "))),
        }
    }

    /// Refuse tables whose replica identity is `NOTHING`: once published, Postgres refuses their updates and deletes from the application itself.
    async fn refuse_unpublishable(
        &self,
        control: &Client,
        names: &[String],
    ) -> Result<(), LoaderError> {
        let nothing: Vec<String> = control
            .query(
                "SELECT name FROM (SELECT '\"' || replace(n.nspname, '\"', '\"\"') || '\".\"' || replace(c.relname, '\"', '\"\"') || '\"' AS name, c.relreplident FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace) AS tables WHERE relreplident = 'n' AND name = ANY($1)",
                &[&names],
            )
            .await
            .map_err(postgres)?
            .iter()
            .map(|row| row.get(0))
            .collect();
        match nothing.is_empty() {
            true => Ok(()),
            false => Err(LoaderError::Invalid(format!(
                "{} {} REPLICA IDENTITY NOTHING, so publishing it would make Postgres refuse its updates and deletes; set REPLICA IDENTITY DEFAULT, or exclude it",
                nothing.join(", "),
                match nothing.len() {
                    1 => "has",
                    _ => "have",
                }
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

    /// Why the slot cannot resume, if it cannot: SQLLoad did not create it; the load leaves out a table the slot captured (its changes would be confirmed unwritten); some table has no snapshot from it (a snapshot stopped part-way, or the table joined since); or a table's columns changed since its snapshot in a way the stream does not carry (see [`reshape_problem`]). When it can, the tables whose columns changed in ways it carries are recorded with their new columns first.
    async fn resume_problem(
        &self,
        control: &Client,
        wal_status: &str,
    ) -> Result<Option<String>, LoaderError> {
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
            .map(|(_, record)| record.table.as_str())
            .filter(|name| !loading.contains(name))
            .collect();
        if !left_out.is_empty() {
            return Ok(Some(format!(
                "this load leaves out {} of source {}, whose tables share slot {slot}; load the whole source (no --select or --exclude of its tables, and none blocked), or their changes would be skipped",
                left_out.join(", "),
                self.name
            )));
        }
        let mut rewrites: Vec<(i64, SnapshotRecord)> = Vec::new();
        let mut problems: Vec<String> = Vec::new();
        for table in &self.tables {
            let name = table.spec.destination.table.as_str();
            let Some((copied_at, recorded)) =
                snapshotted.iter().find(|(_, record)| record.table == name)
            else {
                continue;
            };
            let columns = data_columns(&table.read_columns);
            let key = key_list(&table.spec.unique_key);
            let known: Vec<String> = table
                .spec
                .source_table
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect();
            let planned = PlannedShape {
                columns: &columns,
                key: &key,
                merge: self.cdc.apply != ChangeApply::Changelog,
                known: &known,
            };
            let catalog = catalog_columns(control, &table.spec.source_table).await?;
            match reshape_problem(recorded, planned, &catalog) {
                Ok(rewrite) => rewrites.extend(rewrite.map(|record| (*copied_at, record))),
                Err(problem) => problems.push(problem),
            }
        }
        if !problems.is_empty() {
            return Ok(Some(problems.join("; ")));
        }
        let recorded: Vec<&str> = snapshotted
            .iter()
            .map(|(_, record)| record.table.as_str())
            .collect();
        let unfinished: Vec<&str> = loading
            .into_iter()
            .filter(|name| !recorded.contains(name))
            .collect();
        if !unfinished.is_empty() {
            return Ok(Some(format!(
                "{} {} no snapshot from slot {slot} (a snapshot stopped part-way, or the table joined since); run `sqlload load --resnapshot` to drop and recreate the slot and copy every table again",
                unfinished.join(", "),
                match unfinished.len() {
                    1 => "has",
                    _ => "have",
                }
            )));
        }
        let log = self.log()?;
        for (copied_at, record) in &rewrites {
            log.record(record, *copied_at).await?;
        }
        Ok(None)
    }

    /// Load the source: rows written per destination table name.
    pub(crate) async fn load(&self) -> Result<BTreeMap<String, u64>, LoaderError> {
        let control = self.connect().await?;
        self.ensure_publication(&control).await?;
        self.refuse_unstreamed(&control).await?;
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
                if let Some(problem) = self.resume_problem(&control, &wal_status).await? {
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
        let streamed = self.stream(&control, &mut stream, stop).await;
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
        log.record(
            &SnapshotRecord {
                table: String::new(),
                columns: String::new(),
                max_attnum: None,
                key: None,
                left: String::new(),
            },
            snapshot_lsn,
        )
        .await?;
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
            let catalog = catalog_columns(&client, &table.spec.source_table).await?;
            let record = SnapshotRecord {
                table: table.spec.destination.table.clone(),
                columns: data_columns(&table.read_columns),
                max_attnum: catalog.iter().map(|column| column.attnum).max(),
                key: Some(key_list(&table.spec.unique_key)),
                left: key_list(&[]),
            };
            log.record(&record, snapshot_lsn).await?;
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
        control: &Client,
        stream: &mut ReplicationClient,
        stop: Lsn,
    ) -> Result<(Option<u64>, BTreeMap<String, u64>), LoaderError> {
        let mut rows: BTreeMap<String, u64> = BTreeMap::new();
        let mut buffer = ChangeBuffer {
            spill_bytes: spill_bytes(),
            floors: self.copy_points().await?,
            ..ChangeBuffer::default()
        };
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
                let spilled = std::mem::take(&mut buffer.spilled);
                buffer.count = 0;
                buffer.bytes = 0;
                for (table, count) in self.flush(control, tables, spilled).await? {
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

    /// Each loaded table's copy point by its source `(schema, table)`, from its snapshot record.
    async fn copy_points(&self) -> Result<BTreeMap<(String, String), i64>, LoaderError> {
        let mut points: BTreeMap<(String, String), i64> = BTreeMap::new();
        let Some((_, records)) = self.log()?.current().await? else {
            return Ok(points);
        };
        for (lsn, record) in records {
            let table = self
                .tables
                .iter()
                .find(|table| table.spec.destination.table == record.table);
            if let Some(table) = table {
                let source = &table.spec.source_table;
                points.insert((source.schema.clone(), source.name.clone()), lsn);
            }
        }
        Ok(points)
    }

    /// Write a batch's transactions, one commit per touched table; returns the rows each table wrote. The slot may confirm the batch once this returns. A batch with a column added since the load was planned writes nothing, so the next load, which plans for it, streams it again.
    async fn flush(
        &self,
        control: &Client,
        tables: BTreeMap<u32, TableChanges>,
        mut spilled: BTreeMap<u32, SpillFile>,
    ) -> Result<BTreeMap<String, u64>, LoaderError> {
        let mut planned: Vec<(u32, &TablePlan, Vec<String>)> = Vec::with_capacity(tables.len());
        for (oid, changes) in &tables {
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
            let gone = self.gone_columns(control, changes, table).await?;
            planned.push((*oid, table, gone));
        }
        let mut rows: BTreeMap<String, u64> = BTreeMap::new();
        let mut written = 0;
        for (oid, table, gone) in planned {
            let Some(changes) = tables.get(&oid) else {
                continue;
            };
            let relation = &changes.relation;
            let mut spill = spilled.remove(&oid);
            refuse_renamed(changes, spill.as_mut(), (&table.read_columns, &gone))
                .map_err(LoaderError::Invalid)?;
            let (mut parts, truncated) = change_parts(
                changes,
                (spill, spill_bytes()),
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
                while let Some(part) = parts.next_changes().map_err(LoaderError::Invalid)? {
                    let batch = rows_batch(&part, relation, &table.read_columns)
                        .map_err(LoaderError::Invalid)?;
                    publisher.stage(batch, None).await?;
                }
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

    /// The columns on `table`'s stream that its plan lacks and the source no longer has (dropped, or renamed, while their changes were unloaded); refuses the batch when the source still has one (added or renamed during this load), or has a column the plan lacks while one is gone (a rename during this load: the next load plans the new name and then sees the rename). Gone columns alone are left out, or a column added and dropped between loads would stop every load.
    async fn gone_columns(
        &self,
        control: &Client,
        changes: &TableChanges,
        table: &TablePlan,
    ) -> Result<Vec<String>, LoaderError> {
        let unplanned = unplanned_columns(&changes.relation, &table.spec.source_table);
        if unplanned.is_empty() {
            return Ok(Vec::new());
        }
        let live: Vec<String> = control
            .query(
                "SELECT attname::text FROM pg_catalog.pg_attribute WHERE attrelid = $1 AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
                &[&changes.relation.oid],
            )
            .await
            .map_err(postgres)?
            .iter()
            .map(|row| row.get(0))
            .collect();
        let (present, gone): (Vec<String>, Vec<String>) =
            unplanned.into_iter().partition(|name| live.contains(name));
        let known: std::collections::BTreeSet<&str> = table
            .spec
            .source_table
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        let new_live: Vec<&String> = live
            .iter()
            .filter(|name| !known.contains(name.as_str()))
            .collect();
        let gained: Vec<String> = match gone.is_empty() {
            true => present,
            false => new_live.into_iter().cloned().collect(),
        };
        match gained.is_empty() {
            true => Ok(gone),
            false => Err(LoaderError::Invalid(format!(
                "{}.{} gained {} {} during this load; run it again, and the next load plans for {}",
                changes.relation.schema,
                changes.relation.table,
                match gained.len() {
                    1 => "column",
                    _ => "columns",
                },
                gained.join(", "),
                match gained.len() {
                    1 => "it",
                    _ => "them",
                }
            ))),
        }
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
    key_list(
        &read
            .iter()
            .filter(|column| !column.name.starts_with(CDC_PREFIX))
            .map(|column| column.name.clone())
            .collect::<Vec<String>>(),
    )
}

/// `names` as `,a,b,`.
fn key_list(names: &[String]) -> String {
    let mut list = String::from(",");
    for name in names {
        list.push_str(name);
        list.push(',');
    }
    list
}

/// `table`'s columns in the catalog as `client` sees it, dropped ones included.
async fn catalog_columns(
    client: &Client,
    table: &SourceTable,
) -> Result<Vec<CatalogColumn>, LoaderError> {
    Ok(client
        .query(
            "SELECT a.attname::text, a.attnum, a.attisdropped, a.atthasdef OR a.atthasmissing OR a.attidentity <> '' OR a.attgenerated <> '' FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid = a.attrelid JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0",
            &[&table.schema, &table.name],
        )
        .await
        .map_err(postgres)?
        .iter()
        .map(|row| CatalogColumn {
            name: row.get(0),
            attnum: row.get(1),
            dropped: row.get(2),
            has_default: row.get(3),
        })
        .collect())
}
