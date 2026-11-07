//! One `postgres_cdc` source's load: its slot (created with a snapshot of every table when missing), then its changes up to where the load started, written batch by batch of whole transactions, the slot confirming a batch only once every table it touched has committed.

use std::collections::BTreeMap;
use std::time::Duration;

use pgwire_replication::client::ReplicationEvent;
use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, TlsConfig};
use tokio_postgres::Client;
use tokio_util::sync::CancellationToken;

use crate::adapters::contract::models::{ChangeApply, ChangeWrite, WriteMode};
use crate::adapters::dispatch::main::dialect::files_destination::files_destination;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::models::Destination;
use crate::adapters::files::main::cursor_maximum::cursor_maximum;
use crate::adapters::files::main::newest_load_files::newest_load_files;
use crate::adapters::files::main::read_table_state::read_table_state;
use crate::adapters::files::main::table_path::table_path;
use crate::adapters::files::models::TablePathRequest;
use crate::cdc::_helpers::batch::{change_parts, refuse_renamed, rows_batch, unplanned_columns};
use crate::cdc::_helpers::buffer::buffer_change;
use crate::cdc::_helpers::reshape::reshape_problem;
use crate::cdc::_helpers::snapshot_log::{SnapshotLog, snapshot_folder};
use crate::cdc::_helpers::spill::SpillFile;
use crate::cdc::constants::{
    BACKFILL_SLOT_SUFFIX, BATCH_CHANGES, COLUMN_LISTS_VERSION, GENERATED_COLUMNS_VERSION,
    LOST_WAL_STATUS, MICROS_PER_SECOND, POSTGRES_EPOCH_MICROS, REPLAN_ADVICE, REPLICATION_OPTIONS,
    SLOT_NAME_BYTES, SLOTS_FULL_ERROR, SNAPSHOT_SETTINGS, SPILL_BYTES, STOP_MESSAGE_PREFIX,
};
use crate::cdc::main::metadata_read_columns::metadata_read_columns;
use crate::cdc::models::{
    CatalogColumn, ChangeBuffer, FollowStep, PlannedShape, SnapshotRecord, StreamStep, Streamed,
    TableChanges,
};
use crate::column_types::models::ColumnType;
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::models::StoredFile;
use crate::files_source::main::read_footers::read_footers;
use crate::loader::constants::SOURCE_BATCH_ROWS;
use crate::loader::errors::LoaderError;
use crate::loader::main::apply_project::apply_project;
use crate::loader::main::plan_project::plan_project;
use crate::loader::models::{
    CdcSpec, CommitRule, LoadRun, PlanOptions, ProjectPlan, Publisher, TableLoaded, TableOutcome,
    TablePlan, WholeReason,
};
use crate::postgres_source::constants::DEFAULT_BATCH_BYTES;
use crate::postgres_source::errors::PostgresError;
use crate::postgres_source::main::connect_postgres::connect_postgres;
use crate::postgres_source::main::copy_table::copy_table;
use crate::postgres_source::models::{CopyRequest, PostgresAddress, ReadColumn, SourceTable};
use crate::project::constants::DEFAULT_DESTINATION_PATH;
use crate::project::models::{ConnectionConfig, SourceConfig, TargetConfig};
use crate::schema_plan::constants::{CDC_LSN_COLUMN, CDC_PREFIX, CDC_UNCHANGED_COLUMN};

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
    /// Under `load --follow`: keep streaming, writing a batch every `batch_interval`, until this is cancelled.
    pub(crate) follow: Option<&'a CancellationToken>,
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

    /// The source's snapshot records: beside its first table in a warehouse, in the records folder of its path template in a files target (see [`snapshot_folder`]).
    fn log(&self) -> Result<SnapshotLog<'_>, LoaderError> {
        let first = self
            .tables
            .first()
            .map(|table| &table.spec)
            .ok_or_else(|| {
                LoaderError::Invalid(format!("source {} selects no tables", self.name))
            })?;
        let folder = match files_destination(self.destination) {
            Some(files) => {
                let target = self.run.project.targets.get(&first.target);
                let template = self
                    .source
                    .destination_path
                    .as_deref()
                    .or_else(|| target.map(TargetConfig::path_template))
                    .unwrap_or(DEFAULT_DESTINATION_PATH);
                let request = TablePathRequest {
                    template: &snapshot_folder(template, self.name, &self.cdc.slot),
                    connection: &files.connection,
                    store: &files.store,
                    source: self.name,
                    schema: "",
                    table: "",
                };
                Some(table_path(&request).map_err(LoaderError::Invalid)?.folder)
            }
            None => None,
        };
        Ok(SnapshotLog {
            folder,
            destination: self.destination,
            warehouse: first.warehouse,
            beside: &first.destination,
            source: self.name,
            slot: &self.cdc.slot,
        })
    }

    /// Create the publication for the selected tables when it is missing; when it exists, it must publish no other table, and every selected table unless it is SQLLoad's own (`sqlload_<source>`): its missing tables are returned, for the load to add before copying them.
    async fn ensure_publication(&self, control: &Client) -> Result<Vec<&TablePlan>, LoaderError> {
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
            return Ok(Vec::new());
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
        match (missing.is_empty() || made_here, extra.is_empty()) {
            (true, true) => Ok(self
                .tables
                .iter()
                .zip(&names)
                .filter(|(_, name)| missing.contains(name))
                .map(|(table, _)| *table)
                .collect()),
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

    /// Why the slot cannot resume, if it cannot: SQLLoad did not create it; the load leaves out a table the slot captured (its changes would be confirmed unwritten); a recorded table left the publication (its changes since were not captured); a table to copy alone still holds rows from before the slot was recreated (the copy would replace them unasked); or a table's columns changed since its snapshot in a way the stream does not carry (see [`reshape_problem`]). When it can, the tables whose columns changed in ways it carries are recorded with their new columns first, and the tables without a snapshot from it (joined since, or a snapshot stopped part-way) are returned, to be copied alone before streaming.
    async fn resume_problem(
        &self,
        control: &Client,
        (wal_status, missing): (&str, &[&TablePlan]),
    ) -> Result<Result<Vec<&TablePlan>, String>, LoaderError> {
        let slot = &self.cdc.slot;
        if wal_status == LOST_WAL_STATUS {
            return Ok(Err(format!(
                "slot {slot} lost WAL it had not confirmed, so changes are missing; run `sqlload load --resnapshot` to drop and recreate it and copy every table again"
            )));
        }
        let Some((newest, snapshotted, earlier)) = self.log()?.current().await? else {
            return Ok(Err(format!(
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
            return Ok(Err(format!(
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
            return Ok(Err(problems.join("; ")));
        }
        let recorded: Vec<&str> = snapshotted
            .iter()
            .map(|(_, record)| record.table.as_str())
            .collect();
        let dropped: Vec<String> = missing
            .iter()
            .filter(|table| recorded.contains(&table.spec.destination.table.as_str()))
            .map(|table| {
                let source = &table.spec.source_table;
                format!("{}.{}", quoted(&source.schema), quoted(&source.name))
            })
            .collect();
        if !dropped.is_empty() {
            return Ok(Err(format!(
                "publication {} no longer publishes {}, so their changes since were not captured; run `sqlload load --resnapshot` to copy every table again",
                self.cdc.publication,
                dropped.join(", ")
            )));
        }
        let unrecorded: Vec<&TablePlan> = self
            .tables
            .iter()
            .filter(|table| !recorded.contains(&table.spec.destination.table.as_str()))
            .copied()
            .collect();
        let log = self.log()?;
        let mut held: Vec<&str> = Vec::new();
        for table in &unrecorded {
            let destination = &table.spec.destination;
            let earlier_rows = table.exists
                && earlier.contains(&destination.table)
                && log.holds_before(destination, newest).await?;
            held.extend(earlier_rows.then_some(destination.table.as_str()));
        }
        if !held.is_empty() {
            let remove = match log.folder {
                Some(_) => "delete their folders",
                None => "drop the tables from the destination",
            };
            return Ok(Err(format!(
                "rows captured before slot {slot} was recreated are still in {}; copying those tables alone would replace them without asking, so run `sqlload load --resnapshot` to copy every table again (it asks first), or {remove} to copy them alone",
                held.join(", ")
            )));
        }
        for (copied_at, record) in &rewrites {
            log.record(record, *copied_at).await?;
        }
        Ok(Ok(unrecorded))
    }

    /// Load the source: rows written per destination table name, and, when following, why it stopped to re-plan, if it did.
    pub(crate) async fn load(
        &self,
    ) -> Result<(BTreeMap<String, u64>, Option<String>), LoaderError> {
        let control = self.connect().await?;
        let missing = self.ensure_publication(&control).await?;
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
                let unrecorded = self
                    .resume_problem(&control, (&wal_status, &missing))
                    .await?
                    .map_err(LoaderError::Invalid)?;
                let copied = match unrecorded.is_empty() {
                    true => BTreeMap::new(),
                    false => self.backfill(&control, &unrecorded, &missing).await?,
                };
                crash_point("backfill-before-stream");
                let session = ReplicationClient::connect(self.replication_config()?)
                    .await
                    .map_err(replication)?;
                (session, copied)
            }
            None => {
                let recreated = self.run.options.resnapshot;
                if !recreated && self.log()?.current().await?.is_some() {
                    return Err(LoaderError::Invalid(format!(
                        "slot {} is gone (dropped, or never kept after a failover), so the changes since the last load are lost; run `sqlload load --resnapshot` to recreate it and copy every table again",
                        self.cdc.slot
                    )));
                }
                self.add_to_publication(&control, &missing).await?;
                self.snapshot().await?
            }
        };
        let stop = match self.follow {
            Some(_) => None,
            None => Some(self.emit_stop(&control).await?),
        };
        let streamed = self.stream(&control, &mut stream, stop).await;
        let confirmed = match streamed {
            Ok(streamed) => {
                for (table, count) in streamed.rows {
                    *rows.entry(table).or_default() += count;
                }
                match streamed.end {
                    Some(end) => self.await_confirmed(&control, end).await,
                    None => Ok(()),
                }
                .map(|()| streamed.replan)
            }
            Err(error) => Err(error),
        };
        stream.stop();
        let _ = stream.shutdown().await;
        confirmed.map(|replan| (rows, replan))
    }

    /// Mark the WAL position a catch-up load stops at with a logical message; its LSN.
    async fn emit_stop(&self, control: &Client) -> Result<Lsn, LoaderError> {
        let stop: String = control
            .query_one(
                "SELECT pg_logical_emit_message(false, $1, '')::text",
                &[&STOP_MESSAGE_PREFIX],
            )
            .await
            .map_err(postgres)?
            .get(0);
        Lsn::parse(&stop).map_err(replication)
    }

    /// Add `tables` to SQLLoad's own publication, so the slot captures their changes from here on.
    async fn add_to_publication(
        &self,
        control: &Client,
        tables: &[&TablePlan],
    ) -> Result<(), LoaderError> {
        if tables.is_empty() {
            return Ok(());
        }
        let names: Vec<String> = tables
            .iter()
            .map(|table| {
                let source = &table.spec.source_table;
                format!("{}.{}", quoted(&source.schema), quoted(&source.name))
            })
            .collect();
        control
            .batch_execute(&format!(
                "ALTER PUBLICATION {} ADD TABLE {}",
                quoted(&self.cdc.publication),
                names.join(", ")
            ))
            .await
            .map_err(|error| {
                LoaderError::Invalid(format!(
                    "cannot add {} to publication {} ({}); Postgres lets only a table's owner publish it, so add them as the owner (ALTER PUBLICATION {} ADD TABLE {})",
                    names.join(", "),
                    self.cdc.publication,
                    PostgresError::from(error),
                    quoted(&self.cdc.publication),
                    names.join(", ")
                ))
            })
    }

    /// Copy `tables`, which have no snapshot from the slot (joined since, or a snapshot stopped part-way), alone: `missing` ones join SQLLoad's publication first, then a temporary slot exports a snapshot they are copied under, and each is recorded at that slot's consistent point, so the stream drops their changes from before it. The slot and every other table are left as they are; returns the rows each table wrote.
    async fn backfill(
        &self,
        control: &Client,
        tables: &[&TablePlan],
        missing: &[&TablePlan],
    ) -> Result<BTreeMap<String, u64>, LoaderError> {
        self.add_to_publication(control, missing).await?;
        crash_point("backfill-after-publication");
        let mut session = ReplicationClient::connect_gated(self.replication_config()?)
            .await
            .map_err(replication)?;
        let name: String = self
            .cdc
            .slot
            .chars()
            .take(SLOT_NAME_BYTES - BACKFILL_SLOT_SUFFIX.len())
            .chain(BACKFILL_SLOT_SUFFIX.chars())
            .collect();
        let created = session
            .simple_query(&format!(
                "CREATE_REPLICATION_SLOT {} TEMPORARY LOGICAL pgoutput (SNAPSHOT 'export')",
                quoted(&name)
            ))
            .await
            .map_err(|error| {
                let hint = match error.to_string().contains(SLOTS_FULL_ERROR) {
                    true => " (it needs a free slot under max_replication_slots)",
                    false => "",
                };
                replication(format!(
                    "cannot make temporary slot {name} to copy {} alone{hint}: {error}",
                    tables
                        .iter()
                        .map(|table| table.spec.destination.table.as_str())
                        .collect::<Vec<&str>>()
                        .join(", ")
                ))
            })?;
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
        let copy_lsn = i64::try_from(consistent.as_u64()).map_err(replication)?;
        let client = self.connect().await?;
        client
            .batch_execute(&format!(
                "{SNAPSHOT_SETTINGS}; BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT '{}'",
                snapshot_name.replace('\'', "''")
            ))
            .await
            .map_err(postgres)?;
        drop(session);
        tracing::info!(
            tables = tables.len(),
            "copying tables that joined the source alone, under their own snapshot"
        );
        let rows = self
            .copy_snapshot(&client, tables, (copy_lsn, "backfill"))
            .await?;
        client.batch_execute("COMMIT").await.map_err(postgres)?;
        Ok(rows)
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
        rows.extend(
            self.copy_snapshot(&client, &self.tables, (snapshot_lsn, "snapshot"))
                .await?,
        );
        client.batch_execute("COMMIT").await.map_err(postgres)?;
        session.start().await.map_err(replication)?;
        session.wait_started().await.map_err(replication)?;
        Ok((session, rows))
    }

    /// Copy `tables` under the snapshot open on `client`, taken at `snapshot_lsn`, recording each once it is in: its rows get `snapshot_lsn - 1`, so its changes from `snapshot_lsn` on apply after them. `stage` names the crash points (`<stage>-after-first-table`, `<stage>-before-record`); returns the rows each table wrote.
    async fn copy_snapshot(
        &self,
        client: &Client,
        tables: &[&TablePlan],
        (snapshot_lsn, stage): (i64, &str),
    ) -> Result<BTreeMap<String, u64>, LoaderError> {
        let log = self.log()?;
        let mut rows: BTreeMap<String, u64> = BTreeMap::new();
        for (index, table) in tables.iter().enumerate() {
            if index == 1 {
                crash_point(&format!("{stage}-after-first-table"));
            }
            let mut plan = (*table).clone();
            plan.mode = WriteMode::Replace;
            plan.commits = CommitRule::Whole(WholeReason::Chosen);
            plan.read_columns =
                with_snapshot_columns(&plan.read_columns, self.cdc, snapshot_lsn - 1);
            let written = self.copy_table(client, plan).await?;
            crash_point(&format!("{stage}-before-record"));
            let catalog = catalog_columns(client, &table.spec.source_table).await?;
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
        Ok(rows)
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

    /// Read changes until the stop message at `stop`, writing a batch whenever it holds enough changes at a transaction's end, and the rest at the stop. With no `stop` (following), keep reading until cancelled, also writing what is committed every `batch_interval` and, while nothing is buffered, confirming the server's position from its keepalives; a table that gains a column ends it for a re-plan, with nothing of that batch written. Returns what it confirmed, wrote and, following, why it stopped to re-plan.
    async fn stream(
        &self,
        control: &Client,
        stream: &mut ReplicationClient,
        stop: Option<Lsn>,
    ) -> Result<Streamed, LoaderError> {
        let mut streamed = Streamed::default();
        let mut buffer = ChangeBuffer {
            spill_bytes: spill_bytes(),
            floors: self.copy_points().await?,
            ..ChangeBuffer::default()
        };
        let interval = self.cdc.batch_interval;
        let mut deadline = tokio::time::Instant::now() + interval;
        let mut open = false;
        let mut server_end: u64 = 0;
        let mut newest_commit: Option<i64> = None;
        loop {
            let next = match (stop, self.follow) {
                (None, Some(follow)) => tokio::select! {
                    biased;
                    () = follow.cancelled() => FollowStep::Stop,
                    () = tokio::time::sleep_until(deadline) => FollowStep::Tick,
                    event = stream.recv() => FollowStep::Event(event.map_err(replication)?),
                },
                _ => FollowStep::Event(stream.recv().await.map_err(replication)?),
            };
            let event = match next {
                FollowStep::Event(Some(event)) => event,
                FollowStep::Event(None) => {
                    return Err(replication(match stop {
                        Some(_) => "the stream ended before the load caught up",
                        None => "the stream ended while following",
                    }));
                }
                FollowStep::Tick | FollowStep::Stop => {
                    let cut = self
                        .cut(control, stream, (buffer, server_end, newest_commit))
                        .await;
                    let (next_buffer, written) = match replanned(cut, true)? {
                        Ok(cut) => cut,
                        Err(reason) => {
                            streamed.replan = Some(reason);
                            return Ok(streamed);
                        }
                    };
                    (buffer, streamed) = (next_buffer, merged(streamed, written));
                    deadline = tokio::time::Instant::now() + interval;
                    if matches!(next, FollowStep::Stop) {
                        return Ok(streamed);
                    }
                    continue;
                }
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
            open = match &event {
                ReplicationEvent::Begin { .. } => true,
                ReplicationEvent::Commit { .. } => false,
                _ => open,
            };
            if let ReplicationEvent::Commit {
                commit_time_micros, ..
            } = &event
            {
                newest_commit = Some(*commit_time_micros);
            }
            if let ReplicationEvent::KeepAlive { wal_end, .. }
            | ReplicationEvent::XLogData { wal_end, .. } = &event
            {
                server_end = server_end.max(wal_end.as_u64());
            }
            if let Some(step) = step {
                buffer = buffer_change(buffer, step).map_err(LoaderError::Invalid)?;
            }
            let idle = !open && buffer.tables.is_empty() && buffer.spilled.is_empty();
            if let (None, true, ReplicationEvent::KeepAlive { wal_end, .. }) = (stop, idle, &event)
            {
                let quiet = wal_end.as_u64().max(buffer.end_lsn.unwrap_or_default());
                stream.update_applied_lsn(Lsn::from_u64(quiet));
                streamed.end = Some(quiet).max(streamed.end);
            }
            let at_stop = matches!(
                (&event, stop),
                (ReplicationEvent::Message { prefix, lsn, .. }, Some(stop))
                    if prefix == STOP_MESSAGE_PREFIX && *lsn >= stop
            );
            let full =
                matches!(event, ReplicationEvent::Commit { .. }) && buffer.count >= BATCH_CHANGES;
            if full || at_stop {
                let cut = self
                    .cut(control, stream, (buffer, server_end, newest_commit))
                    .await;
                let (next_buffer, written) = match replanned(cut, stop.is_none())? {
                    Ok(cut) => cut,
                    Err(reason) => {
                        streamed.replan = Some(reason);
                        return Ok(streamed);
                    }
                };
                (buffer, streamed) = (next_buffer, merged(streamed, written));
                deadline = tokio::time::Instant::now() + interval;
            }
            if let (true, Some(stop)) = (at_stop, stop) {
                stream.update_applied_lsn(stop);
                streamed.end = Some(stop.as_u64()).max(streamed.end);
                return Ok(streamed);
            }
        }
    }

    /// Write the buffer's committed transactions as one batch and confirm them to the slot: the emptied buffer, and the rows each table wrote with the end LSN confirmed; a buffer holding none only confirms. Following, a written batch is logged with how far it is behind the server's position `server_end` and the newest commit's time.
    async fn cut(
        &self,
        control: &Client,
        stream: &mut ReplicationClient,
        (mut buffer, server_end, newest_commit): (ChangeBuffer, u64, Option<i64>),
    ) -> Result<(ChangeBuffer, Streamed), LoaderError> {
        let mut written = Streamed::default();
        let tables = std::mem::take(&mut buffer.tables);
        let spilled = std::mem::take(&mut buffer.spilled);
        let changes = buffer.count;
        buffer.count = 0;
        buffer.bytes = buffer.open.iter().map(Vec::len).sum();
        let touched = tables.len()
            + spilled
                .keys()
                .filter(|oid| !tables.contains_key(oid))
                .count();
        if touched > 0 {
            written.rows = self.flush(control, tables, spilled).await?;
        }
        let Some(end) = buffer.end_lsn else {
            return Ok((buffer, written));
        };
        stream.update_applied_lsn(Lsn::from_u64(end));
        written.end = Some(end);
        if let (Some(_), true) = (self.follow, touched > 0) {
            let behind_secs = newest_commit
                .map(|micros| (postgres_now_micros() - micros).max(0) / MICROS_PER_SECOND);
            tracing::info!(
                source = self.name,
                changes,
                tables = touched,
                lsn = %Lsn::from_u64(end),
                behind_bytes = server_end.saturating_sub(end),
                behind_secs,
                "batch written"
            );
        }
        Ok((buffer, written))
    }

    /// Each loaded table's floor by its source `(schema, table)`: the stream drops its changes committed before it. That is its copy point, from its snapshot record; in a files target, past its newest committed change too, which files cannot skip on a replay as a warehouse's changelog insert does.
    async fn copy_points(&self) -> Result<BTreeMap<(String, String), i64>, LoaderError> {
        let mut points: BTreeMap<(String, String), i64> = BTreeMap::new();
        let Some((_, records, _)) = self.log()?.current().await? else {
            return Ok(points);
        };
        for (lsn, record) in records {
            let table = self
                .tables
                .iter()
                .find(|table| table.spec.destination.table == record.table);
            if let Some(table) = table {
                let source = &table.spec.source_table;
                let floor = newest_change_lsn(table)
                    .await?
                    .map_or(lsn, |newest| lsn.max(newest + 1));
                points.insert((source.schema.clone(), source.name.clone()), floor);
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
            false => Err(LoaderError::Replan(format!(
                "{}.{} gained {} {}{REPLAN_ADVICE}, and the next load plans for {}",
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

/// A batch's outcome as the stream goes on: what it wrote, or why to re-plan when following and a table gained a column (stopping first at the `follow-before-replan` crash point); else its error.
fn replanned<T>(
    cut: Result<T, LoaderError>,
    following: bool,
) -> Result<Result<T, String>, LoaderError> {
    match (cut, following) {
        (Ok(cut), _) => Ok(Ok(cut)),
        (Err(LoaderError::Replan(reason)), true) => {
            crash_point("follow-before-replan");
            Ok(Err(reason))
        }
        (Err(error), _) => Err(error),
    }
}

/// `streamed` with one batch's rows added and its confirmed end LSN, when later.
fn merged(mut streamed: Streamed, batch: Streamed) -> Streamed {
    for (table, count) in batch.rows {
        *streamed.rows.entry(table).or_default() += count;
    }
    streamed.end = streamed.end.max(batch.end);
    streamed
}

/// Now, in microseconds since 2000-01-01 UTC, the epoch Postgres times commits from.
fn postgres_now_micros() -> i64 {
    let unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_micros()).unwrap_or(i64::MAX))
        .unwrap_or_default();
    unix.saturating_sub(POSTGRES_EPOCH_MICROS)
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

/// Load source `name` with `tables`, following until `follow` is cancelled when given: rows written per destination table name and, following, why it stopped to re-plan, or why it failed.
pub(crate) async fn load_source(
    run: &LoadRun<'_>,
    name: &str,
    (tables, follow): (&[&TablePlan], Option<&CancellationToken>),
) -> Result<(BTreeMap<String, u64>, Option<String>), String> {
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
        follow,
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

/// One follow of `plan`'s source until `stop`, or until a table gains a column: its tables, the rows each wrote and why to re-plan, or why it failed. A source with a blocked table does not follow, since its slot would skip that table's changes; the error then names every blocked table with its reasons, and the plan's problems.
pub(crate) async fn follow_round<'a>(
    run: &LoadRun<'_>,
    plan: &'a ProjectPlan,
    stop: &CancellationToken,
) -> (
    Vec<&'a TablePlan>,
    Result<(BTreeMap<String, u64>, Option<String>), String>,
) {
    let Some((name, (tables, blocked))) = captured_sources(plan).into_iter().next() else {
        let problems: Vec<&str> = std::iter::once("load --follow needs a postgres_cdc source")
            .chain(plan.problems.iter().map(String::as_str))
            .collect();
        return (Vec::new(), Err(problems.join("; ")));
    };
    if !blocked.is_empty() {
        let all: Vec<&TablePlan> = plan
            .tables
            .iter()
            .filter(|table| table.spec.cdc.is_some() && table.spec.source_name == name)
            .collect();
        let mut reasons: Vec<String> = Vec::new();
        for table in &all {
            if let Some(blocked) = &table.schema.blocked {
                reasons.push(format!(
                    "{} ({})",
                    table.spec.destination.table,
                    blocked.reasons.join("; ")
                ));
            }
        }
        reasons.extend(plan.problems.iter().cloned());
        let error = format!(
            "source {name} does not follow while {} {} blocked: its tables share one slot, so following the rest would skip the blocked tables' changes; {}",
            blocked.join(", "),
            match blocked.len() {
                1 => "is",
                _ => "are",
            },
            reasons.join("; ")
        );
        return (all, Err(error));
    }
    let followed = load_source(run, name, (&tables, Some(stop))).await;
    (tables, followed)
}

/// Plan the project again with `options` because of `reason`, and apply it: the new plan and the lines its apply printed.
pub(crate) async fn replan(
    run: &LoadRun<'_>,
    options: &PlanOptions,
    reason: &str,
) -> Result<(ProjectPlan, Vec<String>), String> {
    tracing::info!(%reason, "planning again");
    let plan = plan_project(run.project, run.destinations, options)
        .await
        .map_err(|error| error.to_string())?;
    let lines = apply_project(&plan, run.destinations)
        .await
        .map_err(|error| error.to_string())?;
    for line in &lines {
        tracing::info!(%line, "applied");
    }
    Ok((plan, lines))
}

/// What a follow round's outcome means next, given the rows so far, the last re-plan's reason and whether following was asked to stop: the rows with the round's added, and `Ok(None)` to finish, `Ok(Some(reason))` to plan again, or the error that stops it. The same gain again with no batch written since stops it, since planning again did not take it in; a stop as a table gains a column finishes, leaving that batch for the next load.
pub(crate) fn follow_next(
    followed: Result<(BTreeMap<String, u64>, Option<String>), String>,
    (rows, last_reason, stopped): (BTreeMap<String, u64>, Option<&str>, bool),
) -> (BTreeMap<String, u64>, Result<Option<String>, String>) {
    let (written, replan) = match followed {
        Ok(followed) => followed,
        Err(error) => return (rows, Err(error)),
    };
    let wrote = written.values().sum::<u64>() > 0;
    let rows = added(rows, written);
    let Some(reason) = replan else {
        return (rows, Ok(None));
    };
    let gained = reason.split(REPLAN_ADVICE).next().unwrap_or_default();
    if stopped {
        tracing::warn!(%gained, "stopped before writing the last batch; the next load plans for it and writes it");
        return (rows, Ok(None));
    }
    match (wrote, last_reason == Some(reason.as_str())) {
        (false, true) => (
            rows,
            Err(format!(
                "{gained} again after it was planned for, with no batch written since, so following stopped; run `sqlload plan` to see why the plan leaves it out"
            )),
        ),
        _ => (rows, Ok(Some(reason))),
    }
}

/// `rows` with each table's `written` rows added.
fn added(mut rows: BTreeMap<String, u64>, written: BTreeMap<String, u64>) -> BTreeMap<String, u64> {
    for (table, count) in written {
        *rows.entry(table).or_default() += count;
    }
    rows
}

/// Each of `tables` failing with `error`; one failure for the whole follow when there are none, so the load still fails.
pub(crate) fn failed_outcomes(
    tables: &[&TablePlan],
    error: String,
    elapsed: Duration,
) -> Vec<TableOutcome> {
    let outcomes = source_outcomes(tables, &Err(error.clone()), elapsed);
    match outcomes.is_empty() {
        true => vec![TableOutcome {
            table: "load --follow".to_owned(),
            loaded: Err(error),
            elapsed,
        }],
        false => outcomes,
    }
}

/// `outcomes` with the re-plans' `applied` lines on the first, so the tally prints them.
pub(crate) fn with_changes(
    mut outcomes: Vec<TableOutcome>,
    applied: Vec<String>,
) -> Vec<TableOutcome> {
    if let Some(Ok(loaded)) = outcomes.first_mut().map(|outcome| &mut outcome.loaded) {
        loaded.changes.extend(applied);
    }
    outcomes
}

/// The newest `_sqlload_lsn` in a files table's committed files, from the statistics of its newest commit's files (a table's commits each hold newer changes than the last); `None` for a warehouse table or a folder without files. An unfinished load's files do not count.
async fn newest_change_lsn(table: &TablePlan) -> Result<Option<i64>, LoaderError> {
    let Some(output) = &table.output else {
        return Ok(None);
    };
    let folder = open_folder(&output.path.folder)?;
    let state = read_table_state(&folder, &output.path, None).await?;
    let newest: Vec<StoredFile> = state
        .listed
        .partitions
        .last()
        .map(|partition| newest_load_files(&partition.files))
        .unwrap_or_default();
    if newest.len() > output.max_footer_reads {
        return Err(LoaderError::Invalid(format!(
            "the newest load in {} wrote {} files, more footers than max_footer_reads = {} allows reading for its newest change; raise max_footer_reads on the target",
            output.path.folder,
            newest.len(),
            output.max_footer_reads
        )));
    }
    let footers = read_footers(&folder, &newest).await?;
    let maximum =
        cursor_maximum(&footers, CDC_LSN_COLUMN, &ColumnType::Int).map_err(LoaderError::Invalid)?;
    maximum
        .map(|text| {
            text.parse::<i64>().map_err(|_| {
                LoaderError::Invalid(format!(
                    "{}: unreadable {CDC_LSN_COLUMN} maximum {text:?}",
                    output.path.folder
                ))
            })
        })
        .transpose()
}
