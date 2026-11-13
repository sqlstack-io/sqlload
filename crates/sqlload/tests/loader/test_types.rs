pub(crate) struct PostgresColumnTestCase {
    pub(crate) description: &'static str,
    pub(crate) column: &'static str,
    /// The natural portable type the catalog maps the column to.
    pub(crate) expected_type: &'static str,
    /// Each row's value as Arrow displays it; empty for NULL.
    pub(crate) expected_values: &'static [&'static str],
}

pub(crate) struct SplitReadTestCase {
    pub(crate) description: &'static str,
    /// Extra connections offered to the read.
    pub(crate) helpers: usize,
    /// The source filter, combined with each range.
    pub(crate) filter: Option<&'static str>,
    /// Rows read and the sum of their ids.
    pub(crate) expected_rows: i64,
    pub(crate) expected_sum: i64,
    /// `COPY` statements running when the first batch arrives.
    pub(crate) expected_copies: i64,
}

pub(crate) struct TargetTlsTestCase {
    pub(crate) description: &'static str,
    /// TLS settings for the connection, `FILES` standing for the test certificates' folder.
    pub(crate) settings: &'static str,
    /// What `debug` says protects the target's connection.
    pub(crate) expected_debug: &'static str,
    /// The loaded table's codes.
    pub(crate) expected_result: &'static str,
}

pub(crate) struct PostgresTargetTestCase {
    pub(crate) description: &'static str,
    /// SQL run in the test database before this load.
    pub(crate) source_change: &'static str,
    pub(crate) options: sqlload::loader::models::PlanOptions,
    /// `ok`, or a part of the plan's or the load's error.
    pub(crate) expected_load: &'static str,
    /// A query on the target and its expected single text result.
    pub(crate) check_query: &'static str,
    pub(crate) expected_result: &'static str,
}

pub(crate) struct LoadRunTestCase {
    pub(crate) description: &'static str,
    /// SQL run in Postgres before this load.
    pub(crate) source_change: &'static str,
    /// A query on the DuckDB destination and its expected single text result.
    pub(crate) check_query: &'static str,
    pub(crate) expected_result: &'static str,
}

/// One `plan` + `apply` + `load` whose plan text, table shape and rows are pinned exactly.
pub(crate) struct PinnedRunTestCase {
    pub(crate) description: &'static str,
    /// SQL run in Postgres before this load.
    pub(crate) source_change: &'static str,
    /// The rendered plan, line by line.
    pub(crate) expected_plan: &'static str,
    /// `DESCRIBE` of the loaded table: each column's name, type and nullability.
    pub(crate) expected_describe: &'static str,
    /// Every row, as DuckDB prints it, without the load's own id and time.
    pub(crate) expected_rows: &'static str,
}

/// One tenant's load into a table two tenants share.
pub(crate) struct TenantStepTestCase {
    pub(crate) description: &'static str,
    /// SQL run in Postgres before this load.
    pub(crate) source_change: &'static str,
    /// The source loaded, one tenant's.
    pub(crate) select: &'static str,
    /// The other tenant, whose rows, load id and load time must not change.
    pub(crate) untouched_tenant: &'static str,
    /// A source whose connection is down for this load, or empty.
    pub(crate) unreadable: &'static str,
    /// The cursor line the plan shows for the loaded tenant.
    pub(crate) expected_cursor: &'static str,
    /// Text a plan warning must contain, or empty.
    pub(crate) expected_warning: &'static str,
    /// Every row after the load, as `tenant_id:id:label`.
    pub(crate) expected_values: &'static str,
}

/// A table loaded once, then again after its settings change.
pub(crate) struct SettingsChangeTestCase {
    pub(crate) description: &'static str,
    /// Settings after the `app` source's for the first load, starting with its `include` line.
    pub(crate) before: &'static str,
    /// The settings for the second load.
    pub(crate) after: &'static str,
    /// The cursor line the second plan shows.
    pub(crate) expected_cursor: &'static str,
    /// A query on the DuckDB destination after the second load, and its expected single text result.
    pub(crate) check_query: &'static str,
    pub(crate) expected_result: &'static str,
}

/// One project with constant columns planned and loaded once on a fresh Postgres.
pub(crate) struct ConstantLoadTestCase {
    pub(crate) description: &'static str,
    /// Settings after the `app` source's, starting with its `include` line.
    pub(crate) settings: &'static str,
    /// Text the plan's or the load's error must contain; empty when the load must succeed.
    pub(crate) expected_error: &'static str,
    /// A query on the DuckDB destination and its expected single text result.
    pub(crate) check_query: &'static str,
    pub(crate) expected_result: &'static str,
}

/// One `plan` + `apply` + `load` within a scenario.
pub(crate) struct ScenarioRun {
    /// SQL run in Postgres first.
    pub(crate) source_change: &'static str,
    /// SQL run in the DuckDB destination first (a hand edit).
    pub(crate) destination_change: &'static str,
    /// Text the plan or load error must contain; empty when the run must succeed.
    pub(crate) expected_error: &'static str,
    /// Text the rendered plan of a run that succeeds must contain, such as a warning.
    pub(crate) expected_plan: &'static str,
    /// `--reload`.
    pub(crate) reload: bool,
    /// `--start-cursor-*` and `--end-cursor-*`.
    pub(crate) window: (Option<&'static str>, Option<&'static str>),
    /// TOML added after the case's settings for this run only.
    pub(crate) settings: &'static str,
    /// `--select`: the tables this run plans; empty plans every table.
    pub(crate) select: &'static [&'static str],
}

pub(crate) struct ScenarioTestCase {
    pub(crate) description: &'static str,
    /// SQL creating the source tables in a fresh `public` schema.
    pub(crate) setup: &'static str,
    /// Project TOML appended after `[sources.app]`.
    pub(crate) settings: &'static str,
    pub(crate) runs: &'static [ScenarioRun],
    /// A query on DuckDB after the last run, and its expected single text result.
    pub(crate) check_query: &'static str,
    pub(crate) expected_result: &'static str,
}

pub(crate) struct FileStoreTestCase {
    pub(crate) description: &'static str,
    /// The store under test, given the S3 endpoint and a temporary directory.
    pub(crate) store: fn(&str, &std::path::Path) -> sqlload::store_paths::models::StoreKind,
    /// The data files listed after writing, in order.
    pub(crate) expected_keys: &'static [&'static str],
}

/// One Parquet file a files scenario writes: every row gets `score`; `extra` adds a text column.
pub(crate) struct ParquetFile {
    pub(crate) key: &'static str,
    pub(crate) ids: &'static [i64],
    pub(crate) score: f64,
    pub(crate) extra: Option<&'static str>,
}

/// One load of a files scenario, after writing `files`.
pub(crate) struct FilesRun {
    pub(crate) files: &'static [ParquetFile],
    /// Text the plan or load error must contain; empty when the run must succeed.
    pub(crate) expected_error: &'static str,
}

/// One JSON or CSV file a files scenario writes, compressed by `encode`.
pub(crate) struct TextFile {
    pub(crate) key: &'static str,
    pub(crate) contents: &'static str,
    pub(crate) encode: fn(&str) -> Result<bytes::Bytes, String>,
}

/// One load of a text files scenario, after writing `files`.
pub(crate) struct TextFilesRun {
    pub(crate) files: &'static [TextFile],
    /// Text the plan or load error must contain; empty when the run must succeed.
    pub(crate) expected_error: &'static str,
    /// Text the rendered plan must contain (empty matches any).
    pub(crate) expected_plan: &'static str,
}

pub(crate) struct TextFilesScenarioTestCase {
    pub(crate) description: &'static str,
    /// The store and its connection TOML, given the S3 endpoint and a fresh temporary directory.
    pub(crate) store:
        fn(&str, &std::path::Path) -> (sqlload::store_paths::models::StoreKind, String),
    /// Source settings appended after `[sources.scores]`, starting with its format.
    pub(crate) settings: &'static str,
    pub(crate) runs: &'static [TextFilesRun],
    pub(crate) check_query: &'static str,
    pub(crate) expected_result: &'static str,
}

pub(crate) struct FilesScenarioTestCase {
    pub(crate) description: &'static str,
    /// The store and its connection TOML, given the S3 endpoint and a fresh temporary directory.
    pub(crate) store:
        fn(&str, &std::path::Path) -> (sqlload::store_paths::models::StoreKind, String),
    /// Source settings appended after `[sources.scores]`.
    pub(crate) settings: &'static str,
    pub(crate) runs: &'static [FilesRun],
    pub(crate) check_query: &'static str,
    pub(crate) expected_result: &'static str,
}

/// One `plan` + `apply` + `load` of a files target scenario.
pub(crate) struct FilesTargetRun {
    /// SQL run in Postgres first.
    pub(crate) source_change: &'static str,
    /// Text the plan or load error must contain; empty when the run must succeed.
    pub(crate) expected_error: &'static str,
    /// Text the rendered plan must contain; empty to skip the check.
    pub(crate) expected_plan: &'static str,
    /// `--reload`.
    pub(crate) reload: bool,
    /// Replaces the scenario's source settings for this run and those after it.
    pub(crate) settings: Option<&'static str>,
    /// Tables picked on the command line; empty picks all.
    pub(crate) select: &'static [&'static str],
    /// Files put in the table folder first (key, contents), such as a crashed load's marker and files.
    pub(crate) leftovers: &'static [(&'static str, &'static str)],
}

pub(crate) struct FilesTargetScenarioTestCase {
    pub(crate) description: &'static str,
    /// The store and its connection TOML, given the S3 endpoint and a fresh temporary directory.
    pub(crate) store:
        fn(&str, &std::path::Path) -> (sqlload::store_paths::models::StoreKind, String),
    /// SQL creating the source table `public.orders` in a fresh `public` schema.
    pub(crate) setup: &'static str,
    /// TOML added under `[targets.lake]`.
    pub(crate) target_settings: &'static str,
    /// Project TOML appended after `[sources.app]`; `PREFIX` is replaced by the scenario's own folder.
    pub(crate) settings: &'static str,
    pub(crate) runs: &'static [FilesTargetRun],
    /// The column shown beside each row's id.
    pub(crate) check_column: &'static str,
    /// Per partition folder: the runs that wrote its files, then `id:value` per row; then mixed column types and pending markers, if any.
    pub(crate) expected_files: &'static str,
}

/// One row read back from a written file.
pub(crate) struct WrittenRow {
    /// The partition folder, `-` without partitions.
    pub(crate) folder: String,
    pub(crate) id: String,
    pub(crate) value: String,
    pub(crate) load_id: String,
    /// The Arrow type of the checked column in the row's file, empty when the file lacks it.
    pub(crate) data_type: String,
}

/// One partition folder of the written files: the runs whose loads wrote it, and its rows as `(id, "id:value")`.
#[derive(Default)]
pub(crate) struct WrittenPartition {
    pub(crate) runs: std::collections::BTreeSet<usize>,
    pub(crate) rows: Vec<(i64, String)>,
}

/// A files target scenario whose partition values are then read back by the files source.
pub(crate) struct RoundTripTestCase {
    pub(crate) description: &'static str,
    pub(crate) scenario: FilesTargetScenarioTestCase,
    pub(crate) partition_column: &'static str,
    /// The partition values the files source reads, sorted.
    pub(crate) expected_values: &'static [&'static str],
}

pub(crate) struct DebugProjectTestCase {
    pub(crate) description: &'static str,
    /// TOML added to the Postgres + DuckDB project (`duckdb_project`), given the Postgres port and a directory.
    pub(crate) extra: fn(u16, &std::path::Path) -> String,
    pub(crate) connect: bool,
    /// Each expected line's start, in order.
    pub(crate) expected_lines: &'static [&'static str],
    pub(crate) expected_ok: bool,
}

/// What a files scenario runs: each run's files, already encoded, and the error it expects.
pub(crate) struct FilesScenario<'a> {
    pub(crate) description: &'a str,
    pub(crate) store:
        fn(&str, &std::path::Path) -> (sqlload::store_paths::models::StoreKind, String),
    pub(crate) settings: &'a str,
    pub(crate) runs: Vec<WrittenRun<'a>>,
    pub(crate) check_query: &'a str,
}

/// One run of a [`FilesScenario`].
pub(crate) struct WrittenRun<'a> {
    /// The files written first, as (key, bytes).
    pub(crate) files: Vec<(&'a str, bytes::Bytes)>,
    /// Text the plan or load error must contain; empty when the run must succeed.
    pub(crate) expected_error: &'a str,
    /// Text the rendered plan must contain (empty matches any).
    pub(crate) expected_plan: &'a str,
}

/// The source files a run adds, as `(relative key, bytes)`.
pub(crate) type SourceFiles = fn() -> Result<Vec<(String, bytes::Bytes)>, String>;

/// A files source loaded into a files target once per run.
pub(crate) struct FilesToFilesTestCase {
    pub(crate) description: &'static str,
    /// The store and its connection TOML, given the S3 endpoint and a fresh temporary directory.
    pub(crate) store:
        fn(&str, &std::path::Path) -> (sqlload::store_paths::models::StoreKind, String),
    /// TOML added to `[sources.orders]`; `PREFIX` is replaced by the case's own folder.
    pub(crate) settings: &'static str,
    /// The source files added before each run.
    pub(crate) runs: &'static [SourceFiles],
    /// After each run, the rows the target holds and how many loads wrote them, as `rows|loads`.
    pub(crate) expected_held: &'static [&'static str],
}

/// Postgres into a files target, then those files through a files source into DuckDB, loaded twice.
pub(crate) struct CsvTargetTestCase {
    pub(crate) description: &'static str,
    pub(crate) scenario: FilesTargetScenarioTestCase,
    /// The data files left in the table folder, sorted, with each `part-<load>-<n>` name as `part-*`.
    pub(crate) expected_keys: &'static [&'static str],
}

pub(crate) struct MetadataRoundTripTestCase {
    pub(crate) description: &'static str,
    /// SQL creating `public.orders`.
    pub(crate) setup: &'static str,
    /// SQL run before the second load.
    pub(crate) change: &'static str,
    /// Text the files source's plan shows on the second load.
    pub(crate) expected_plan: &'static str,
    /// The DuckDB table's columns, rows, distinct load ids, and rows not stamped by the second load.
    pub(crate) expected_check: &'static str,
}

/// One run of a batched scenario: a source change, the load, and what the destination holds after it.
pub(crate) struct BatchedRun {
    /// SQL run in Postgres first.
    pub(crate) source_change: &'static str,
    /// SQL run in the DuckDB destination first (a hand edit, such as a unique index that makes a later batch fail).
    pub(crate) destination_change: &'static str,
    /// `--end-cursor-*`.
    pub(crate) end_cursor: Option<&'static str>,
    /// A query on DuckDB after the run, giving one text result.
    pub(crate) check_query: &'static str,
    /// Add the plan line holding this text to the run's outcome, from that text on; empty adds none.
    pub(crate) show: &'static str,
}

/// Loads that commit in batches, run in order on fresh source tables and a fresh DuckDB.
pub(crate) struct BatchedScenarioTestCase {
    pub(crate) description: &'static str,
    pub(crate) setup: &'static str,
    /// Project TOML appended after `[sources.app]`.
    pub(crate) settings: &'static str,
    /// The hidden `--batch-bytes`, so a few rows make several commits.
    pub(crate) batch_bytes: Option<usize>,
    pub(crate) concurrency: Option<usize>,
    pub(crate) runs: &'static [BatchedRun],
    /// Each run's `<load error, or ok> => <check query result>`.
    pub(crate) expected_outcomes: &'static [&'static str],
}

/// A `delete_insert` files source with one partition of many files, which alone have an `extra` column.
pub(crate) struct LargePartitionTestCase {
    pub(crate) description: &'static str,
    pub(crate) files: usize,
    /// Text the second run's plan must contain.
    pub(crate) expected_plan: &'static str,
    /// Rows, rows with `extra`, distinct source files and the distinct ids, after both runs.
    pub(crate) expected_result: &'static str,
}

/// Where a routes scenario's source files are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RouteFiles {
    /// In a directory, so Snowflake cannot read them.
    Directory,
    /// In the fake's bucket, under the stage URL.
    Bucket,
}

/// Makes a routes scenario's source files: each key and its bytes.
pub(crate) type RouteFileSet = fn() -> Result<Vec<(String, bytes::Bytes)>, String>;

/// One mask, planned for every fixture format under every files strategy.
pub(crate) struct MaskMatrixTestCase {
    pub(crate) description: &'static str,
    /// TOML ending `[sources.orders]`; `COLUMN` stands for the fixture's text column.
    pub(crate) mask: &'static str,
    /// Text every plan's route must contain.
    pub(crate) expected_plan: &'static [&'static str],
    /// Text no plan's route may contain.
    pub(crate) expected_unplanned: &'static [&'static str],
}

/// One load of a files source into the fake Snowflake.
pub(crate) struct RouteTestCase {
    pub(crate) description: &'static str,
    /// The connection's `stage`, with `FAKE` for the fake's URL; empty leaves it unset.
    pub(crate) stage: &'static str,
    /// TOML added to `[sources.orders]`.
    pub(crate) settings: &'static str,
    pub(crate) files: RouteFiles,
    pub(crate) file_set: RouteFileSet,
    /// What the fake refuses or holds first.
    pub(crate) prepare: fn(&sqlload_testkit::fake_snowflake::models::FakeSnowflake),
    /// Plan and load with `--reload`.
    pub(crate) reload: bool,
    /// Text the plan or load error must contain; empty when the run must succeed.
    pub(crate) expected_error: &'static str,
    /// Text the rendered plan must contain.
    pub(crate) expected_plan: &'static [&'static str],
    /// Text the rendered plan must not contain.
    pub(crate) expected_unplanned: &'static [&'static str],
    /// Text some statement sent must contain.
    pub(crate) expected_sent: &'static [&'static str],
    /// Text no statement sent may contain.
    pub(crate) expected_unsent: &'static [&'static str],
    /// How many `PUT`s fetched upload credentials, probes included; `None` skips the check.
    pub(crate) expected_fetches: Option<usize>,
    /// How many objects stay under `_sqlload/` in the bucket.
    pub(crate) expected_staged_left: usize,
}

/// What a routes scenario left behind.
#[derive(Debug, Default)]
pub(crate) struct RouteOutcome {
    pub(crate) error: String,
    pub(crate) plan: String,
    pub(crate) sent: Vec<String>,
    pub(crate) fetches: usize,
    pub(crate) staged_left: usize,
    /// Whether the driver's sequence ids only went up.
    pub(crate) sequence_ids_rise: bool,
}

/// `debug` on a project with a managed external stage on the fake.
pub(crate) struct DebugStageTestCase {
    pub(crate) description: &'static str,
    pub(crate) prepare: fn(&sqlload_testkit::fake_snowflake::models::FakeSnowflake),
    /// Text one of the lines must contain.
    pub(crate) expected_line: &'static str,
    /// Text one of the statements must contain.
    pub(crate) expected_sent: &'static str,
}

/// A files table loaded once, its `_sqlload_source_row` then gone as in a table from before it, then loaded again.
pub(crate) struct SourceRowUpgradeTestCase {
    pub(crate) description: &'static str,
    /// TOML added to the source.
    pub(crate) settings: &'static str,
    /// Each row's id and row number, `-` for NULL.
    pub(crate) expected_rows: &'static str,
}

/// A files source of one partition, with its cursor settings.
pub(crate) struct CursorSource {
    pub(crate) source: &'static str,
    /// The partition column and its type.
    pub(crate) partition: (&'static str, &'static str),
    pub(crate) value: &'static str,
    /// TOML naming its cursor_column, and any other settings.
    pub(crate) settings: &'static str,
}

/// One `--start-cursor` over a selection of `CursorSource`s, and what the plan says.
pub(crate) struct MixedCursorTestCase {
    pub(crate) description: &'static str,
    pub(crate) selectors: &'static [&'static str],
    pub(crate) start: &'static str,
    /// Text the plan must contain.
    pub(crate) expected_plan: &'static str,
    /// Text the plan must not contain.
    pub(crate) expected_absent: &'static str,
}

/// One load of files into a fresh fake Databricks workspace.
pub(crate) struct DatabricksLoadTestCase {
    pub(crate) description: &'static str,
    /// TOML added to `[sources.orders]`.
    pub(crate) settings: &'static str,
    pub(crate) file_set: RouteFileSet,
    /// What the fake fails or holds before the load.
    pub(crate) prepare: fn(&sqlload_testkit::fake_databricks::models::FakeDatabricks),
    /// Text the plan or load error must contain; empty when the run must succeed.
    pub(crate) expected_error: &'static str,
    /// Text the rendered plan must contain.
    pub(crate) expected_plan: &'static [&'static str],
    /// Text some statement sent must contain.
    pub(crate) expected_sent: &'static [&'static str],
    /// Text no statement sent may contain.
    pub(crate) expected_unsent: &'static [&'static str],
    /// Rows in `raw.orders` afterwards; `None` when the fake cannot apply the write.
    pub(crate) expected_rows: Option<usize>,
    /// How many `COPY INTO`s read staged files.
    pub(crate) expected_copies: usize,
    /// Files and folders left under the volume's `sqlload/` folder.
    pub(crate) expected_left: &'static [&'static str],
    /// Statements cancelled after polling them failed.
    pub(crate) expected_cancelled: usize,
}

/// What one Databricks load planned, sent and left behind.
pub(crate) struct DatabricksOutcome {
    pub(crate) error: String,
    pub(crate) plan: String,
    pub(crate) sent: Vec<String>,
    pub(crate) rows: usize,
    pub(crate) copies: usize,
    pub(crate) left: Vec<String>,
    pub(crate) cancelled: usize,
}

/// A real Databricks workspace the live test may load into: only its `sqlload_` schema.
pub(crate) struct LiveDatabricks {
    pub(crate) host: String,
    pub(crate) client_id: String,
    pub(crate) client_secret: String,
    pub(crate) warehouse_id: String,
    pub(crate) catalog: String,
    pub(crate) schema: String,
}

/// One live load: the source's name (its table's too), its settings and its files.
pub(crate) struct LiveDatabricksTestCase {
    pub(crate) description: &'static str,
    pub(crate) source: &'static str,
    pub(crate) settings: &'static str,
    pub(crate) file_set: RouteFileSet,
    /// Rows in the table afterwards.
    pub(crate) expected_rows: &'static str,
}

/// One `plan` of a `postgres_cdc` project and a line its output must hold.
pub(crate) struct CdcPlanTestCase {
    pub(crate) description: &'static str,
    pub(crate) args: &'static [&'static str],
    pub(crate) expected_output: &'static str,
}

/// One `postgres_cdc` scenario against the live Databricks workspace, checked with a query there.
pub(crate) struct LiveCdcTestCase {
    pub(crate) description: &'static str,
    /// More `sqlload.toml` text appended to the project's, such as another source.
    pub(crate) extra_config: &'static str,
    pub(crate) steps: &'static [CdcStep],
    /// A Databricks query, `{schema}` standing for the qualified live schema, and its expected single text result.
    pub(crate) check_query: &'static str,
    pub(crate) expected_result: &'static str,
}

/// One query against the fake workspace with its answers delivered a given way.
pub(crate) struct DatabricksAnswerTestCase {
    pub(crate) description: &'static str,
    /// Rows per result chunk; `None` sends every row in the first.
    pub(crate) chunk_rows: Option<usize>,
    /// Polls before each statement is done.
    pub(crate) pending_polls: usize,
    /// The column names `information_schema.columns` returns, in order.
    pub(crate) expected_columns: &'static [&'static str],
}

/// One `sqlload` run of a captured-changes scenario, as a separate process.
pub(crate) struct CdcStep {
    /// SQL run in Postgres before this run.
    pub(crate) source_change: &'static str,
    /// The `sqlload` arguments after `--config`.
    pub(crate) args: &'static [&'static str],
    /// Where the process stops itself (`SQLLOAD_TEST_CRASH_AT`); empty for nowhere.
    pub(crate) crash_at: &'static str,
    pub(crate) expected_success: bool,
    /// Text the run's output must contain.
    pub(crate) expected_output: &'static str,
}

pub(crate) struct CdcScenarioTestCase {
    pub(crate) description: &'static str,
    /// The Postgres image tag.
    pub(crate) tag: &'static str,
    pub(crate) steps: &'static [CdcStep],
    /// The most bytes of changes a load keeps in memory before spilling them to disk; empty for the default.
    pub(crate) spill_bytes: &'static str,
    /// More `sqlload.toml` text appended to [`cdc_project`](crate::helpers::cdc_project)'s, such as another source.
    pub(crate) extra_config: &'static str,
    /// A query on the DuckDB file after the last step, and its expected single text result.
    pub(crate) check_query: &'static str,
    pub(crate) expected_result: &'static str,
}

/// A quiet `postgres_cdc` source on one Postgres version: whether its load confirmed past the WAL other tables wrote.
pub(crate) struct QuietSlotTestCase {
    pub(crate) description: &'static str,
    /// The Postgres image tag.
    pub(crate) tag: &'static str,
    pub(crate) expected_advanced: (bool, bool),
}

/// A `load --follow` started in the background while the source changes, then stopped.
pub(crate) struct FollowTestCase {
    pub(crate) description: &'static str,
    /// The Postgres image tag.
    pub(crate) tag: &'static str,
    /// The `load --follow` arguments.
    pub(crate) args: &'static [&'static str],
    /// Where it stops itself (`SQLLOAD_TEST_CRASH_AT`); empty for nowhere.
    pub(crate) crash_at: &'static str,
    /// Source statements run one by one, about every 1.5 seconds, once it streams.
    pub(crate) writes: &'static [&'static str],
    /// The source's `batch_interval`.
    pub(crate) batch_interval: &'static str,
    /// The signal sent after the writes, as `kill` takes it; `-0` sends none, leaving `--max-duration` to stop it.
    pub(crate) signal: &'static str,
    /// Whether, 3 seconds after the last write and before the signal, the slot confirmed the WAL position read after it.
    pub(crate) expected_advanced: bool,
    /// The fewest batches its output logs as written.
    pub(crate) expected_batches: usize,
    /// Whether the follow succeeds, and text its output holds.
    pub(crate) expected_follow: (bool, &'static str),
    /// Text the output of a catch-up load run after it holds.
    pub(crate) expected_after: &'static str,
    /// A query on the DuckDB file at the end, and its expected single text result.
    pub(crate) check_query: &'static str,
    pub(crate) expected_result: &'static str,
}

/// A `postgres_cdc` scenario into a files target (see [`cdc_files_project`](crate::helpers::cdc_files_project)), with what is planted in the folders after its first step.
pub(crate) struct CdcFilesTestCase {
    pub(crate) description: &'static str,
    /// The Postgres image tag.
    pub(crate) tag: &'static str,
    pub(crate) steps: &'static [CdcStep],
    /// The target's store (see [`local_store`](crate::helpers::local_store)), from an S3 endpoint and a local folder.
    pub(crate) store:
        fn(&str, &std::path::Path) -> (sqlload::store_paths::models::StoreKind, String),
    /// The most bytes of changes a load keeps in memory before spilling them to disk; empty for the default.
    pub(crate) spill_bytes: &'static str,
    /// More `sqlload.toml` text appended to the project's.
    pub(crate) extra_config: &'static str,
    /// Writes into the local folder of a local store after the first step.
    pub(crate) plant: fn(&std::path::Path) -> Result<(), String>,
    /// A DuckDB query over the written files, copied to a local folder that `{root}` stands for, and its expected single text result.
    pub(crate) check_query: &'static str,
    pub(crate) expected_result: &'static str,
}

/// A real BigQuery project the live tests may load into: only its `sqlload_` dataset.
pub(crate) struct LiveBigQuery {
    /// The service-account key file's JSON text.
    pub(crate) key: String,
    pub(crate) project: String,
    pub(crate) location: String,
    pub(crate) dataset: String,
}

/// Rows streamed through one pending Storage Write API stream into a new table.
pub(crate) struct LiveWriteApiTestCase {
    pub(crate) description: &'static str,
    pub(crate) table: &'static str,
    /// The ids of each append request's rows.
    pub(crate) batches: &'static [&'static [i64]],
    /// The row offset the first request claims; a new stream is at 0.
    pub(crate) first_offset: i64,
    pub(crate) finalize: bool,
    pub(crate) expected_stream_type: i32,
    /// Each request's error code (0 when accepted), the offset its rows landed at (-1 when none), how many rows it refused, and the first refused row's index (-1 when none).
    pub(crate) expected_append_answers: &'static [(i32, i64, usize, i64)],
    pub(crate) expected_finalized_rows: i64,
    pub(crate) expected_visible_before_commit: &'static str,
    /// `committed true` (with a commit time), or the commit's error with the stream's name as `<stream>`.
    pub(crate) expected_commit: &'static str,
    pub(crate) expected_rows: &'static str,
}

/// What one streamed table showed.
#[derive(Debug, Default)]
pub(crate) struct WriteApiOutcome {
    pub(crate) stream_type: i32,
    pub(crate) append_answers: Vec<(i32, i64, usize, i64)>,
    pub(crate) finalized_rows: i64,
    /// The rows a query saw after the appends, before the commit.
    pub(crate) visible_before_commit: String,
    pub(crate) commit: String,
    /// The rows after the commit, as `id:name:amount:placed_at:note`.
    pub(crate) rows: String,
}

/// One load of files into a live BigQuery dataset.
pub(crate) struct LiveBigQueryLoadTestCase {
    pub(crate) description: &'static str,
    pub(crate) source: &'static str,
    pub(crate) settings: &'static str,
    pub(crate) file_set: RouteFileSet,
    /// Rows in the table afterwards.
    pub(crate) expected_rows: &'static str,
}

/// One load of files into a fresh fake BigQuery project.
pub(crate) struct BigQueryLoadTestCase {
    pub(crate) description: &'static str,
    /// TOML added to `[sources.orders]`.
    pub(crate) settings: &'static str,
    pub(crate) file_set: RouteFileSet,
    /// What the fake refuses or holds before the load.
    pub(crate) prepare: fn(&sqlload_testkit::fake_bigquery::models::FakeBigQuery),
    /// Text the load error must contain; empty when the run must succeed.
    pub(crate) expected_error: &'static str,
    /// Text some statement sent must contain.
    pub(crate) expected_sent: &'static [&'static str],
    /// Text no statement sent may contain.
    pub(crate) expected_unsent: &'static [&'static str],
    /// Rows in `raw.orders` afterwards; `None` when the fake cannot apply the write.
    pub(crate) expected_rows: Option<usize>,
    /// The tables each Storage Write API commit went into, in order.
    pub(crate) expected_commits: &'static [&'static str],
}

/// What a load into the fake BigQuery did.
#[derive(Debug, Default)]
pub(crate) struct BigQueryOutcome {
    pub(crate) error: String,
    pub(crate) plan: String,
    pub(crate) sent: Vec<String>,
    pub(crate) rows: usize,
    /// Each commit's table, the load's staging names cut to `orders__sqlload_staging_`.
    pub(crate) commits: Vec<String>,
    /// Staging or build tables left in `raw`.
    pub(crate) staged: Vec<String>,
    /// Storage Write API calls sent without a routing header.
    pub(crate) unrouted_calls: usize,
}

/// Which test server a TLS case connects to; its index in the case's list of ports.
#[derive(Debug, Clone, Copy)]
pub(crate) enum TlsServer {
    /// Refuses connections without TLS.
    TlsOnly = 0,
    /// Offers no TLS.
    Plain = 1,
}

pub(crate) struct TlsDebugTestCase {
    pub(crate) description: &'static str,
    pub(crate) server: TlsServer,
    pub(crate) host: &'static str,
    pub(crate) user: &'static str,
    /// TLS settings for the connection, `FILES` standing for the test certificates' folder.
    pub(crate) settings: &'static str,
    /// How `debug`'s source line starts: connected or failed.
    pub(crate) expected_status: &'static str,
    /// Text that line holds.
    pub(crate) expected_text: &'static str,
}

/// Where a plan-warning case reaches its server; its index in the case's list of addresses.
#[derive(Debug, Clone, Copy)]
pub(crate) enum PlanAddress {
    /// The plain server on a loopback address.
    Loopback = 0,
    /// The plain server on its network address.
    Network = 1,
    /// The TLS-only server on its network address.
    TlsNetwork = 2,
}

pub(crate) struct PlanWarningTestCase {
    pub(crate) description: &'static str,
    pub(crate) address: PlanAddress,
    pub(crate) settings: &'static str,
    pub(crate) expected_warning: bool,
}

pub(crate) struct RelayTestCase {
    pub(crate) description: &'static str,
    pub(crate) server: TlsServer,
    pub(crate) host: &'static str,
    pub(crate) mode: sqlload::postgres_source::models::SslMode,
    /// A file in the test certificates' folder.
    pub(crate) root_cert: Option<&'static str>,
    /// Whether it opens a relay rather than leaving the client to connect directly or failing.
    pub(crate) expected_relayed: bool,
    /// Text of its error; empty when it opens.
    pub(crate) expected_error: &'static str,
}

pub(crate) struct RelaySessionTestCase {
    pub(crate) description: &'static str,
    pub(crate) server: TlsServer,
    pub(crate) host: &'static str,
    pub(crate) mode: sqlload::postgres_source::models::SslMode,
    /// The socket folder's permissions.
    pub(crate) expected_folder_mode: u32,
    /// Whether the folder still exists, and a second session can connect, once the first is taken.
    pub(crate) expected_left: (bool, bool),
}

pub(crate) struct CdcTlsTestCase {
    pub(crate) description: &'static str,
    /// TLS settings for the connection, `FILES` standing for the test certificates' folder.
    pub(crate) settings: &'static str,
    pub(crate) expected_result: &'static str,
}

/// A `debug` case through the SSH bastion.
pub(crate) struct SshDebugTestCase {
    pub(crate) description: &'static str,
    pub(crate) server: TlsServer,
    /// The database port as the bastion sees it; 5432 reaches the server.
    pub(crate) port: u16,
    pub(crate) user: &'static str,
    /// The connection's TLS and `ssh` settings: `FILES` stands for the test certificates' folder, `KEYS` for the bastion's key files, `SSH_PORT` for its port.
    pub(crate) settings: &'static str,
    pub(crate) expected_status: &'static str,
    pub(crate) expected_text: &'static str,
}

/// A `postgres_cdc` case through the SSH bastion.
pub(crate) struct CdcSshTestCase {
    pub(crate) description: &'static str,
    pub(crate) server: TlsServer,
    /// As in [`SshDebugTestCase::settings`].
    pub(crate) settings: &'static str,
    pub(crate) expected_result: &'static str,
}

/// A load through the bastion whose tunnel drops part way.
pub(crate) struct TunnelDropTestCase {
    pub(crate) description: &'static str,
    /// How long into the load (or, following, after streaming starts) the bastion stops.
    pub(crate) drop_after: std::time::Duration,
    pub(crate) expected_success: bool,
    /// Text the output holds; `SSH_PORT` stands for the bastion's port.
    pub(crate) expected_text: &'static str,
}

/// A ClickHouse load stopped at every statement in turn, its source switching between two states.
pub(crate) struct StoppedLoadTestCase {
    pub(crate) description: &'static str,
    /// The destination table, whose statements stop.
    pub(crate) table: &'static str,
    /// The options of every load after the first.
    pub(crate) options: sqlload::loader::models::PlanOptions,
    /// The table's state as one text.
    pub(crate) query: &'static str,
    /// Each source state: what sets it up (SQL for a database source, `day=id,...` parts for a files source), and the query's text once it is loaded.
    pub(crate) states: [(&'static str, &'static str); 2],
    pub(crate) expected_minimum_stops: usize,
}

/// One `load` of the ODBC scenario.
#[cfg(feature = "odbc")]
pub(crate) struct OdbcLoadTestCase {
    pub(crate) description: &'static str,
    /// SQL run in Postgres first.
    pub(crate) source_change: &'static str,
    /// The source's `max_text_size`.
    pub(crate) max_text_size: usize,
    /// Whether the load succeeds, and text its output holds.
    pub(crate) expected_output: (bool, &'static str),
    pub(crate) check_query: &'static str,
    pub(crate) expected_result: &'static str,
}

/// A Parquet events file a ClickHouse pull test writes.
pub(crate) struct EventsFile {
    pub(crate) key: &'static str,
    pub(crate) ids: std::ops::RangeInclusive<i64>,
    pub(crate) note: Option<&'static str>,
}

/// One load of the ClickHouse pull test: the files written before it, and what it must show.
pub(crate) struct FilesPullTestCase {
    pub(crate) description: &'static str,
    pub(crate) files: Vec<EventsFile>,
    /// Text the plan holds, or the error the load fails with.
    pub(crate) expected_plan: Result<&'static str, &'static str>,
    pub(crate) check_query: &'static str,
    pub(crate) expected_result: &'static str,
}

/// One load of a Google Sheets project against the fake API.
pub(crate) struct SheetsLoadTestCase {
    pub(crate) description: &'static str,
    /// The spreadsheet's tabs for this load.
    pub(crate) tabs: fn() -> Vec<sqlload_testkit::fake_sheets::models::FakeTab>,
    /// How many reads the fake refuses with 401 first.
    pub(crate) refused_reads: usize,
    /// Text the load's output holds.
    pub(crate) expected_output: &'static str,
    pub(crate) query: &'static str,
    pub(crate) expected_result: &'static str,
}

/// A google_sheets source `plan` refuses.
pub(crate) struct SheetsRefusalTestCase {
    pub(crate) description: &'static str,
    /// The source's TOML, added to the test project.
    pub(crate) source: &'static str,
    /// Text the refusal holds.
    pub(crate) expected_output: &'static str,
}

pub(crate) struct S3ProfileTestCase {
    pub(crate) description: &'static str,
    /// The `s3` connection's `profile`.
    pub(crate) profile: &'static str,
    /// The `sqlload` command run, with no AWS keys in its environment.
    pub(crate) command: &'static str,
    /// Text its output must contain.
    pub(crate) expected_text: &'static str,
    pub(crate) expected_ok: bool,
}

/// One load from a bigquery source into DuckDB.
pub(crate) struct BigQuerySourceTestCase {
    pub(crate) description: &'static str,
    /// Rows added to the fake's `marts.orders` before the load.
    pub(crate) rows: fn() -> Vec<serde_json::Value>,
    /// How many read streams break after their first row.
    pub(crate) broken_reads: usize,
    /// The table's rows after the load, as `id:label:amount:at`.
    pub(crate) expected_rows: &'static str,
    /// The row restriction the load's read session asked for.
    pub(crate) expected_restriction: &'static str,
}

/// Two loads from a bigquery source into DuckDB of a one-column table appended by that column as its cursor.
pub(crate) struct BigQueryCursorTestCase {
    pub(crate) description: &'static str,
    /// The table's column and its DDL type in the fake's `marts.values` table.
    pub(crate) column: (&'static str, &'static str),
    /// The values the first load reads, then those added before the next.
    pub(crate) values: (&'static [&'static str], &'static [&'static str]),
    /// The row restriction of the next load's read session.
    pub(crate) expected_restriction: &'static str,
    /// The loaded values, as DuckDB text in order, joined by commas.
    pub(crate) expected_values: &'static str,
}

/// A file put on the SFTP server: its absolute path there, its bytes and its modification time (Unix seconds).
pub(crate) struct SftpFile {
    pub(crate) path: &'static str,
    pub(crate) bytes: fn() -> Result<bytes::Bytes, String>,
    pub(crate) mtime: u32,
}

/// One step against the SFTP server, in order: put `files`, sign in with `sign_in`, run `command`, then check the output and DuckDB.
pub(crate) struct SftpStepTestCase {
    pub(crate) description: &'static str,
    pub(crate) files: &'static [SftpFile],
    /// The connection's sign-in and host key settings, with `KEYS` for the bastion's key files.
    pub(crate) sign_in: &'static str,
    pub(crate) command: &'static str,
    pub(crate) expected_success: bool,
    pub(crate) expected_output: &'static str,
    /// Run on the DuckDB file after the command.
    pub(crate) check_query: &'static str,
    pub(crate) expected_result: &'static str,
}

/// One plan of SFTP files into the fake Snowflake, with `settings` on the source.
pub(crate) struct SftpSnowflakePlanTestCase {
    pub(crate) description: &'static str,
    pub(crate) settings: &'static str,
    pub(crate) expected_success: bool,
    pub(crate) expected_output: &'static str,
}

/// One database's run of the ODBC cursor scenario.
#[cfg(feature = "odbc")]
pub(crate) struct OdbcDriverTestCase {
    pub(crate) description: &'static str,
    pub(crate) database: sqlload_testkit::odbc_servers::models::OdbcDatabase,
    /// Appended to the connection string SQLLoad reads through.
    pub(crate) connection_options: &'static str,
    /// The events and days tables as the source's `include` names them.
    pub(crate) tables: (&'static str, &'static str),
    /// Statements creating both tables and their first rows.
    pub(crate) setup: &'static [&'static str],
    /// Statements adding rows around each cursor's maximum before the second load.
    pub(crate) change: &'static [&'static str],
    /// Each table's ids and newest cursor value after the first load, then after the second.
    pub(crate) expected_tables: (&'static str, &'static str),
}

pub(crate) struct SheetsUserTestCase {
    pub(crate) description: &'static str,
    /// The connection's `refresh_token`.
    pub(crate) refresh: Option<&'static str>,
    /// Refresh tokens the fake refuses as revoked.
    pub(crate) revoked: &'static [&'static str],
    pub(crate) expected_code: i32,
    /// Text the output holds.
    pub(crate) expected_output: &'static str,
    /// The grant type every token request has (plan and load each ask for a token), or none when no token is asked for.
    pub(crate) expected_grant: Option<&'static str>,
}

/// A CSV file a files merge reads, with its modified time in seconds since 1970.
pub(crate) struct MergeFile {
    pub(crate) key: &'static str,
    pub(crate) contents: &'static str,
    pub(crate) modified_secs: u64,
}

/// One load of a files merge, after writing `files`.
pub(crate) struct MergeStep {
    /// SQL run on the target before the step, such as a `DROP TABLE`.
    pub(crate) before: &'static [&'static str],
    pub(crate) files: &'static [MergeFile],
    /// Whether the step loads; `false` plans and applies only, as `sqlload apply` does.
    pub(crate) loads: bool,
    /// Text the load's error must contain; empty when it must succeed.
    pub(crate) expected_error: &'static str,
    /// Text the plan must contain when the load succeeds.
    pub(crate) expected_plan: &'static str,
}

/// A files source loading CSV files with `settings` into one table, step by step, then `check_query`'s single text.
pub(crate) struct FilesMergeTestCase {
    pub(crate) description: &'static str,
    pub(crate) settings: &'static str,
    pub(crate) steps: &'static [MergeStep],
    pub(crate) check_query: &'static str,
    pub(crate) expected_result: &'static str,
}

/// One `sqlload load --json` of an `after_load` case.
pub(crate) struct AfterLoadStep {
    /// Files written first, by path below the connection's root, such as `drop/a.csv`.
    pub(crate) files: &'static [(&'static str, &'static str)],
    /// Folders removed next, by path below the root, such as `drop`.
    pub(crate) removed: &'static [&'static str],
    pub(crate) expected_exit: i32,
    /// The table's `status`, `rows` and `after_load` in the report, with the plan's `errors` and `skipped` reasons (the root as `<root>`), as compact JSON.
    pub(crate) expected_report: &'static str,
}

/// A CSV files source under `drop/` with `settings`, loaded step by step into DuckDB by the executable, then the files left below the root.
pub(crate) struct AfterLoadTestCase {
    pub(crate) description: &'static str,
    pub(crate) settings: &'static str,
    /// Folders below the root made links to a fresh folder in another directory, such as `/dev/shm`, which may be another volume.
    pub(crate) linked: &'static [(&'static str, &'static str)],
    pub(crate) steps: &'static [AfterLoadStep],
    /// Every file left below the root, sorted, as `path=contents`.
    pub(crate) expected_files: &'static [&'static str],
}

/// `after_load` run directly on files in its own folder of an S3 bucket.
pub(crate) struct S3AfterLoadTestCase {
    pub(crate) description: &'static str,
    /// The case's folder in the bucket, such as `move/`; the source reads `drop/` below it.
    pub(crate) folder: &'static str,
    pub(crate) after_load: sqlload::files_source::models::AfterLoad,
    /// Files written first, by key below `folder`.
    pub(crate) files: &'static [(&'static str, &'static str)],
    /// Files rewritten, by key below `folder`, after the source's files were listed.
    pub(crate) rewritten: &'static [(&'static str, &'static str)],
    /// The report as `action status files failed`.
    pub(crate) expected_report: &'static str,
    /// Every file left below `folder`, sorted, as `key=contents`.
    pub(crate) expected_files: &'static [&'static str],
}

/// One repeated-key rule: a load of `public.readings`, merged on `sensor` with `reading_id` as cursor, from a fresh target table.
pub(crate) struct DedupeRule {
    pub(crate) description: &'static str,
    /// Table settings after the merge's own, such as `dedup_sort`.
    pub(crate) settings: &'static str,
    /// Text the plan's or the load's error must contain; empty when the load must succeed.
    pub(crate) expected_error: &'static str,
    /// Text the plan must contain when the load succeeds.
    pub(crate) expected_plan: &'static str,
}

/// A `dedup_sort` plan refuses.
pub(crate) struct DedupeRefusalTestCase {
    pub(crate) description: &'static str,
    pub(crate) rule: DedupeRule,
    /// How many tables named `app__readings` the target has afterwards.
    pub(crate) expected_tables: &'static str,
}

/// A target the repeated-key rules are checked on.
pub(crate) struct DedupeTargetTestCase {
    pub(crate) description: &'static str,
    /// The target connection's TOML lines.
    pub(crate) connection: String,
    pub(crate) schema: &'static str,
    /// A statement run on the target before each rule, so each starts from no table.
    pub(crate) reset: &'static str,
    /// The table's rows as one text value, `sensor:amount` ordered by sensor (NULLs last) and amount.
    pub(crate) check_query: &'static str,
    /// What `check_query` returns after each repeated-key rule, in their order: last row, highest, lowest, fail.
    pub(crate) expected_rows: [&'static str; 4],
}

/// An NDJSON files source under `drop/` with `settings`, planned by the executable into a DuckDB target, or a filesystem files target when `files_target`.
pub(crate) struct UnnestPlanTestCase {
    pub(crate) description: &'static str,
    pub(crate) settings: &'static str,
    pub(crate) files_target: bool,
    /// The one file under `drop/`.
    pub(crate) file: &'static str,
    pub(crate) expected_ok: bool,
    /// Text the plan's output holds; empty when it plans.
    pub(crate) expected_problem: &'static str,
}

/// An NDJSON files source under `drop/` with `settings`, loaded into DuckDB once per run, then once more with `failing`'s file, which fails.
pub(crate) struct UnnestFailTestCase {
    pub(crate) description: &'static str,
    pub(crate) settings: &'static str,
    /// Each run's new file before the failing one, as `name`, `contents`.
    pub(crate) runs: &'static [(&'static str, &'static str)],
    pub(crate) failing: (&'static str, &'static str),
    /// Text the failed load's output holds.
    pub(crate) expected_error: &'static str,
    /// A query of the database after the failed load and what it returns: what the failure left.
    pub(crate) expected_after: (&'static str, &'static str),
}

/// An NDJSON files source under `drop/` with `settings`, loaded by the executable into DuckDB once per run, each run adding its file.
pub(crate) struct RowIdTestCase {
    pub(crate) description: &'static str,
    pub(crate) settings: &'static str,
    /// Each run's new file, as `name`, `contents`.
    pub(crate) runs: &'static [(&'static str, &'static str)],
    /// The table's columns in order, joined by `,`.
    pub(crate) expected_columns: &'static str,
    /// A query over `raw.orders` (and its child tables) returning one text.
    pub(crate) check_query: &'static str,
    pub(crate) expected_result: &'static str,
}

/// An NDJSON files source under `drop/` with `settings`, loaded by the executable into a files target (`out/{table}/`) once per run, each run adding its file.
pub(crate) struct FilesTargetUnnestTestCase {
    pub(crate) description: &'static str,
    pub(crate) settings: &'static str,
    /// Each run's new file, as `name`, `contents`.
    pub(crate) runs: &'static [(&'static str, &'static str)],
    /// A DuckDB query over the target's Parquet files, each table read as `{out}/<table>/**/*.parquet`, returning one text.
    pub(crate) check_query: &'static str,
    pub(crate) expected_result: &'static str,
}

/// A merge into child tables whose rows hang off root ids the merge's keys did not give (as dlt's `delete-insert` merge leaves them).
pub(crate) struct RekeyedTestCase {
    pub(crate) description: &'static str,
    pub(crate) settings: &'static str,
    /// The run loaded before the ids are rewritten.
    pub(crate) loaded: &'static [(&'static str, &'static str)],
    /// The run merged after.
    pub(crate) merged: &'static [(&'static str, &'static str)],
    /// Statements giving the loaded roots ids their keys don't derive, their child rows following.
    pub(crate) rekey: &'static str,
    pub(crate) check_query: &'static str,
    pub(crate) expected_result: &'static str,
}

/// A ClickHouse child table cleared of the items of the roots a merge stages.
pub(crate) struct ClearRowsTestCase {
    pub(crate) description: &'static str,
    /// The child table's `PARTITION BY` clause, empty for none.
    pub(crate) partition_by: &'static str,
    /// The child table's rows, as `VALUES` of `(root id, sku)`.
    pub(crate) rows: &'static str,
    /// The staged parent rows' ids, as `VALUES`.
    pub(crate) roots: &'static str,
    /// The rows left, as sorted `root:sku`, joined by `,`.
    pub(crate) expected_rows: &'static str,
    /// The staged roots left once root `r1`'s staged row is deleted, sorted and joined by `,`.
    pub(crate) expected_staged: &'static str,
}

/// `orders` loaded twice with `settings` (a replace under `unnest = "all"`) into DuckDB, Postgres and ClickHouse, each seeded first with tables named like its child tables.
pub(crate) struct NamedLikeTestCase {
    pub(crate) description: &'static str,
    pub(crate) settings: &'static str,
    /// The naming's parent-id column, which the seeded child table of another root holds.
    pub(crate) parent_id: &'static str,
    /// Rows left in `orders__notes/orders__x__items/orders__tags/orders__items`.
    pub(crate) expected_result: &'static str,
}
