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

pub(crate) struct LoadRunTestCase {
    pub(crate) description: &'static str,
    /// SQL run in Postgres before this load.
    pub(crate) source_change: &'static str,
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
    /// `--reload`.
    pub(crate) reload: bool,
    /// `--start-cursor-*` and `--end-cursor-*`.
    pub(crate) window: (Option<&'static str>, Option<&'static str>),
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

/// Postgres into a files target, then those files through a files source into DuckDB, loaded twice.
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
