pub(crate) struct PostgresColumnTestCase {
    pub(crate) description: &'static str,
    pub(crate) column: &'static str,
    /// The natural portable type the catalog maps the column to.
    pub(crate) expected_type: &'static str,
    /// Each row's value as Arrow displays it; empty for NULL.
    pub(crate) expected_values: &'static [&'static str],
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
}

pub(crate) struct FilesTargetScenarioTestCase {
    pub(crate) description: &'static str,
    /// The store and its connection TOML, given the S3 endpoint and a fresh temporary directory.
    pub(crate) store:
        fn(&str, &std::path::Path) -> (sqlload::store_paths::models::StoreKind, String),
    /// SQL creating the source table `public.orders` in a fresh `public` schema.
    pub(crate) setup: &'static str,
    /// Project TOML appended after `[sources.app]`; `PREFIX` is replaced by the scenario's own folder.
    pub(crate) settings: &'static str,
    pub(crate) runs: &'static [FilesTargetRun],
    /// The column shown beside each row's id.
    pub(crate) check_column: &'static str,
    /// Per partition folder: the runs that wrote its files, then `id:value` per row.
    pub(crate) expected_files: &'static str,
}

/// One row read back from a written file.
pub(crate) struct WrittenRow {
    /// The partition folder, `-` without partitions.
    pub(crate) folder: String,
    pub(crate) id: String,
    pub(crate) value: String,
    pub(crate) load_id: String,
}

/// One partition folder of the written files: the runs whose loads wrote it, and its rows as `(id, "id:value")`.
#[derive(Default)]
pub(crate) struct WrittenPartition {
    pub(crate) runs: std::collections::BTreeSet<usize>,
    pub(crate) rows: Vec<(i64, String)>,
}
