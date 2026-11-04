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
    /// `s3` (SeaweedFS) or `filesystem` (a temporary directory).
    pub(crate) store: &'static str,
}
