use std::time::Duration;

use crate::config::models::LoadErrorPolicy;

pub(crate) struct ParseConfigTestCase {
    pub(crate) description: &'static str,
    pub(crate) text: &'static str,
    pub(crate) expected_columns: usize,
}

pub(crate) struct ParseConfigErrorTestCase {
    pub(crate) description: &'static str,
    pub(crate) text: &'static str,
    pub(crate) expected_error: &'static str,
}

pub(crate) struct StreamSettingsTestCase {
    pub(crate) description: &'static str,
    pub(crate) topic: &'static str,
    pub(crate) expected_max_rows: usize,
    pub(crate) expected_max_age: Duration,
}

pub(crate) struct DestinationNameTestCase {
    pub(crate) description: &'static str,
    pub(crate) schema: &'static str,
    /// The stored schema name, or text the error must contain.
    pub(crate) expected_outcome: &'static str,
}

pub(crate) struct StartupSettingsTestCase {
    pub(crate) description: &'static str,
    /// Lines added to `[source]`.
    pub(crate) source_lines: &'static str,
    pub(crate) expected_batch: usize,
    pub(crate) expected_interval: Duration,
    pub(crate) expected_budget: u64,
}

pub(crate) struct LoadErrorPolicyTestCase {
    pub(crate) description: &'static str,
    /// Lines added to `[staging]`.
    pub(crate) staging_lines: &'static str,
    pub(crate) expected_policy: LoadErrorPolicy,
    pub(crate) expected_max_pending_files: usize,
    pub(crate) expected_check_after: Duration,
}

pub(crate) struct DatabricksConfigTestCase {
    pub(crate) description: &'static str,
    /// Lines added after the Databricks `[destination]`.
    pub(crate) text: &'static str,
    pub(crate) expected_path: crate::planning::models::LoadPath,
    pub(crate) expected_format: crate::config::models::FileFormat,
}

pub(crate) struct ExampleConfigTestCase {
    pub(crate) description: &'static str,
    /// Relative to the crate's directory.
    pub(crate) path: &'static str,
    pub(crate) expected_database: &'static str,
    pub(crate) expected_schema: &'static str,
    pub(crate) expected_path: crate::planning::models::LoadPath,
    pub(crate) expected_format: crate::config::models::FileFormat,
    pub(crate) expected_first_column: Option<&'static str>,
    pub(crate) expected_column_count: usize,
}

pub(crate) struct BigQueryConfigTestCase {
    pub(crate) description: &'static str,
    /// Lines added after the BigQuery `[destination]`.
    pub(crate) text: &'static str,
    pub(crate) expected_path: crate::planning::models::LoadPath,
    pub(crate) expected_format: crate::config::models::FileFormat,
}
