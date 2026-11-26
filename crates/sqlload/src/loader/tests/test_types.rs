use arrow_schema::Schema;

use crate::column_types::models::{ColumnType, Warehouse};
use crate::load_naming::models::Naming;
use crate::loader::models::CursorBinding;

pub(crate) struct FittingTypeTestCase {
    pub(crate) description: &'static str,
    pub(crate) inferred: ColumnType,
    pub(crate) live: Option<ColumnType>,
    pub(crate) expected_type: ColumnType,
}

pub(crate) struct InferredTableTestCase {
    pub(crate) description: &'static str,
    /// The schema the yielded batches merged to.
    pub(crate) schema: fn() -> Schema,
    /// Columns declared in `columns` before any data.
    pub(crate) declared: &'static [&'static str],
    /// Columns planning knew before the data: the declared ones and the live table's.
    pub(crate) planned: &'static [&'static str],
    /// Columns that only ever held NULL.
    pub(crate) null_columns: &'static [&'static str],
    pub(crate) live: &'static [(&'static str, ColumnType)],
    /// The table's columns as `name:type`, in order.
    pub(crate) expected_columns: &'static str,
}

pub(crate) struct SourceSpellingTestCase {
    pub(crate) description: &'static str,
    pub(crate) stored: &'static str,
    pub(crate) warehouse: Warehouse,
    pub(crate) expected_name: &'static str,
}

pub(crate) struct DeclaredProblemsTestCase {
    pub(crate) description: &'static str,
    /// The Python table's settings, as the host's JSON.
    pub(crate) settings: &'static str,
    /// The data's column names.
    pub(crate) columns: &'static [&'static str],
    pub(crate) rows: u64,
    /// The problems joined by `; `, empty when there are none.
    pub(crate) expected_problems: &'static str,
}

pub(crate) struct RenderTallyTestCase {
    pub(crate) description: &'static str,
    /// The lines `apply` printed.
    pub(crate) applied: &'static [&'static str],
    /// Each table, its rows or failure, and its milliseconds.
    pub(crate) outcomes: &'static [(&'static str, Result<u64, &'static str>, u64)],
    pub(crate) elapsed_ms: u64,
    pub(crate) expected_output: &'static str,
}

/// A Python table's settings, with its `parse` and `timezone`.
pub(crate) struct TableParsesTestCase {
    pub(crate) description: &'static str,
    /// The table's settings as JSON.
    pub(crate) settings: &'static str,
    /// Each parsed column by its loaded name, the zone and where it is set (`updated_at|Europe/Paris|table`), or text a plan problem holds.
    pub(crate) expected_outcome: Result<&'static str, &'static str>,
}

pub(crate) struct TransformReaderTestCase {
    pub(crate) description: &'static str,
    /// The files table's `transform`, if any.
    pub(crate) transform: Option<&'static str>,
    /// The column the table parses, if any.
    pub(crate) parse: Option<&'static str>,
    /// Why SQLLoad must read the Parquet files itself, if it must.
    pub(crate) expected_reason: Option<&'static str>,
}

/// A table's run log records, oldest first, as event, note, files and `transform` version.
pub(crate) type TransformRecord = (
    crate::run_log::models::RunEvent,
    &'static str,
    &'static [&'static str],
    Option<&'static str>,
);

pub(crate) struct OlderFilesTestCase {
    pub(crate) description: &'static str,
    pub(crate) records: &'static [TransformRecord],
    /// How many files the table holds from a version other than `f@2`.
    pub(crate) expected_older: usize,
}

pub(crate) struct LookbackStartTestCase {
    pub(crate) description: &'static str,
    pub(crate) maximum: &'static str,
    pub(crate) units: i64,
    pub(crate) cursor_type: crate::column_types::models::ColumnType,
    pub(crate) expected_start: Result<&'static str, &'static str>,
}

/// A plan item: its name, its action, and each detail's label and text.
pub(crate) type ItemSpec = (
    &'static str,
    &'static str,
    &'static [(&'static str, &'static str)],
);

/// A plan view built from items, and the exact text `plan` prints for it.
pub(crate) struct ViewLinesTestCase {
    pub(crate) description: &'static str,
    pub(crate) ok: bool,
    pub(crate) summary: &'static str,
    pub(crate) selected: usize,
    pub(crate) objects: &'static [ItemSpec],
    pub(crate) loads: &'static [ItemSpec],
    pub(crate) errors: &'static [ItemSpec],
    pub(crate) warnings: &'static [ItemSpec],
    /// The run's memory budget line.
    pub(crate) memory: Option<&'static str>,
    pub(crate) expected_output: &'static str,
}

/// A table of one plan: its name, its target, and the route its rows take.
pub(crate) struct RoutedTable {
    pub(crate) name: &'static str,
    pub(crate) target: &'static str,
    pub(crate) route: fn() -> crate::routing::models::Route,
}

/// Tables of one plan, and the plan's `to` and `route` lines.
pub(crate) struct RouteLinesTestCase {
    pub(crate) description: &'static str,
    pub(crate) tables: &'static [RoutedTable],
    pub(crate) expected_lines: &'static [&'static str],
}

/// Errors items before merging, and each merged item's name.
pub(crate) struct MergedErrorsTestCase {
    pub(crate) description: &'static str,
    pub(crate) errors: &'static [ItemSpec],
    pub(crate) expected_names: &'static [&'static str],
}

pub(crate) struct SourceFilterTestCase {
    pub(crate) description: &'static str,
    pub(crate) cursor_type: ColumnType,
    pub(crate) dialect: Option<Warehouse>,
    /// How the bounds are written: literals, or, for an ODBC source, parameters or escapes with the cursor column's fractional digits when the source reports them.
    pub(crate) binding: CursorBinding,
    pub(crate) lower: Option<(&'static str, &'static str)>,
    pub(crate) upper: Option<&'static str>,
    pub(crate) expected_filter: Option<&'static str>,
    /// The values bound to the filter's placeholders, in order, as Debug prints them.
    pub(crate) expected_parameters: &'static [&'static str],
}

pub(crate) struct RetryableErrorTestCase {
    pub(crate) description: &'static str,
    pub(crate) error: fn() -> crate::loader::errors::LoaderError,
    pub(crate) expected_retryable: bool,
}

pub(crate) struct MergedFileTestCase {
    pub(crate) description: &'static str,
    /// The file's name, then its version, as a merge records them.
    pub(crate) file: (&'static str, &'static str),
    /// The name and version the ledger reads back.
    pub(crate) expected_file: (&'static str, &'static str),
}

pub(crate) struct ReloadOrderTestCase {
    pub(crate) description: &'static str,
    /// The files listed now, by folder (`drop/`, the source's, or `done/`, where `after_load` moved them), key and modified time (their version is `1-{time}`).
    pub(crate) files: &'static [(&'static str, &'static str, i64)],
    /// The table's merge records, oldest first, as name (`drop/{key}`) and version.
    pub(crate) merged: &'static [(&'static str, &'static str)],
    pub(crate) expected_keys: &'static [&'static str],
}

pub(crate) struct ConstantsAddedTestCase {
    pub(crate) description: &'static str,
    /// The constants added: name, type and value as text.
    pub(crate) constants: &'static [(&'static str, ColumnType, &'static str)],
    /// Whether the batch comes back equal, and its columns as `name:Arrow type=values`; or the error.
    pub(crate) expected_outcome: Result<(bool, &'static str), &'static str>,
}

pub(crate) struct ChildPathsTestCase {
    pub(crate) description: &'static str,
    pub(crate) warehouse: Warehouse,
    pub(crate) naming: Naming,
    /// The parent's `child_pinned` keys (`items.sku`).
    pub(crate) pinned: &'static [&'static str],
    /// The parent table's name as the load writes it.
    pub(crate) parent: &'static str,
    /// Live child tables of `parent`, as the warehouse lists them.
    pub(crate) tables: &'static [&'static str],
    /// Each table's dotted path, as `(path, table)`.
    pub(crate) expected_paths: &'static [(&'static str, &'static str)],
}

#[cfg(feature = "odbc")]
pub(crate) struct RefusedBoundsTestCase {
    pub(crate) description: &'static str,
    /// Lines added to `[sources.erp]`.
    pub(crate) source_settings: &'static str,
    /// The cursor column's type, as the ODBC driver describes it, and the portable type it reads as.
    pub(crate) cursor_type: arrow_schema::DataType,
    pub(crate) column_type: ColumnType,
    /// The planned lower bound, when the destination has rows.
    pub(crate) from: Option<&'static str>,
    /// What the stub driver answers each query, in order; a query past the list runs.
    pub(crate) responses: &'static [Result<(), &'static str>],
    pub(crate) expected_problem: Option<&'static str>,
    /// Each query the check ran: its filter and bound values.
    pub(crate) expected_runs: &'static [&'static str],
}

/// A Postgres table `erp.stock` of `columns` (each name and whether it may be NULL) whose source reports `primary_key`, loaded with `settings`.
pub(crate) struct KeyNullsTestCase {
    pub(crate) description: &'static str,
    pub(crate) columns: &'static [(&'static str, bool)],
    pub(crate) primary_key: &'static [&'static str],
    /// The table's settings, `write_strategy` included.
    pub(crate) settings: &'static str,
    /// The key columns compared with `=`, joined by `, `; or the problem `plan` reports.
    pub(crate) expected_keys: Result<&'static str, &'static str>,
}

/// A coalesced or plain cursor's bounded filter under `null_cursor = "include"`, in one dialect.
pub(crate) struct NullCursorFilterTestCase {
    pub(crate) description: &'static str,
    /// The cursor's source expression, as `source_cursor` writes it.
    pub(crate) expression: &'static str,
    pub(crate) dialect: Option<Warehouse>,
    pub(crate) binding: CursorBinding,
    pub(crate) expected_filter: Option<&'static str>,
    /// How many values are bound to the filter's placeholders.
    pub(crate) expected_parameters: usize,
}

/// A table of Python source `s` named `name` (landing as `s__<name>`), written with `mode`.
pub(crate) struct ModedTable {
    pub(crate) name: &'static str,
    pub(crate) mode: fn() -> crate::adapters::contract::models::WriteMode,
}

/// A plan of `tables` and its skipped tables (destination and reason), and the lines `plan` prints for it without `-v`.
pub(crate) struct PlanLinesTestCase {
    pub(crate) description: &'static str,
    pub(crate) tables: &'static [ModedTable],
    pub(crate) skipped: &'static [(&'static str, &'static str)],
    pub(crate) expected_output: &'static str,
}

/// A `snowflake` source whose stage `url` is written as `url`, and the store its unloads go to.
pub(crate) struct UnloadStoreTestCase {
    pub(crate) description: &'static str,
    pub(crate) url: &'static str,
    pub(crate) expected_store: &'static str,
}

/// A table's spec, and the most cursor windows its read may be split into.
pub(crate) struct MostWindowsTestCase {
    pub(crate) description: &'static str,
    pub(crate) spec: fn() -> Result<crate::loader::models::TableSpec, String>,
    pub(crate) expected_most: Result<u64, &'static str>,
}

pub(crate) struct GcsReaderTestCase {
    pub(crate) description: &'static str,
    /// Whether the gcs connection sets an HMAC key.
    pub(crate) hmac: bool,
    /// The gcs connection's custom endpoint, if any.
    pub(crate) endpoint: Option<&'static str>,
    /// Why SQLLoad must read the files instead of DuckDB, if it must.
    pub(crate) expected_reason: Option<&'static str>,
}

/// A Parquet files table in GCS loading into BigQuery.
pub(crate) struct BigQueryReaderTestCase {
    pub(crate) description: &'static str,
    /// The columns the load writes, with their types.
    pub(crate) columns: Vec<(&'static str, crate::column_types::models::ColumnType)>,
    /// The gcs connection's custom endpoint, if any.
    pub(crate) endpoint: Option<&'static str>,
    /// Why SQLLoad must read the files instead of BigQuery, if it must.
    pub(crate) expected_reason: Option<&'static str>,
}

/// A BigQuery source's table loading into BigQuery through the source's own connection.
pub(crate) struct SelectedReaderTestCase {
    pub(crate) description: &'static str,
    /// A column the table masks, with its method.
    pub(crate) mask: Option<(&'static str, crate::project::models::MaskMethod)>,
    /// A column the table parses.
    pub(crate) parse: Option<&'static str>,
    /// Whether BigQuery selects the rows itself.
    pub(crate) expected_selected: bool,
    /// Why SQLLoad reads the rows instead, as `plan` shows it.
    pub(crate) expected_reason: Option<&'static str>,
}

pub(crate) struct MissingColumnsTestCase {
    pub(crate) description: &'static str,
    pub(crate) missing: &'static [&'static str],
    pub(crate) on_schema_change: crate::project::models::OnSchemaChange,
    /// The warnings, or the problem under `"fail"`.
    pub(crate) expected_result: Result<&'static [&'static str], &'static str>,
}
