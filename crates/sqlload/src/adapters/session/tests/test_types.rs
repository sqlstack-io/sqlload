use std::sync::{Arc, Mutex};

use crate::column_types::models::Warehouse;

/// What a fake caller's sessions were asked to do, in order: each statement, query, `COPY` and Parquet load.
pub(crate) type Calls = Arc<Mutex<Vec<String>>>;

/// A caller's connection over a DuckDB database, as a caller's own DuckDB connection would be; it records every call and fails the first statement containing `fail_on`.
#[derive(Debug)]
pub(crate) struct DuckDbCaller {
    pub(crate) connection: Mutex<duckdb::Connection>,
    pub(crate) calls: Calls,
    pub(crate) fail_on: Option<String>,
}

/// One session of a [`DuckDbCaller`].
#[derive(Debug)]
pub(crate) struct DuckDbSession {
    pub(crate) connection: Mutex<duckdb::Connection>,
    pub(crate) calls: Calls,
    pub(crate) fail_on: Option<String>,
}

/// A caller's connection that runs nothing and records every call: statements, queries, `COPY` input (its signature) and Parquet loads; it answers a question about autocommit with `autocommit`.
#[derive(Debug, Default)]
pub(crate) struct RecordingCaller {
    pub(crate) calls: Calls,
    pub(crate) autocommit: Option<&'static str>,
    /// The rows it answers a query of the catalog's constraints with.
    pub(crate) references: Vec<Vec<Option<String>>>,
}

/// One session of a [`RecordingCaller`].
#[derive(Debug)]
pub(crate) struct RecordingSession {
    pub(crate) calls: Calls,
    pub(crate) autocommit: Option<&'static str>,
    pub(crate) references: Vec<Vec<Option<String>>>,
}

pub(crate) struct SessionLoadTestCase {
    pub(crate) description: &'static str,
    /// The table's settings in `[sources.shop.tables."main.orders"]`.
    pub(crate) settings: &'static str,
    /// SQL changing the source's `main.orders` before the second load.
    pub(crate) change: &'static str,
    /// The loaded table's rows after the second load, as `id:name`, by id.
    pub(crate) expected_rows: &'static [&'static str],
}

pub(crate) struct SchemaChangeTestCase {
    pub(crate) description: &'static str,
    /// The table's settings, its `on_schema_change` among them.
    pub(crate) settings: &'static str,
    /// Whether the second load, after the source gains a column, loads the table.
    pub(crate) expected_loaded: bool,
    /// The loaded table's data columns after it, in order.
    pub(crate) expected_columns: &'static [&'static str],
}

pub(crate) struct RefusedCommitTestCase {
    pub(crate) description: &'static str,
    pub(crate) settings: &'static str,
    /// SQL changing the source before the second load, whose `COMMIT` the session refuses.
    pub(crate) change: &'static str,
    /// The statement the session sends right after the refused `COMMIT`.
    pub(crate) expected_next: &'static str,
    /// The loaded table's rows after the refused load, as `id:name`, by id.
    pub(crate) expected_rows: &'static [&'static str],
}

pub(crate) struct SessionRouteTestCase {
    pub(crate) description: &'static str,
    pub(crate) warehouse: Warehouse,
    /// How each call the session gets starts, in order (the writer's folder written `<folder>`).
    pub(crate) expected_calls: &'static [&'static str],
}

pub(crate) struct DescribedTypeTestCase {
    pub(crate) description: &'static str,
    /// The type as `INFORMATION_SCHEMA.COLUMNS` writes it.
    pub(crate) written: &'static str,
    /// The portable type read from it, as `Debug` prints it.
    pub(crate) expected_type: &'static str,
}

pub(crate) struct DescribeQueryTestCase {
    pub(crate) description: &'static str,
    pub(crate) warehouse: Warehouse,
    pub(crate) schema: &'static str,
    pub(crate) table: &'static str,
    /// Text the query must hold: the schema and table as literals.
    pub(crate) expected_parts: &'static [&'static str],
}

pub(crate) struct AttachedNameTestCase {
    pub(crate) description: &'static str,
    /// The project's TOML; the caller attaches a connection named `caller`.
    pub(crate) project: &'static str,
    /// Why the project does not parse; `None` when it does.
    pub(crate) expected_problem: Option<&'static str>,
}

pub(crate) struct AutocommitTestCase {
    pub(crate) description: &'static str,
    pub(crate) warehouse: Warehouse,
    /// What the session says its autocommit is, if asked.
    pub(crate) autocommit: Option<&'static str>,
    /// Why the session is refused; `None` when it opens.
    pub(crate) expected_refusal: Option<&'static str>,
}

pub(crate) struct ReferenceCheckTestCase {
    pub(crate) description: &'static str,
    /// The foreign key referencing the table: its `ON DELETE` action (`confdeltype`), whether it is deferred, and whether the table references itself.
    pub(crate) reference: (&'static str, bool, bool),
    /// Whether the write is a `replace` (else a `delete_insert`).
    pub(crate) replace: bool,
    /// What the check refuses, and what it warns of, each as a part of its line.
    pub(crate) expected_problem: Option<&'static str>,
    pub(crate) expected_warning: Option<&'static str>,
}
