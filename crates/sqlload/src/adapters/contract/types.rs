//! The warehouse adapter contract, after sqlbuild's `StrictAdapter` and `BaseAdapter`: the shared rendering is the default, and each warehouse overrides what its SQL does differently.

use crate::adapters::contract::_helpers::changes::change_statements;
use arrow_array::RecordBatch;
use chrono::NaiveDateTime;

use crate::adapters::contract::_helpers::identifiers::{
    exact_identifier, qualified, source_expression_cast, staging_type, string_literal,
};
use crate::adapters::contract::_helpers::logged::{commit_statements, logged_statements};
use crate::adapters::contract::_helpers::promote::{
    drop_and_rename, merge_statement, swap_statements,
};
use crate::adapters::contract::_helpers::queries::{
    check_query, child_tables_query, cursor_query, insert_rows, loaded_files_query,
    staging_tables_query, staging_time, stale_staging,
};
use crate::adapters::contract::_helpers::recreate::recreate_statements;
use crate::adapters::contract::_helpers::schema::{
    alter_statements, build_table, create_schema, create_table, staging_changes, staging_name,
    staging_table,
};
use crate::adapters::contract::_helpers::type_fit::type_fit;
use crate::adapters::contract::_helpers::write::write_statements;
use crate::adapters::contract::main::_dropped_ordinals::dropped_ordinals;
use crate::adapters::contract::main::_rekeyed_row_ids::rekeyed_row_ids;
use crate::adapters::contract::models::{
    ChangeApply, ChangeWrite, CheckQuery, CursorQuery, CursorRead, CursorValue, TableName,
    WriteStatementsRequest, WriteTarget,
};
use crate::column_types::models::{ColumnType, LiveColumnType, TypeFit, Warehouse, WarehouseType};
use crate::loader::errors::LoaderError;
use crate::loader::models::{TableLayout, TableSpec};
use crate::schema_plan::models::{ColumnChange, DesiredColumn, LiveColumn};

/// What a warehouse's SQL looks like. Pure rendering, with no I/O; names follow sqlbuild's adapter contract.
pub trait SqlAdapter: Sync + std::fmt::Debug {
    /// The warehouse this adapter renders for.
    fn warehouse(&self) -> Warehouse;

    /// Whether the destination runs SQL; a files target writes Parquet and alters nothing in place.
    fn runs_sql(&self) -> bool {
        true
    }

    /// Whether a `postgres_cdc` source applying its changes by `apply` may load into the destination: every SQL warehouse its changes have been applied to live; a files target only as a changelog, having no tables to merge into.
    fn captures_changes(&self, apply: ChangeApply) -> bool {
        self.runs_sql() || apply == ChangeApply::Changelog
    }

    /// Whether the warehouse's writer has been taught to write the child tables `unnest` makes, beside their parent.
    fn writes_child_tables(&self) -> bool {
        false
    }

    /// The statements deleting published table `table`'s rows matching `condition` (a merge clearing the items of the roots it replaces from a child table): one `DELETE`. `scratch` names the tables a warehouse that publishes by swaps builds beside it.
    fn render_clear_rows(
        &self,
        table: &TableName,
        condition: &str,
        _scratch: &TableName,
    ) -> Vec<String> {
        vec![format!(
            "DELETE FROM {} WHERE {condition}",
            self.render_qualified_name(table)
        )]
    }

    /// The statement deleting staging table `staging`'s rows matching `condition`.
    fn render_delete_staged(&self, staging: &TableName, condition: &str) -> String {
        format!(
            "DELETE FROM {} WHERE {condition}",
            self.render_qualified_name(staging)
        )
    }

    /// The longest table or column name, in characters, that dlt's naming gives on the warehouse (the shorter of dlt's table and column limits), past which it shortens a name with a tag; `None` where it gives no limit.
    fn max_identifier_length(&self) -> Option<usize> {
        None
    }

    /// Whether SQLLoad never drops or recreates the warehouse's tables and never alters one it did not create: `replace` empties and refills the table in one transaction instead of swapping a new one in, `--reload` is refused, and an existing table without `_sqlload_load_id` is loaded as it is.
    fn keeps_tables(&self) -> bool {
        false
    }

    /// Whether a `delete_insert` read in windows or cursor-ordered batches is staged whole and published in one commit at the end, not one commit per batch: each publish here rewrites the partitions it touches, so batches sharing a partition rewrite it again each time, and readers would see the run half published between commits.
    fn publishes_ranges_once(&self) -> bool {
        false
    }

    /// Whether a column SQLLoad keeps as it is (see [`keeps_tables`](SqlAdapter::keeps_tables)), of type `live`, takes every `desired` value exactly, or refuses one that does not fit rather than rounding it.
    fn assigns_exactly(&self, desired: &ColumnType, live: &ColumnType) -> bool {
        self.type_fit(desired, live) == TypeFit::Fits
    }

    /// Whether a `--reload` build table starts `LIKE` the table to keep its grants, and so keeps the live columns.
    fn builds_like_target(&self) -> bool {
        false
    }

    /// Whether SQLLoad's reader keeps nanosecond times and timestamps whole for the warehouse, instead of cutting them to microseconds.
    fn keeps_nanoseconds(&self) -> bool {
        true
    }

    /// How the warehouse spells `column_type` in DDL, and what is lost if it cannot hold it exactly.
    fn render_loader_logical_type(&self, column_type: &ColumnType) -> WarehouseType;

    /// Why the warehouse cannot load a column of `column_type` yet, if it cannot.
    fn type_problem(&self, _column_type: &ColumnType) -> Option<&'static str> {
        None
    }

    /// The portable type of a live column the information schema reports; `native(...)` when it has none.
    fn live_type(&self, live: &LiveColumnType) -> ColumnType {
        ColumnType::Native(live.data_type.clone())
    }

    /// Whether the warehouse changes a `live` column to `target` without rewriting the table.
    fn widens_in_place(&self, _live: &ColumnType, _target: &ColumnType) -> bool {
        false
    }

    /// Whether `live` holds every `desired` value, can be widened to, or cannot.
    fn type_fit(&self, desired: &ColumnType, live: &ColumnType) -> TypeFit {
        type_fit(self, desired, live)
    }

    /// `name` as the warehouse stores it and reports it in its information schema.
    fn stored_name(&self, name: &str) -> String {
        name.to_owned()
    }

    /// Whether the live column `live` is the one stored as `stored`.
    fn is_stored_as(&self, live: &str, stored: &str) -> bool {
        live.eq_ignore_ascii_case(stored)
    }

    /// `name` quoted, as the warehouse stores it.
    fn render_identifier(&self, name: &str) -> String {
        exact_identifier(&self.stored_name(name))
    }

    /// A live name, quoted exactly as the warehouse reports it.
    fn render_exact_identifier(&self, name: &str) -> String {
        exact_identifier(name)
    }

    /// A fully qualified table name.
    fn render_qualified_name(&self, table: &TableName) -> String {
        qualified(self, table)
    }

    /// `text` as a SQL string literal.
    fn render_loader_value_literal(&self, text: &str) -> String {
        string_literal(text)
    }

    /// A numeric cursor's bound `value`, a plain decimal number, as a source filter compares it with a column of `column_type`: bare.
    fn render_cursor_number(&self, value: &str, _column_type: &ColumnType) -> String {
        value.to_owned()
    }

    /// How `plan` names the warehouse reading files itself: the verb for reading them in place, and the step loading them from a stage.
    fn render_read_words(&self) -> (&'static str, &'static str) {
        ("reads", "reads them")
    }

    /// `SCHEMA.TABLE` as `plan` prints it, the way the warehouse stores the names.
    fn render_display_name(&self, table: &TableName) -> String {
        format!("{}.{}", table.schema, table.table)
    }

    /// The type a staging column has: what the loaded files or batches carry (JSON and UUIDs as text).
    fn render_staging_type(&self, column_type: &ColumnType) -> String {
        staging_type(self, column_type)
    }

    /// The expression turning staging column `name` into the table's type.
    fn render_source_expression_cast(&self, name: &str, column_type: &ColumnType) -> String {
        source_expression_cast(self, name, column_type)
    }

    /// `value`, a merge key of `column_type`, as a window can be partitioned by it.
    fn render_partition_key(&self, value: &str, _column_type: &ColumnType) -> String {
        value.to_owned()
    }

    /// `expression` as text.
    fn render_text_cast(&self, expression: &str) -> String {
        format!("CAST({expression} AS VARCHAR)")
    }

    /// The query `check` runs on `relation`, returning its values as text.
    fn render_check_query(&self, relation: &TableName, check: CheckQuery<'_>) -> String {
        check_query(self, relation, check)
    }

    /// A query returning a row when `table` holds one.
    fn render_any_row_query(&self, table: &TableName) -> String {
        format!(
            "SELECT 'x' FROM {} LIMIT 1",
            self.render_qualified_name(table)
        )
    }

    /// `CREATE SCHEMA IF NOT EXISTS` for `table`'s schema.
    fn render_create_schema(&self, table: &TableName) -> String {
        create_schema(self, table)
    }

    /// `CREATE TABLE IF NOT EXISTS` with every column nullable.
    fn render_create_table(&self, table: &TableName, columns: &[DesiredColumn]) -> String {
        create_table(self, table, columns)
    }

    /// Problems with a table's layout settings (`engine`, `order_by`, `partition_by`, `primary_key`, `table_settings`), which only ClickHouse takes.
    fn layout_problems(&self, layout: &TableLayout) -> Vec<String> {
        match layout.is_empty() {
            true => Vec::new(),
            false => vec![format!(
                "engine, order_by, partition_by, primary_key and table_settings lay out ClickHouse tables; {} tables take none",
                self.warehouse()
            )],
        }
    }

    /// `CREATE TABLE IF NOT EXISTS` for a loaded table: the table `spec` names with `columns`, laid out as it says where the warehouse takes a layout.
    fn render_create_loaded_table(&self, spec: &TableSpec, columns: &[DesiredColumn]) -> String {
        self.render_create_table(&spec.destination, columns)
    }

    /// `COMMENT ON TABLE`, giving `table` its description; none for a table without one.
    fn render_table_comment(&self, table: &TableName, description: &str) -> Option<String> {
        (!description.is_empty()).then(|| {
            format!(
                "COMMENT ON TABLE {} IS {}",
                self.render_qualified_name(table),
                self.render_loader_value_literal(description)
            )
        })
    }

    /// The statement creating the unique index a `merge` on `unique_key` needs, for a table SQLLoad creates; none where `merge` needs no index.
    fn render_key_index(&self, _table: &TableName, _unique_key: &[String]) -> Option<String> {
        None
    }

    /// The table kind a load's staging table is created as.
    fn staging_table_kind(&self) -> &'static str {
        "TABLE"
    }

    /// The staging or build table for `table` under `staging_id`.
    fn staging_relation_name(&self, table: &TableName, staging_id: &str) -> TableName {
        staging_name(table, staging_id)
    }

    /// The staging table for `table` under `staging_id`, and the statement creating it with `columns` (the `_sqlload_*` metadata included, which the staged batches carry).
    fn render_create_staging_table(
        &self,
        table: &TableName,
        columns: &[DesiredColumn],
        staging_id: &str,
    ) -> (TableName, String) {
        staging_table(self, table, columns, staging_id)
    }

    /// The statements creating `build` in `table`'s own shape, so batches land in their final form, what the destination added stays, and the swap copies nothing: from the table's `definition` by default. A `--reload` passes the `differing` live columns, as it rebuilds with the current columns.
    fn render_create_build_table(
        &self,
        build: &TableName,
        table: &TableName,
        shape: (&[DesiredColumn], Option<&[LiveColumn]>, Option<&str>),
    ) -> Vec<String> {
        build_table(self, build, table, shape)
    }

    /// The statement changing `column` of `target` to `sql_type` in place.
    fn render_alter_column_type(&self, target: &str, column: &str, sql_type: &str) -> String {
        format!("ALTER TABLE {target} ALTER COLUMN {column} TYPE {sql_type}")
    }

    /// The statements applying `changes` to `table`, whose current columns are `live`.
    fn render_change_columns(
        &self,
        table: &TableName,
        changes: &[ColumnChange],
        live: &[LiveColumn],
    ) -> Vec<String> {
        alter_statements(self, table, changes, live)
    }

    /// The statement rewriting `target` from its own rows through `select`, in place and keeping its grants.
    fn render_rebuild_table(&self, target: &str, select: &str) -> String {
        format!("CREATE OR REPLACE TABLE {target} COPY GRANTS AS SELECT {select} FROM {target}")
    }

    /// The statement emptying staging table `staging` for the next batches.
    fn render_empty_staging(&self, staging: &TableName) -> String {
        format!("DELETE FROM {}", self.render_qualified_name(staging))
    }

    /// Whether a staging table's changed types are cast by rewriting it rather than altered in place.
    fn widens_staging_by_rebuild(&self) -> bool {
        false
    }

    /// The statements changing `staging`, made for `old` columns, to hold `new` ones; none when nothing changed. A build table (`built`, the table's live columns it started with) holds the table's own types, and those live columns stay.
    fn render_staging_changes(
        &self,
        staging: &TableName,
        columns: (&[DesiredColumn], &[DesiredColumn]),
        built: Option<&[LiveColumn]>,
    ) -> Vec<String> {
        staging_changes(self, staging, columns, built)
    }

    /// Promote the finished build table in place of `target` with no row copied (sqlbuild's staged promotion): by default the old table is dropped and the new one renamed in one transaction, then `indexes` are re-created.
    fn render_swap(
        &self,
        target: &TableName,
        build: &TableName,
        indexes: &[String],
    ) -> Vec<String> {
        drop_and_rename(self, target, build, indexes)
    }

    /// Under a merge that dedupes, a query of the ordinal (as text) of each row of `staging` the merge of `request` drops, so the rows hanging off them (child tables' rows) are dropped too; `None` without `dedupe`.
    fn render_dropped_ordinals(
        &self,
        request: &WriteStatementsRequest<'_>,
        staging: &TableName,
    ) -> Option<String> {
        dropped_ordinals(self, request, staging)
    }

    /// Under a merge, a query of the `row_id` of each row of the table that a row of `staging` shares its key with but whose `row_id` differs (an id not derived from the key, as dlt's `delete-insert` merge gives), so the rows hanging off it can be replaced too; `None` without a merge.
    fn render_rekeyed_row_ids(
        &self,
        request: &WriteStatementsRequest<'_>,
        staging: &TableName,
        row_id: &str,
    ) -> Option<String> {
        rekeyed_row_ids(self, request, staging, row_id)
    }

    /// One `MERGE`: key matches update every other column, the rest are inserted.
    fn render_merge(
        &self,
        request: &WriteStatementsRequest<'_>,
        unique_key: &[String],
        source: &str,
        values: &[String],
    ) -> String {
        merge_statement(self, request, unique_key, (source, values))
    }

    /// `replace` from rows that arrived as text: build a copy beside the table, then swap it in.
    fn render_replace(
        &self,
        request: &WriteStatementsRequest<'_>,
        staging: &TableName,
        insert_from: &str,
    ) -> Vec<String> {
        swap_statements(self, request, staging, insert_from)
    }

    /// Rebuild the table from `source` in one statement, so readers never see it empty (`--reload`).
    fn render_replace_table_from_relation(
        &self,
        request: &WriteStatementsRequest<'_>,
        source: &str,
        values: &[String],
    ) -> Vec<String> {
        recreate_statements(self, request, source, values)
    }

    /// Statements applying a batch of captured changes from `staging`, guarded by `_sqlload_lsn` so a replay changes nothing; in one transaction where the warehouse has them.
    fn render_changes(
        &self,
        request: &WriteStatementsRequest<'_>,
        change: &ChangeWrite,
        staging: &TableName,
    ) -> Vec<String> {
        change_statements(self, request, change, staging)
    }

    /// Atomic statements applying `request.mode` from `staging`, which holds every column, the `_sqlload_*` metadata included; the caller drops `staging` and its swap table.
    fn render_write(
        &self,
        request: &WriteStatementsRequest<'_>,
        staging: &TableName,
    ) -> Vec<String> {
        write_statements(self, request, staging)
    }

    /// Whether a `merge` batch's truncate is a `WHEN NOT MATCHED BY SOURCE` clause of its `MERGE` rather than a statement before it, so a warehouse that commits every statement alone still applies the batch in one commit.
    fn truncates_in_merge(&self) -> bool {
        false
    }

    /// Whether a files source may `merge` into this warehouse, file by file with a run record in each file's commit (tested on DuckDB and Postgres so far).
    fn merges_files(&self) -> bool {
        false
    }

    /// Whether `statement`, run alone, commits by itself, so a run record cannot share its transaction.
    fn commits_alone(&self, _statement: &str) -> bool {
        false
    }

    /// `statements` with `insert` inside their transaction where the warehouse allows it, so the record commits with the data; otherwise after them.
    fn render_logged(&self, statements: Vec<String>, insert: String) -> Vec<String> {
        logged_statements(self, statements, insert)
    }

    /// The write of `request` from `staging`, with a logged table's run record insert and (unless the staging table is swapped in or rebuilt from at the end) the emptying of the staging table for the next batches.
    fn render_commit(
        &self,
        request: &WriteStatementsRequest<'_>,
        staging: &TableName,
        record: (Option<String>, bool),
    ) -> Vec<String> {
        commit_statements(self, request, staging, record)
    }

    /// `INSERT` statements adding `rows` (each value as text, in `columns`' order) to staging table `table`, in statements of bounded size, each value cast to its column's type.
    fn render_insert_rows(
        &self,
        table: &TableName,
        columns: &[DesiredColumn],
        rows: &[Vec<Option<String>>],
    ) -> Vec<String> {
        insert_rows(self, table, columns, rows)
    }

    /// The text of a cursor's maximum `column`, read as `value`, independent of session time zones and output formats.
    fn render_cursor_text(&self, column: &str, value: CursorValue) -> String {
        match value {
            CursorValue::EpochMicros { .. } => format!("CAST(epoch_us(MAX({column})) AS VARCHAR)"),
            CursorValue::Date => format!("strftime(MAX({column}), '%Y-%m-%d')"),
            CursorValue::Plain => format!("CAST(MAX({column}) AS VARCHAR)"),
        }
    }

    /// Text `expression` compared bytewise (in code point order), whatever collation its column or database has, as the source's cursor filter and SQLLoad itself compare text cursors.
    fn render_binary_text(&self, expression: &str) -> String {
        expression.to_owned()
    }

    /// A query for a cursor's maximum and how to read it; `None` if a lookback needs a time column.
    fn render_max_cursor(&self, request: &CursorQuery<'_>) -> Option<CursorRead> {
        cursor_query(self, request)
    }

    /// `SELECT DISTINCT file || '|' || version`; without a version column, just the file names.
    fn render_loaded_files_query(
        &self,
        table: &TableName,
        file_column: &str,
        version_column: Option<&str>,
    ) -> String {
        loaded_files_query(self, table, file_column, version_column)
    }

    /// The condition choosing staging tables a crashed load may have left, by name (`_` matches any character).
    fn render_staging_filter(&self, suffix: &str) -> String {
        format!(
            "table_name ILIKE {}",
            self.render_loader_value_literal(&format!("%{suffix}%"))
        )
    }

    /// The relation listing the tables of `table`'s schema, with `table_schema` and `table_name` columns.
    fn render_schema_tables(&self, _table: &TableName) -> String {
        "information_schema.tables".to_owned()
    }

    /// The query listing the tables named as `parent`'s child tables are, in its schema.
    fn list_child_tables_sql(&self, parent: &TableName) -> String {
        child_tables_query(self, (parent, &self.render_schema_tables(parent)))
    }

    /// The query listing staging tables a crashed load may have left in `table`'s schema; `database` is the connection's, for warehouses that need one.
    fn list_staging_relations_sql(&self, table: &TableName, database: Option<&str>) -> String {
        staging_tables_query(self, table, database)
    }

    /// Whether the listed staging table `name` is a crashed load's: by default, the time in its name is before `before`.
    fn is_stale_staging(&self, name: &str, before: NaiveDateTime) -> bool {
        staging_time(name).is_some_and(|time| time < before)
    }

    /// `DROP` statements for the stale staging tables among `found`, in `schema_of`'s schema.
    fn render_stale_staging(
        &self,
        schema_of: &TableName,
        found: &[String],
        before: NaiveDateTime,
    ) -> Vec<String> {
        stale_staging(self, schema_of, found, before)
    }

    /// `CREATE TABLE IF NOT EXISTS` for a run log of `columns`, and a fallback to try when the warehouse refuses the first.
    fn render_create_run_log_table_sql(
        &self,
        runs: &TableName,
        columns: &[DesiredColumn],
    ) -> (String, Option<String>) {
        (self.render_create_table(runs, columns), None)
    }
}

/// One table's write session on its destination: how its batches get in. The loader keeps the commit cycle (staging and build tables, commit points, run records); the writer owns the transport.
pub trait TableWriter {
    /// Whether an unlogged `append` may write batches straight into the table rather than through a staging table.
    fn writes_directly(&self) -> bool;

    /// Add one batch, the load's metadata included, where `target`'s batches go.
    fn write_batch(
        &mut self,
        target: &WriteTarget<'_>,
        batch: RecordBatch,
    ) -> impl Future<Output = Result<(), LoaderError>> + Send;

    /// The bytes of files the batch has staged so far, where it goes through files; `None` when its Arrow bytes measure it.
    fn staged_bytes(&self) -> Option<usize>;

    /// Stage what the batch still holds in memory, so every row it was given is in a file or sent.
    fn finish_files(
        &mut self,
        target: &WriteTarget<'_>,
    ) -> impl Future<Output = Result<(), LoaderError>> + Send;

    /// Load a batch that has rows where it goes: `COPY` its staged files, or commit its open transaction.
    fn load_batch(
        &mut self,
        target: &WriteTarget<'_>,
    ) -> impl Future<Output = Result<(), LoaderError>> + Send;

    /// Start the next batch, with nothing staged.
    fn next_batch(&mut self);

    /// Wait until every row sent without files is in the staging table.
    fn settle_rows(&mut self) -> impl Future<Output = Result<(), LoaderError>> + Send;

    /// Open the row channel again on its next use, after a schema change replaced the staging table.
    fn reopen_rows(&mut self);

    /// Roll back a batch's open transaction, before its staging table is dropped; best effort.
    fn rollback_batch(&mut self) -> impl Future<Output = ()> + Send;

    /// Remove the staged files of a batch that did not load, unless the stage keeps them; best effort.
    fn remove_batch_files(&mut self, target: &WriteTarget<'_>) -> impl Future<Output = ()> + Send;
}
