//! ClickHouse SQL, where it differs from the contract's shared rendering.

use crate::adapters::clickhouse::_helpers::dialect::layout::{layout_clauses, layout_problems};
use crate::adapters::clickhouse::_helpers::dialect::type_names::{logical_type, portable_type};
use crate::adapters::clickhouse::_helpers::dialect::write::{swap_statements, write_statements};
use crate::adapters::clickhouse::constants::EMPTY_SORTING_KEY;
use crate::adapters::clickhouse::models::ClickHouseSql;
use crate::adapters::contract::models::{
    ChangeApply, CursorValue, TableName, WriteStatementsRequest,
};
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::{ColumnType, LiveColumnType, Warehouse, WarehouseType};
use crate::loader::models::{TableLayout, TableSpec};
use crate::schema_plan::models::{DesiredColumn, LiveColumn};

impl ClickHouseSql {
    /// `name type, ...` for `columns`, each typed by `sql_type`.
    fn column_definitions(
        &self,
        columns: &[DesiredColumn],
        sql_type: &dyn Fn(&ColumnType) -> String,
    ) -> String {
        columns
            .iter()
            .map(|column| {
                format!(
                    "{} {}",
                    self.render_identifier(&column.name),
                    sql_type(&column.column_type)
                )
            })
            .collect::<Vec<String>>()
            .join(", ")
    }
}

impl SqlAdapter for ClickHouseSql {
    fn warehouse(&self) -> Warehouse {
        Warehouse::ClickHouse
    }

    fn max_identifier_length(&self) -> Option<usize> {
        Some(255)
    }

    /// Not yet: changes would need merging into partitions batch by batch.
    fn captures_changes(&self, _apply: ChangeApply) -> bool {
        false
    }

    fn keeps_nanoseconds(&self) -> bool {
        false
    }

    fn publishes_ranges_once(&self) -> bool {
        true
    }

    fn render_loader_logical_type(&self, column_type: &ColumnType) -> WarehouseType {
        logical_type(column_type)
    }

    fn live_type(&self, live: &LiveColumnType) -> ColumnType {
        portable_type(live).unwrap_or_else(|| ColumnType::Native(live.data_type.clone()))
    }

    /// `MODIFY COLUMN` converts the column's values in place.
    fn widens_in_place(&self, _live: &ColumnType, _target: &ColumnType) -> bool {
        true
    }

    /// ClickHouse names are case-sensitive.
    fn is_stored_as(&self, live: &str, stored: &str) -> bool {
        live == stored
    }

    /// Backticks, with `\` and `` ` `` escaped by a backslash.
    fn render_exact_identifier(&self, name: &str) -> String {
        format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
    }

    fn render_identifier(&self, name: &str) -> String {
        self.render_exact_identifier(name)
    }

    /// A SQLLoad schema is a ClickHouse database, which has no level above it.
    fn render_qualified_name(&self, table: &TableName) -> String {
        format!(
            "{}.{}",
            self.render_identifier(&table.schema),
            self.render_identifier(&table.table)
        )
    }

    /// ClickHouse reads a backslash in a string literal as an escape.
    fn render_loader_value_literal(&self, text: &str) -> String {
        format!("'{}'", text.replace('\\', "\\\\").replace('\'', "\\'"))
    }

    fn render_create_schema(&self, table: &TableName) -> String {
        format!(
            "CREATE DATABASE IF NOT EXISTS {}",
            self.render_identifier(&table.schema)
        )
    }

    /// An unsorted `MergeTree` table (the run log, for one).
    fn render_create_table(&self, table: &TableName, columns: &[DesiredColumn]) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {} ({}) ENGINE = MergeTree ORDER BY {EMPTY_SORTING_KEY}",
            self.render_qualified_name(table),
            self.column_definitions(columns, &|column_type| {
                self.render_loader_logical_type(column_type).sql
            })
        )
    }

    fn layout_problems(&self, layout: &TableLayout) -> Vec<String> {
        layout_problems(layout)
    }

    fn render_create_loaded_table(&self, spec: &TableSpec, columns: &[DesiredColumn]) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {} ({}) {}",
            self.render_qualified_name(&spec.destination),
            self.column_definitions(columns, &|column_type| {
                self.render_loader_logical_type(column_type).sql
            }),
            layout_clauses(&spec.layout, &spec.unique_key)
        )
    }

    fn render_table_comment(&self, table: &TableName, description: &str) -> Option<String> {
        (!description.is_empty()).then(|| {
            format!(
                "ALTER TABLE {} MODIFY COMMENT {}",
                self.render_qualified_name(table),
                self.render_loader_value_literal(description)
            )
        })
    }

    /// An unsorted `MergeTree` table named for the load, which a crashed load's successor finds by its name; `CREATE OR REPLACE` needs an `Atomic` database.
    fn render_create_staging_table(
        &self,
        table: &TableName,
        columns: &[DesiredColumn],
        staging_id: &str,
    ) -> (TableName, String) {
        let staging = self.staging_relation_name(table, staging_id);
        let create = format!(
            "CREATE TABLE {} ({}) ENGINE = MergeTree ORDER BY {EMPTY_SORTING_KEY}",
            self.render_qualified_name(&staging),
            self.column_definitions(columns, &|column_type| {
                self.render_staging_type(column_type)
            })
        );
        (staging, create)
    }

    /// `AS` the table, so the build table has its engine, keys and settings and `EXCHANGE TABLES` keeps them; a `--reload` then gives the `differing` live columns their planned types, adds new columns and drops columns no longer planned.
    fn render_create_build_table(
        &self,
        build: &TableName,
        table: &TableName,
        (columns, differing, _): (&[DesiredColumn], Option<&[LiveColumn]>, Option<&str>),
    ) -> Vec<String> {
        let name = self.render_qualified_name(build);
        let mut statements = vec![format!(
            "CREATE TABLE {name} AS {}",
            self.render_qualified_name(table)
        )];
        let Some(live) = differing else {
            return statements;
        };
        let sql_type = |column_type: &ColumnType| self.render_loader_logical_type(column_type).sql;
        for column in columns {
            match live.iter().find(|known| known.name == column.name) {
                None => statements.push(format!(
                    "ALTER TABLE {name} ADD COLUMN {} {}",
                    self.render_identifier(&column.name),
                    sql_type(&column.column_type)
                )),
                Some(known) if sql_type(&known.column_type) != sql_type(&column.column_type) => {
                    statements.push(self.render_alter_column_type(
                        &name,
                        &self.render_exact_identifier(&known.name),
                        &sql_type(&column.column_type),
                    ));
                }
                Some(_) => {}
            }
        }
        for known in live {
            if !columns.iter().any(|column| column.name == known.name) {
                statements.push(format!(
                    "ALTER TABLE {name} DROP COLUMN {}",
                    self.render_exact_identifier(&known.name)
                ));
            }
        }
        statements
    }

    fn render_alter_column_type(&self, target: &str, column: &str, sql_type: &str) -> String {
        format!("ALTER TABLE {target} MODIFY COLUMN {column} {sql_type}")
    }

    fn render_empty_staging(&self, staging: &TableName) -> String {
        format!("TRUNCATE TABLE {}", self.render_qualified_name(staging))
    }

    fn render_swap(
        &self,
        target: &TableName,
        build: &TableName,
        _indexes: &[String],
    ) -> Vec<String> {
        swap_statements(self, target, build)
    }

    fn render_write(
        &self,
        request: &WriteStatementsRequest<'_>,
        staging: &TableName,
    ) -> Vec<String> {
        write_statements(self, request, staging)
    }

    /// Every statement commits on its own: ClickHouse has no transactions.
    fn commits_alone(&self, _statement: &str) -> bool {
        true
    }

    /// The stored value itself, which the session's UTC time zone never changes: a wall-clock time arrives as its UTC reading.
    fn render_cursor_text(&self, column: &str, value: CursorValue) -> String {
        match value {
            CursorValue::EpochMicros { .. } => {
                format!("toString(toUnixTimestamp64Micro(toDateTime64(MAX({column}), 6)))")
            }
            CursorValue::Date | CursorValue::Plain => format!("toString(MAX({column}))"),
        }
    }
}
