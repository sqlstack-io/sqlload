//! Databricks SQL, where it differs from the contract's shared rendering.

use crate::adapters::contract::constants::STAGING_SUFFIX;
use crate::adapters::contract::models::{CursorValue, TableName, WriteStatementsRequest};
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::databricks::_helpers::dialect::type_names::{
    logical_type, portable_type, widens_in_place,
};
use crate::adapters::databricks::_helpers::dialect::write::{change_statements, write_statements};
use crate::adapters::databricks::models::DatabricksSql;
use crate::column_types::models::{ColumnType, LiveColumnType, Warehouse, WarehouseType};
use crate::schema_plan::models::{ColumnChange, LiveColumn};

impl SqlAdapter for DatabricksSql {
    fn warehouse(&self) -> Warehouse {
        Warehouse::Databricks
    }

    /// Not yet: a later release applies changes to Databricks once a live round has run them.
    fn captures_changes(&self) -> bool {
        false
    }

    fn keeps_nanoseconds(&self) -> bool {
        false
    }

    fn render_loader_logical_type(&self, column_type: &ColumnType) -> WarehouseType {
        logical_type(column_type)
    }

    fn live_type(&self, live: &LiveColumnType) -> ColumnType {
        portable_type(live).unwrap_or_else(|| ColumnType::Native(live.data_type.clone()))
    }

    fn widens_in_place(&self, live: &ColumnType, target: &ColumnType) -> bool {
        widens_in_place(live, target)
    }

    fn render_exact_identifier(&self, name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
    }

    fn render_identifier(&self, name: &str) -> String {
        self.render_exact_identifier(&self.stored_name(name))
    }

    /// Backslash escapes: Databricks reads `\` as an escape, and `'a''b'` as two literals joined.
    fn render_loader_value_literal(&self, text: &str) -> String {
        format!("'{}'", text.replace('\\', "\\\\").replace('\'', "\\'"))
    }

    fn render_read_words(&self) -> (&'static str, &'static str) {
        ("COPYs", "COPY INTO")
    }

    fn render_display_name(&self, table: &TableName) -> String {
        format!("{}.{}", table.schema, table.table).to_ascii_lowercase()
    }

    fn render_source_expression_cast(&self, name: &str, column_type: &ColumnType) -> String {
        let column = self.render_identifier(name);
        match column_type {
            ColumnType::Json => format!("parse_json({column})"),
            _ => column,
        }
    }

    fn render_rebuild_table(&self, target: &str, select: &str) -> String {
        format!("CREATE OR REPLACE TABLE {target} AS SELECT {select} FROM {target}")
    }

    fn render_change_columns(
        &self,
        table: &TableName,
        changes: &[ColumnChange],
        live: &[LiveColumn],
    ) -> Vec<String> {
        change_statements(self, table, changes, live)
    }

    fn widens_staging_by_rebuild(&self) -> bool {
        true
    }

    /// Delta has no swap: the table is replaced by the build table's rows in one commit (keeping its identity, history and grants), so the rows are copied once.
    fn render_swap(
        &self,
        target: &TableName,
        build: &TableName,
        _indexes: &[String],
    ) -> Vec<String> {
        let (old, new) = (
            self.render_qualified_name(target),
            self.render_qualified_name(build),
        );
        vec![
            format!("CREATE OR REPLACE TABLE {old} AS SELECT * FROM {new}"),
            format!("DROP TABLE IF EXISTS {new}"),
        ]
    }

    fn render_write(
        &self,
        request: &WriteStatementsRequest<'_>,
        staging: &TableName,
    ) -> Vec<String> {
        write_statements(self, request, staging)
    }

    fn commits_alone(&self, _statement: &str) -> bool {
        true
    }

    fn render_cursor_text(&self, column: &str, value: CursorValue) -> String {
        match value {
            CursorValue::EpochMicros { zoned: true } => {
                format!("CAST(unix_micros(MAX({column})) AS STRING)")
            }
            CursorValue::EpochMicros { zoned: false } => format!(
                "CAST(timestampdiff(MICROSECOND, TIMESTAMP_NTZ'1970-01-01 00:00:00', MAX({column})) AS STRING)"
            ),
            CursorValue::Date => format!("date_format(MAX({column}), 'yyyy-MM-dd')"),
            CursorValue::Plain => format!("CAST(MAX({column}) AS STRING)"),
        }
    }

    /// Unity Catalog reports schema names lower-cased.
    fn list_staging_relations_sql(&self, table: &TableName, database: Option<&str>) -> String {
        let catalog = table
            .database
            .as_deref()
            .or(database)
            .map(|name| format!("{}.", self.render_identifier(name)))
            .unwrap_or_default();
        format!(
            "SELECT table_name FROM {catalog}information_schema.tables WHERE table_schema = {} AND {}",
            self.render_loader_value_literal(&table.schema.to_ascii_lowercase()),
            self.render_staging_filter(&format!("{STAGING_SUFFIX}_"))
        )
    }
}
