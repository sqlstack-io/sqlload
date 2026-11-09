//! BigQuery GoogleSQL, where it differs from the contract's shared rendering.

use crate::adapters::bigquery::_helpers::dialect::type_names::{
    logical_type, portable_type, widens_in_place,
};
use crate::adapters::bigquery::_helpers::dialect::write::write_statements;
use crate::adapters::bigquery::models::BigQuerySql;
use crate::adapters::contract::constants::STAGING_SUFFIX;
use crate::adapters::contract::models::{
    ChangeApply, CursorValue, TableName, WriteStatementsRequest,
};
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::{ColumnType, LiveColumnType, Warehouse, WarehouseType};

impl SqlAdapter for BigQuerySql {
    fn warehouse(&self) -> Warehouse {
        Warehouse::BigQuery
    }

    /// Not yet: a later release applies changes to BigQuery once a live round has run them.
    fn captures_changes(&self, _apply: ChangeApply) -> bool {
        false
    }

    fn keeps_nanoseconds(&self) -> bool {
        false
    }

    fn render_loader_logical_type(&self, column_type: &ColumnType) -> WarehouseType {
        logical_type(column_type)
    }

    /// The Write API refuses arrays of zoned timestamps.
    fn type_problem(&self, column_type: &ColumnType) -> Option<&'static str> {
        (*column_type == ColumnType::Array(Box::new(ColumnType::TimestampTz))).then_some(
            "BigQuery cannot load array(timestamp_tz) yet, as its write API refuses arrays of zoned timestamps; pin array(timestamp_ntz), which holds UTC times, or json",
        )
    }

    fn live_type(&self, live: &LiveColumnType) -> ColumnType {
        portable_type(live).unwrap_or_else(|| ColumnType::Native(live.data_type.clone()))
    }

    fn widens_in_place(&self, live: &ColumnType, target: &ColumnType) -> bool {
        widens_in_place(live, target)
    }

    /// Backticks, with `\` and `` ` `` escaped by a backslash, as GoogleSQL reads quoted identifiers.
    fn render_exact_identifier(&self, name: &str) -> String {
        format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
    }

    fn render_identifier(&self, name: &str) -> String {
        self.render_exact_identifier(&self.stored_name(name))
    }

    /// Backslash escapes: GoogleSQL has no `''` escape inside a string literal, and a quoted string holds no raw line break.
    fn render_loader_value_literal(&self, text: &str) -> String {
        format!(
            "'{}'",
            text.replace('\\', "\\\\")
                .replace('\'', "\\'")
                .replace('\n', "\\n")
                .replace('\r', "\\r")
        )
    }

    /// A decimal bound as a `BIGNUMERIC`, which `NUMERIC` and `BIGNUMERIC` columns compare with exactly; a bare decimal is a `FLOAT64` literal, which rounds.
    fn render_cursor_number(&self, value: &str, column_type: &ColumnType) -> String {
        match column_type {
            ColumnType::Decimal { .. } => format!("CAST('{value}' AS BIGNUMERIC)"),
            _ => value.to_owned(),
        }
    }

    /// JSON is parsed from its staged text; a UUID is stored as the text it arrived as.
    fn render_source_expression_cast(&self, name: &str, column_type: &ColumnType) -> String {
        let column = self.render_identifier(name);
        match column_type {
            ColumnType::Json => format!("PARSE_JSON({column})"),
            _ => column,
        }
    }

    /// BigQuery has no `COMMENT ON`: a table's description is one of its options.
    fn render_table_comment(&self, table: &TableName, description: &str) -> Option<String> {
        (!description.is_empty()).then(|| {
            format!(
                "ALTER TABLE {} SET OPTIONS (description = {})",
                self.render_qualified_name(table),
                self.render_loader_value_literal(description)
            )
        })
    }

    fn render_alter_column_type(&self, target: &str, column: &str, sql_type: &str) -> String {
        format!("ALTER TABLE {target} ALTER COLUMN {column} SET DATA TYPE {sql_type}")
    }

    fn render_rebuild_table(&self, target: &str, select: &str) -> String {
        format!("CREATE OR REPLACE TABLE {target} AS SELECT {select} FROM {target}")
    }

    /// BigQuery's `DELETE` needs a `WHERE`.
    fn render_empty_staging(&self, staging: &TableName) -> String {
        format!(
            "DELETE FROM {} WHERE TRUE",
            self.render_qualified_name(staging)
        )
    }

    /// BigQuery has no rename into place: the table is replaced by the build table's rows in one statement, so the rows are copied once. The build table carries a load's own name, so no write stream ever meets a table recreated under the same name.
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

    fn render_text_cast(&self, expression: &str) -> String {
        format!("CAST({expression} AS STRING)")
    }

    fn render_cursor_text(&self, column: &str, value: CursorValue) -> String {
        match value {
            CursorValue::EpochMicros { zoned: true } => {
                format!("CAST(UNIX_MICROS(MAX({column})) AS STRING)")
            }
            CursorValue::EpochMicros { zoned: false } => {
                format!("CAST(UNIX_MICROS(TIMESTAMP(MAX({column}), 'UTC')) AS STRING)")
            }
            CursorValue::Date => format!("FORMAT_DATE('%F', MAX({column}))"),
            CursorValue::Plain => format!("CAST(MAX({column}) AS STRING)"),
        }
    }

    /// BigQuery has no `ILIKE`.
    fn render_staging_filter(&self, suffix: &str) -> String {
        format!(
            "LOWER(table_name) LIKE {}",
            self.render_loader_value_literal(&format!("%{}%", suffix.to_ascii_lowercase()))
        )
    }

    /// A dataset's own `INFORMATION_SCHEMA`, qualified by its project.
    fn list_staging_relations_sql(&self, table: &TableName, database: Option<&str>) -> String {
        let project = table
            .database
            .as_deref()
            .or(database)
            .map(|name| format!("{}.", self.render_identifier(name)))
            .unwrap_or_default();
        format!(
            "SELECT table_name FROM {project}{}.INFORMATION_SCHEMA.TABLES WHERE {}",
            self.render_identifier(&table.schema),
            self.render_staging_filter(&format!("{STAGING_SUFFIX}_"))
        )
    }
}
