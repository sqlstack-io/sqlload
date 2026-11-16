//! DuckDB's SQL: the contract's shared rendering, with DuckDB's types.

use crate::adapters::contract::main::_rebuilt_in_place::rebuilt_in_place;
use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::duckdb::_helpers::type_names::{logical_type, portable_type};
use crate::adapters::duckdb::models::DuckDbSql;
use crate::column_types::constants::VARIANT_TYPE;
use crate::column_types::main::is_variant::is_variant;
use crate::column_types::models::{ColumnType, LiveColumnType, Warehouse, WarehouseType};
use crate::schema_plan::models::{ColumnChange, LiveColumn};

impl SqlAdapter for DuckDbSql {
    fn merges_files(&self) -> bool {
        true
    }

    fn warehouse(&self) -> Warehouse {
        Warehouse::DuckDb
    }

    fn memory_note(&self) -> Option<&'static str> {
        Some(
            "DuckDB holds a commit's rows in memory until a row group fills, so a table of few, wide rows peaks at about 4x its largest commit's bytes, plus DuckDB's cache up to its memory_limit",
        )
    }

    fn max_identifier_length(&self) -> Option<usize> {
        Some(65536)
    }

    /// The mark statements of `deletes` and `deletes_from` run in the container tests.
    fn marks_deleted_rows(&self) -> bool {
        true
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

    fn widens_in_place(&self, _live: &ColumnType, _target: &ColumnType) -> bool {
        true
    }

    fn parses_variant_pins(&self) -> bool {
        true
    }

    /// A `VARIANT` column takes its staged text as JSON, so objects, arrays, numbers and booleans keep their types rather than load as strings.
    fn render_source_expression_cast(&self, name: &str, column_type: &ColumnType) -> String {
        let column = self.render_identifier(name);
        match column_type {
            variant if is_variant(variant) => {
                format!("CAST(CAST({column} AS JSON) AS {VARIANT_TYPE})")
            }
            ColumnType::Json | ColumnType::Uuid => format!(
                "CAST({column} AS {})",
                self.render_loader_logical_type(column_type).sql
            ),
            _ => column,
        }
    }

    /// A rebuild (a `json` column turning `VARIANT`, or back) is one more `ALTER COLUMN ... TYPE`: DuckDB rewrites the column itself and keeps the rest of the table.
    fn render_change_columns(
        &self,
        table: &TableName,
        changes: &[ColumnChange],
        live: &[LiveColumn],
    ) -> Vec<String> {
        rebuilt_in_place(self, table, changes, live)
    }

    /// Only the current database's columns: MotherDuck attaches the account's other databases too, whose tables of the same schema and name would count as the target's.
    fn render_schema_columns(&self, _table: &TableName) -> String {
        "(SELECT table_schema, table_name, column_name FROM information_schema.columns WHERE table_catalog = current_database())".to_owned()
    }

    /// Only the current database's staging tables: MotherDuck attaches the account's other databases too.
    fn render_staging_filter(&self, suffix: &str) -> String {
        format!(
            "table_catalog = current_database() AND table_name ILIKE {}",
            self.render_loader_value_literal(&format!("%{suffix}%"))
        )
    }
}
