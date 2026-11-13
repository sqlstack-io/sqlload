//! DuckDB's SQL: the contract's shared rendering, with DuckDB's types.

use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::duckdb::_helpers::type_names::{logical_type, portable_type};
use crate::adapters::duckdb::models::DuckDbSql;
use crate::column_types::models::{ColumnType, LiveColumnType, Warehouse, WarehouseType};

impl SqlAdapter for DuckDbSql {
    fn merges_files(&self) -> bool {
        true
    }

    fn writes_child_tables(&self) -> bool {
        true
    }

    fn warehouse(&self) -> Warehouse {
        Warehouse::DuckDb
    }

    fn max_identifier_length(&self) -> Option<usize> {
        Some(65536)
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
