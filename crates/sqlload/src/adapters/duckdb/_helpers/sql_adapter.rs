//! DuckDB's SQL: the contract's shared rendering, with DuckDB's types.

use crate::adapters::contract::_helpers::queries::staging_tables_query;
use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::duckdb::_helpers::type_names::{logical_type, portable_type};
use crate::adapters::duckdb::models::DuckDbSql;
use crate::column_types::models::{ColumnType, LiveColumnType, Warehouse, WarehouseType};

impl SqlAdapter for DuckDbSql {
    fn warehouse(&self) -> Warehouse {
        Warehouse::DuckDb
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

    /// The current database's staging tables, from its own `information_schema` (DuckDB has no `<catalog>.information_schema`), whatever catalog names the table.
    fn list_staging_relations_sql(&self, table: &TableName, _database: Option<&str>) -> String {
        let table = TableName {
            database: None,
            ..table.clone()
        };
        staging_tables_query(self, &table, None)
    }

    /// Only the current database's staging tables: MotherDuck attaches the account's other databases too.
    fn render_staging_filter(&self, suffix: &str) -> String {
        format!(
            "table_catalog = current_database() AND table_name ILIKE {}",
            self.render_loader_value_literal(&format!("%{suffix}%"))
        )
    }
}
