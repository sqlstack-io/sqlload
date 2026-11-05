//! DuckDB's SQL: the contract's shared rendering, with DuckDB's types.

use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::duckdb::_helpers::type_names::{logical_type, portable_type};
use crate::adapters::duckdb::models::DuckDbSql;
use crate::column_types::models::{ColumnType, LiveColumnType, Warehouse, WarehouseType};

impl SqlAdapter for DuckDbSql {
    fn warehouse(&self) -> Warehouse {
        Warehouse::DuckDb
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
}
