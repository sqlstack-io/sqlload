//! Map a column type to one warehouse's SQL type.

use crate::column_types::_helpers::warehouse_names::for_warehouse;
use crate::column_types::models::{ColumnType, Warehouse, WarehouseType};

/// How `warehouse` spells `column_type` in DDL, and what is lost if it cannot hold it exactly.
pub fn warehouse_type(column_type: &ColumnType, warehouse: Warehouse) -> WarehouseType {
    for_warehouse(warehouse)(column_type)
}
