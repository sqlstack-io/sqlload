//! Parquet files in the contract: they plan with Arrow's types, and run no SQL.

use std::borrow::Cow;

use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::files::models::ParquetFiles;
use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::models::{ColumnType, Warehouse, WarehouseType};

impl SqlAdapter for ParquetFiles {
    fn warehouse(&self) -> Warehouse {
        Warehouse::Parquet
    }

    fn runs_sql(&self) -> bool {
        false
    }

    fn render_loader_logical_type(&self, column_type: &ColumnType) -> WarehouseType {
        match arrow_field("", column_type) {
            Some(field) => WarehouseType {
                sql: field.data_type().to_string(),
                lossy: None,
            },
            None => WarehouseType {
                sql: column_type.to_string(),
                lossy: Some(Cow::Borrowed(
                    "a native type has no Arrow type, so Parquet and CSV files cannot hold it",
                )),
            },
        }
    }

    /// A type with no Arrow type, which only `native(...)` and arrays of it are; a pinned one fails `plan` (path and constant columns still only carry the loss note).
    fn type_problem(&self, column_type: &ColumnType) -> Option<&'static str> {
        arrow_field("", column_type).is_none().then_some(
            "a native type has no Arrow type, so Parquet and CSV files cannot hold it; pin a portable type",
        )
    }
}
