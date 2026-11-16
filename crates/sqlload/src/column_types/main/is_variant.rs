//! Whether a column is DuckDB's `VARIANT`.

use crate::column_types::constants::VARIANT_TYPE;
use crate::column_types::models::ColumnType;

/// Whether `column_type` is `native("VARIANT")`, in any case: DuckDB's `VARIANT`, whose values SQLLoad loads as JSON.
pub fn is_variant(column_type: &ColumnType) -> bool {
    matches!(column_type, ColumnType::Native(name) if name.eq_ignore_ascii_case(VARIANT_TYPE))
}
