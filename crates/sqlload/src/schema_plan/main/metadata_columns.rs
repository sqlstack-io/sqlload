//! The `_sqlload_*` columns every loaded row carries.

use crate::column_types::models::ColumnType;
use crate::schema_plan::constants::{LOAD_ID_COLUMN, LOADED_AT_COLUMN};
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};

/// `_sqlload_load_id` and `_sqlload_loaded_at`, appended after the data columns.
pub fn metadata_columns() -> Vec<DesiredColumn> {
    vec![
        DesiredColumn {
            name: LOAD_ID_COLUMN.to_owned(),
            column_type: ColumnType::Text,
            origin: ColumnOrigin::Metadata,
        },
        DesiredColumn {
            name: LOADED_AT_COLUMN.to_owned(),
            column_type: ColumnType::TimestampTz,
            origin: ColumnOrigin::Metadata,
        },
    ]
}
