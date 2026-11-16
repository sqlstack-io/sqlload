//! The columns a load adds to every row it writes.

use crate::column_types::models::ColumnType;
use crate::load_naming::main::metadata_name::metadata_name;
use crate::load_naming::models::{Compat, MetadataRole};
use crate::schema_plan::constants::CDC_DELETED_AT_COLUMN;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};

/// The load id and load time columns under `compat`, then the row id when the table gives its rows one, then `_sqlload_deleted_at` when it soft-deletes rows (`soft_deletes`), appended after the data columns.
pub fn metadata_columns(
    compat: Compat,
    (row_id, soft_deletes): (bool, bool),
) -> Vec<DesiredColumn> {
    [
        (MetadataRole::LoadId, ColumnType::Text, true),
        (MetadataRole::LoadedAt, ColumnType::TimestampTz, true),
        (MetadataRole::RowId, ColumnType::Text, row_id),
    ]
    .into_iter()
    .filter(|(_, _, added)| *added)
    .map(|(role, column_type, _)| DesiredColumn {
        name: metadata_name(compat, role).to_owned(),
        column_type,
        origin: ColumnOrigin::Metadata,
    })
    .chain(soft_deletes.then(|| DesiredColumn {
        name: CDC_DELETED_AT_COLUMN.to_owned(),
        column_type: ColumnType::TimestampTz,
        origin: ColumnOrigin::Metadata,
    }))
    .collect()
}
