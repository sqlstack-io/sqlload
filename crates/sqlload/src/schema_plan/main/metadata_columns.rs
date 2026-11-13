//! The columns a load adds to every row it writes.

use crate::column_types::models::ColumnType;
use crate::load_naming::main::metadata_name::metadata_name;
use crate::load_naming::models::{MetadataRole, Naming};
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};

/// The load id and load time columns under `naming`, then the row id when the table gives its rows one, appended after the data columns.
pub fn metadata_columns(naming: Naming, row_id: bool) -> Vec<DesiredColumn> {
    [
        (MetadataRole::LoadId, ColumnType::Text, true),
        (MetadataRole::LoadedAt, ColumnType::TimestampTz, true),
        (MetadataRole::RowId, ColumnType::Text, row_id),
    ]
    .into_iter()
    .filter(|(_, _, added)| *added)
    .map(|(role, column_type, _)| DesiredColumn {
        name: metadata_name(naming, role).to_owned(),
        column_type,
        origin: ColumnOrigin::Metadata,
    })
    .collect()
}
