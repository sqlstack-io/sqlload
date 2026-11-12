//! The columns a load adds to every row it writes.

use crate::column_types::models::ColumnType;
use crate::load_naming::main::metadata_name::metadata_name;
use crate::load_naming::models::{MetadataRole, Naming};
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};

/// The load id and load time columns under `naming`, appended after the data columns.
pub fn metadata_columns(naming: Naming) -> Vec<DesiredColumn> {
    vec![
        DesiredColumn {
            name: metadata_name(naming, MetadataRole::LoadId).to_owned(),
            column_type: ColumnType::Text,
            origin: ColumnOrigin::Metadata,
        },
        DesiredColumn {
            name: metadata_name(naming, MetadataRole::LoadedAt).to_owned(),
            column_type: ColumnType::TimestampTz,
            origin: ColumnOrigin::Metadata,
        },
    ]
}
