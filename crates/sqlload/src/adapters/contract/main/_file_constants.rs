//! The values SQLLoad gives every row of one file a destination reads itself.

use crate::adapters::contract::models::DirectBatch;
use crate::files_source::constants::{SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN};
use crate::files_source::main::normal_value::normal_value;
use crate::files_source::models::DataFile;
use crate::loader::errors::LoaderError;
use crate::schema_plan::models::DesiredColumn;

/// The values SQLLoad gives every row of `data`: its name and version as the files source records them, and its partition values (constants among them), each converted to its column's type as SQLLoad's reader would, so a value that does not convert fails here rather than in the destination's cast.
pub(in crate::adapters) fn file_constants(
    batch: &DirectBatch<'_>,
    data: &DataFile,
    columns: &[DesiredColumn],
) -> Result<Vec<(String, Option<String>)>, LoaderError> {
    let Some(source) = batch.spec.files.as_ref() else {
        return Err(LoaderError::Invalid("not a files table".to_owned()));
    };
    let mut constants = vec![
        (
            SOURCE_FILE_COLUMN.to_owned(),
            Some(format!("{}{}", source.location, data.file.relative_key)),
        ),
        (SOURCE_VERSION_COLUMN.to_owned(), Some(data.file.version())),
    ];
    for (name, _) in &source.partition_types {
        let value = data
            .partitions
            .iter()
            .find(|(partition, _)| partition == name)
            .map(|(_, value)| value.clone());
        let loaded = batch.spec.source_table.loaded_as(name);
        let column_type = columns
            .iter()
            .find(|column| column.name == loaded)
            .map(|column| &column.column_type);
        let value = match (value, column_type) {
            (Some(value), Some(column_type)) => {
                Some(normal_value(&value, column_type).map_err(|reason| {
                    LoaderError::Invalid(format!(
                        "{}: {name} = {value:?} does not load as {column_type}: {reason}",
                        data.file.relative_key
                    ))
                })?)
            }
            (value, _) => value,
        };
        constants.push((loaded.to_owned(), value));
    }
    Ok(constants)
}
