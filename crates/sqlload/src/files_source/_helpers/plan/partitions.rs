use std::sync::Arc;

use arrow_array::{Array, ArrayRef, StringArray};
use arrow_cast::{CastOptions, cast_with_options};

use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::models::ColumnType;
use crate::file_store::models::StoredFile;
use crate::files_source::models::{DataFile, PathTemplate, PathValues, UnmatchedPaths};
use crate::store_paths::main::hive_values::hive_values;

/// `file` with its values for `values` (Hive folders, `path_columns` captures and constants), and a problem if a partition folder is missing, its key does not match `path_columns`, or a captured value does not convert; `None` for a file skipped because its key does not match.
pub(crate) fn data_file(
    file: StoredFile,
    values: &PathValues,
) -> (Option<DataFile>, Option<String>) {
    let mut partitions: Vec<(String, String)> = hive_values(&file.relative_key);
    partitions.retain(|(key, _)| values.partition_columns.contains(key));
    let found: Vec<&String> = partitions.iter().map(|(key, _)| key).collect();
    let mut problem = values
        .partition_columns
        .iter()
        .find(|column| !found.contains(column))
        .map(|missing| {
            format!(
                "{} has no {missing}=... folder, but {missing} is a partition column",
                file.relative_key
            )
        });
    if let Some(template) = &values.template {
        match captured_values(template, &file.relative_key) {
            Some(Ok(captured)) => partitions.extend(captured),
            Some(Err(reason)) => problem = problem.or(Some(reason)),
            None if values.unmatched == UnmatchedPaths::Skip => return (None, None),
            None => {
                problem = problem.or_else(|| {
                    Some(format!(
                        "{} does not match path_columns {:?}; fix the template, or set unmatched_paths = \"skip\" to leave such files out",
                        file.relative_key, template.text
                    ))
                });
            }
        }
    }
    partitions.extend(values.constants.iter().cloned());
    (Some(DataFile { file, partitions }), problem)
}

/// The values `template` captures from `relative_key`, each converted to its type; `None` when the key does not match.
fn captured_values(
    template: &PathTemplate,
    relative_key: &str,
) -> Option<Result<Vec<(String, String)>, String>> {
    let groups = template.pattern.captures(relative_key)?;
    let values: Result<Vec<(String, String)>, String> = template
        .captures
        .iter()
        .zip(groups.iter().skip(1))
        .map(|((name, column_type), group)| {
            let raw = group.map_or("", |group| group.as_str());
            let value = match (column_type, raw.len()) {
                (ColumnType::Date, 8) => format!("{}-{}-{}", &raw[..4], &raw[4..6], &raw[6..]),
                _ => raw.to_owned(),
            };
            converts(&value, column_type).map_err(|reason| {
                format!(
                    "{relative_key}: path column {name} is {raw:?}, which is not a {column_type}: {reason}"
                )
            })?;
            Ok((name.clone(), value))
        })
        .collect();
    Some(values)
}

/// Whether `value` converts to `column_type` as SQLLoad's reader converts it; a type with no Arrow form is left to the destination.
pub(crate) fn converts(value: &str, column_type: &ColumnType) -> Result<(), String> {
    let Some(field) = arrow_field("value", column_type) else {
        return Ok(());
    };
    let source: ArrayRef = Arc::new(StringArray::from(vec![value]));
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    match cast_with_options(&source, field.data_type(), &strict) {
        Ok(cast) if cast.is_valid(0) => Ok(()),
        Ok(_) => Err("it converts to NULL".to_owned()),
        Err(error) => Err(error.to_string()),
    }
}
