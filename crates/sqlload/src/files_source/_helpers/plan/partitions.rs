use std::sync::Arc;

use arrow_array::{Array, ArrayRef, StringArray};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_cast::{CastOptions, cast_with_options};

use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::models::ColumnType;
use crate::file_store::models::StoredFile;
use crate::files_source::constants::{
    NORMAL_DATE, NORMAL_TIME, NORMAL_TIMESTAMP, NORMAL_TIMESTAMP_TZ,
};
use crate::files_source::models::{DataFile, PathTemplate, PathValues, UnmatchedPaths};
use crate::store_paths::main::hive_values::hive_values;
use crate::store_paths::main::unescape_partition_value::unescape_partition_value;

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
            let raw = unescape_partition_value(group.map_or("", |group| group.as_str()));
            let value = normalised(&raw, column_type).map_err(|reason| {
                format!(
                    "{relative_key}: path column {name} is {raw:?}, which is not a {column_type}: {reason}"
                )
            })?;
            Ok((name.clone(), value))
        })
        .collect();
    Some(values)
}

/// `value` converted to `column_type` as SQLLoad's reader converts it, and printed back in one form every destination casts to that same value (`true`, `7`, `2026-11-08`, a timestamp in UTC); text, JSON, UUID, binary and types with no Arrow form are kept as written.
pub(crate) fn normalised(value: &str, column_type: &ColumnType) -> Result<String, String> {
    let Some(field) = arrow_field("value", column_type) else {
        return Ok(value.to_owned());
    };
    let source: ArrayRef = Arc::new(StringArray::from(vec![value]));
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    if matches!(column_type, ColumnType::Array(_)) {
        return Err("an array cannot come from a path or be a constant".to_owned());
    }
    let cast = cast_with_options(&source, field.data_type(), &strict)
        .map_err(|error| error.to_string())?;
    if cast.is_null(0) {
        return Err("it converts to NULL".to_owned());
    }
    if matches!(
        column_type,
        ColumnType::Text
            | ColumnType::Varchar(_)
            | ColumnType::Json
            | ColumnType::Uuid
            | ColumnType::Binary
    ) {
        return Ok(value.to_owned());
    }
    let options = FormatOptions::default()
        .with_timestamp_format(Some(NORMAL_TIMESTAMP))
        .with_timestamp_tz_format(Some(NORMAL_TIMESTAMP_TZ))
        .with_date_format(Some(NORMAL_DATE))
        .with_time_format(Some(NORMAL_TIME));
    let formatter =
        ArrayFormatter::try_new(cast.as_ref(), &options).map_err(|error| error.to_string())?;
    Ok(formatter.value(0).to_string())
}
