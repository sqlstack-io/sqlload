use std::collections::BTreeMap;

use chrono::NaiveDate;

use crate::adapters::files::constants::DATA_SUFFIXES;
use crate::adapters::files::models::{Partition, PartitionBy, PartitionKey, TableFiles, TablePath};
use crate::column_types::models::ColumnType;
use crate::file_store::models::StoredFile;

fn date(text: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .map_err(|_| format!("{text:?} is not a date (YYYY-MM-DD)"))
}

/// Partition `value` in comparable form, as `by` and the cursor's type read it.
pub(crate) fn partition_key(
    value: &str,
    by: PartitionBy,
    cursor_type: &ColumnType,
) -> Result<PartitionKey, String> {
    match (by, cursor_type) {
        (PartitionBy::Date, _) | (PartitionBy::Value, ColumnType::Date) => {
            Ok(PartitionKey::Date(date(value)?))
        }
        (PartitionBy::Value, ColumnType::SmallInt | ColumnType::Int) => value
            .parse::<i64>()
            .map(PartitionKey::Int)
            .map_err(|_| format!("{value:?} is not an integer")),
        (PartitionBy::Value, ColumnType::Text | ColumnType::Varchar(_)) => {
            Ok(PartitionKey::Text(value.to_owned()))
        }
        (PartitionBy::Value, other) => Err(format!(
            "a {{cursor}} partition needs an int, date or text cursor, not {other}; use {{cursor:date}} for timestamps"
        )),
    }
}

/// Cursor bound `bound` in the same comparable form as the partitions (timestamp bounds by their date).
pub(crate) fn bound_key(
    bound: &str,
    by: PartitionBy,
    cursor_type: &ColumnType,
) -> Result<PartitionKey, String> {
    match by {
        PartitionBy::Date => Ok(PartitionKey::Date(date(bound.get(..10).unwrap_or(bound))?)),
        PartitionBy::Value => partition_key(bound, by, cursor_type),
    }
}

/// `files` (listed below the table folder) grouped into partitions, oldest first; files outside a partition folder are problems.
pub(crate) fn group_partitions(
    files: Vec<StoredFile>,
    path: &TablePath,
    cursor_type: Option<&ColumnType>,
) -> TableFiles {
    let data: Vec<StoredFile> = files
        .into_iter()
        .filter(|file| is_data_file(&file.relative_key))
        .collect();
    let (Some(partition), Some(cursor_type)) = (&path.partition, cursor_type) else {
        let partitions = match data.is_empty() {
            true => Vec::new(),
            false => vec![Partition {
                value: None,
                files: data,
            }],
        };
        return TableFiles {
            partitions,
            problems: Vec::new(),
        };
    };
    let mut problems: Vec<String> = Vec::new();
    let mut grouped: BTreeMap<PartitionKey, Partition> = BTreeMap::new();
    for file in data {
        let folder = file.relative_key.split_once('/').map(|(folder, _)| folder);
        let Some(value) = folder.and_then(|folder| partition.value_of(folder)) else {
            problems.push(format!(
                "{} is not in a partition folder {partition}",
                file.relative_key
            ));
            continue;
        };
        match partition_key(&value, partition.by, cursor_type) {
            Ok(key) => grouped
                .entry(key)
                .or_insert_with(|| Partition {
                    value: Some(value.clone()),
                    files: Vec::new(),
                })
                .files
                .push(file),
            Err(problem) => problems.push(format!("{}: {problem}", file.relative_key)),
        }
    }
    TableFiles {
        partitions: grouped.into_values().collect(),
        problems,
    }
}

/// Whether `key` names a data file a files target writes, in any format.
pub(crate) fn is_data_file(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    DATA_SUFFIXES.iter().any(|suffix| lower.ends_with(suffix))
}
