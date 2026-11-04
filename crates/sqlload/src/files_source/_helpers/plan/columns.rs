use arrow_schema::SchemaRef;

use crate::column_types::main::portable_type::portable_type;
use crate::column_types::models::ColumnType;
use crate::files_source::models::FileColumn;

/// A column seen in sampled files: its type, or `None` while every sampled value was NULL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SampledColumn {
    pub(crate) name: String,
    pub(crate) column_type: Option<Result<ColumnType, String>>,
}

/// The columns of a Parquet schema.
pub(crate) fn schema_columns(schema: &SchemaRef) -> Vec<SampledColumn> {
    schema
        .fields()
        .iter()
        .map(|field| SampledColumn {
            name: field.name().clone(),
            column_type: Some(portable_type(field)),
        })
        .collect()
}

/// `columns` plus `found`'s new columns (except `replaced` ones, whose values come from the path), and a problem for each column whose type differs from an earlier file's.
pub(crate) fn merge_columns(
    mut columns: Vec<SampledColumn>,
    found: Vec<SampledColumn>,
    key: &str,
    replaced: &[&str],
) -> (Vec<SampledColumn>, Vec<String>) {
    let mut problems: Vec<String> = Vec::new();
    for column in found {
        if replaced.contains(&column.name.as_str()) {
            continue;
        }
        let Some(existing) = columns.iter_mut().find(|seen| seen.name == column.name) else {
            columns.push(column);
            continue;
        };
        match (&existing.column_type, column.column_type) {
            (_, None) => {}
            (None, found_type) => existing.column_type = found_type,
            (Some(seen), Some(found_type)) if *seen == found_type => {}
            (Some(seen), Some(found_type)) => problems.push(format!(
                "column {} is {} in newer files but {} in {key}; pin a type that holds both",
                column.name,
                describe(seen),
                describe(&found_type)
            )),
        }
    }
    (columns, problems)
}

/// `columns` plus `schema`'s new columns (except `replaced` ones), and a problem for each column whose type differs from an earlier file's.
pub(crate) fn merge_schema(
    columns: Vec<FileColumn>,
    schema: &SchemaRef,
    key: &str,
    replaced: &[&str],
) -> (Vec<FileColumn>, Vec<String>) {
    let seen: Vec<SampledColumn> = columns
        .into_iter()
        .map(|column| SampledColumn {
            name: column.name,
            column_type: Some(column.column_type),
        })
        .collect();
    let (merged, problems) = merge_columns(seen, schema_columns(schema), key, replaced);
    (file_columns(merged), problems)
}

/// The sampled columns with their final types: a column that was NULL in every sampled value is text.
pub(crate) fn file_columns(columns: Vec<SampledColumn>) -> Vec<FileColumn> {
    columns
        .into_iter()
        .map(|column| FileColumn {
            name: column.name,
            column_type: column.column_type.unwrap_or(Ok(ColumnType::Text)),
        })
        .collect()
}

fn describe(column_type: &Result<ColumnType, String>) -> String {
    match column_type {
        Ok(column_type) => column_type.to_string(),
        Err(_) => "unsupported".to_owned(),
    }
}
