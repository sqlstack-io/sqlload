use arrow_schema::{DataType, SchemaRef, TimeUnit};

use crate::column_types::main::portable_type::portable_type;
use crate::column_types::models::ColumnType;
use crate::files_source::constants::{LOADER_COLUMNS, MAX_DIRECT_DECIMAL_PRECISION, UTC_ZONES};
use crate::files_source::models::FileColumn;

/// A column seen in sampled files: its type, or `None` while every sampled value was NULL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SampledColumn {
    pub(crate) name: String,
    pub(crate) column_type: Option<Result<ColumnType, String>>,
    /// The file's Arrow type when a direct `COPY` does not load it exactly.
    pub(crate) direct_refusal: Option<String>,
}

/// Whether `name` is one of the loader's own columns, ignoring case.
pub(crate) fn loader_column(name: &str) -> bool {
    LOADER_COLUMNS
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(name))
}

/// `found` without the loader's own columns, and the names of those dropped (as the file spells them).
pub(crate) fn drop_loader_columns(found: Vec<SampledColumn>) -> (Vec<SampledColumn>, Vec<String>) {
    let (dropped, kept): (Vec<SampledColumn>, Vec<SampledColumn>) = found
        .into_iter()
        .partition(|column| loader_column(&column.name));
    (
        kept,
        dropped.into_iter().map(|column| column.name).collect(),
    )
}

/// The columns of a Parquet schema.
pub(crate) fn schema_columns(schema: &SchemaRef) -> Vec<SampledColumn> {
    schema
        .fields()
        .iter()
        .map(|field| SampledColumn {
            name: field.name().clone(),
            column_type: Some(portable_type(field)),
            direct_refusal: direct_refusal(field.data_type()),
        })
        .collect()
}

/// `data_type` as text unless a direct `COPY` is known to load it exactly (decision 96): nanosecond and INT96 timestamps, `Date64`, `UInt64`, `Float16`, `Decimal256`, timestamps in a zone other than UTC and anything else not listed stay with SQLLoad until parity proves them.
pub(crate) fn direct_refusal(data_type: &DataType) -> Option<String> {
    let exact = match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Date32 => true,
        DataType::Decimal128(precision, _) => *precision <= MAX_DIRECT_DECIMAL_PRECISION,
        DataType::Timestamp(
            TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond,
            zone,
        ) => zone.as_deref().is_none_or(|zone| UTC_ZONES.contains(&zone)),
        _ => false,
    };
    (!exact).then(|| data_type.to_string())
}

/// How one file's columns join those of newer files.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ColumnMerge<'a> {
    /// The file, for problems.
    pub(crate) key: &'a str,
    /// Columns whose values come from the path, so the file's own are ignored.
    pub(crate) replaced: &'a [&'a str],
    /// Whether an int column in one file and a float column in another make a float column (JSON and CSV, whose types are inferred) rather than a problem.
    pub(crate) widen_numbers: bool,
}

/// `columns` plus `found`'s new columns, and a problem for each column whose type differs from an earlier file's.
pub(crate) fn merge_columns(
    mut columns: Vec<SampledColumn>,
    found: Vec<SampledColumn>,
    merge: ColumnMerge<'_>,
) -> (Vec<SampledColumn>, Vec<String>) {
    let mut problems: Vec<String> = Vec::new();
    for column in found {
        if merge.replaced.contains(&column.name.as_str()) {
            continue;
        }
        let Some(existing) = columns.iter_mut().find(|seen| seen.name == column.name) else {
            columns.push(column);
            continue;
        };
        if existing.direct_refusal.is_none() {
            existing.direct_refusal = column.direct_refusal;
        }
        match (&existing.column_type, column.column_type) {
            (_, None) => {}
            (None, found_type) => existing.column_type = found_type,
            (Some(seen), Some(found_type)) if *seen == found_type => {}
            (Some(Ok(seen)), Some(Ok(found_type)))
                if merge.widen_numbers && is_number(seen) && is_number(&found_type) =>
            {
                existing.column_type = Some(Ok(ColumnType::Float));
            }
            (Some(seen), Some(found_type)) => problems.push(format!(
                "column {} is {} in newer files but {} in {}; pin a type that holds both",
                column.name,
                describe(seen),
                describe(&found_type),
                merge.key
            )),
        }
    }
    (columns, problems)
}

/// A problem for each pair of columns whose names differ only in case, which most warehouses cannot hold apart.
pub(crate) fn case_problems(columns: &[SampledColumn]) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    for (position, column) in columns.iter().enumerate() {
        let clash = columns[..position]
            .iter()
            .find(|earlier| earlier.name.eq_ignore_ascii_case(&column.name));
        if let Some(earlier) = clash {
            problems.push(format!(
                "columns {} and {} differ only in case, which most warehouses cannot hold apart; rename one in the files",
                earlier.name, column.name
            ));
        }
    }
    problems
}

fn is_number(column_type: &ColumnType) -> bool {
    matches!(column_type, ColumnType::Int | ColumnType::Float)
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
            direct_refusal: column.direct_refusal,
        })
        .collect();
    let merge = ColumnMerge {
        key,
        replaced,
        widen_numbers: false,
    };
    let (merged, problems) = merge_columns(seen, schema_columns(schema), merge);
    (file_columns(merged), problems)
}

/// The sampled columns with their final types: a column that was NULL in every sampled value is text.
pub(crate) fn file_columns(columns: Vec<SampledColumn>) -> Vec<FileColumn> {
    columns
        .into_iter()
        .map(|column| FileColumn {
            name: column.name,
            column_type: column.column_type.unwrap_or(Ok(ColumnType::Text)),
            direct_refusal: column.direct_refusal,
        })
        .collect()
}

fn describe(column_type: &Result<ColumnType, String>) -> String {
    match column_type {
        Ok(column_type) => column_type.to_string(),
        Err(_) => "unsupported".to_owned(),
    }
}
