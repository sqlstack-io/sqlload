use arrow_schema::{DataType, SchemaRef, TimeUnit};
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::basic::{
    ConvertedType, LogicalType, TimeUnit as ParquetTimeUnit, Type as PhysicalType,
};
use parquet::schema::types::Type as ParquetType;

use crate::column_types::main::portable_type::portable_type;
use crate::column_types::models::ColumnType;
use crate::files_source::constants::{LOADER_COLUMNS, MAX_DIRECT_DECIMAL_PRECISION};
use crate::files_source::models::{FileColumn, StoredRead};

/// A column seen in sampled files: its type, or `None` while every sampled value was NULL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SampledColumn {
    pub(crate) name: String,
    pub(crate) column_type: Option<Result<ColumnType, String>>,
    /// How the column is held when a direct `COPY` does not load it exactly.
    pub(crate) direct_refusal: Option<String>,
    pub(crate) stored_read: StoredRead,
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
            stored_read: arrow_read(field.data_type()),
        })
        .collect()
}

/// The top-level field `name` of a Parquet schema.
fn stored_field<'a>(
    fields: &'a [std::sync::Arc<ParquetType>],
    name: &str,
) -> Option<&'a ParquetType> {
    fields
        .iter()
        .find(|field| field.name() == name)
        .map(AsRef::as_ref)
}

/// The columns of a Parquet footer, each checked for the direct route against what the file itself stores as well as its Arrow type: plan samples these, and each batch's check reads the same.
pub(crate) fn parquet_columns(footer: &ArrowReaderMetadata) -> Vec<SampledColumn> {
    let stored = footer
        .metadata()
        .file_metadata()
        .schema_descr()
        .root_schema()
        .get_fields();
    footer
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let (direct_refusal, stored_read) =
                match stored_read(field.data_type(), stored_field(stored, field.name())) {
                    Ok(read) => (direct_refusal(field.data_type()), read),
                    Err(refusal) => (Some(refusal), StoredRead::Plain),
                };
            SampledColumn {
                name: field.name().clone(),
                column_type: Some(portable_type(field)),
                direct_refusal,
                stored_read,
            }
        })
        .collect()
}

/// How a stage read returns a column of Arrow type `data_type`, from the type alone (nanoseconds or not).
pub(crate) fn arrow_read(data_type: &DataType) -> StoredRead {
    match data_type {
        DataType::Timestamp(TimeUnit::Nanosecond, _) | DataType::Time64(TimeUnit::Nanosecond) => {
            StoredRead::Nanos
        }
        DataType::Dictionary(_, value) => arrow_read(value),
        _ => StoredRead::Plain,
    }
}

/// How a stage read returns column `stored`, which an embedded Arrow schema reads as `data_type`, or why a direct `COPY` cannot load it exactly (decision 96; live, 2026-11-05). With `USE_LOGICAL_TYPE`, every timestamp, time and date the file annotates reads exactly, nanoseconds and INT96 included (INT96 only where SQLLoad reads it at nanoseconds too: no embedded type, or one of nanoseconds); a bare INT64 under an Arrow timestamp or `Date64` is converted from its Arrow unit; any other column without the logical type its Arrow type needs is refused.
pub(crate) fn stored_read(
    data_type: &DataType,
    stored: Option<&ParquetType>,
) -> Result<StoredRead, String> {
    let logical = stored.and_then(|column| column.get_basic_info().logical_type_ref());
    let physical = stored
        .filter(|column| {
            column.is_primitive() && column.get_basic_info().converted_type() == ConvertedType::NONE
        })
        .map(ParquetType::get_physical_type);
    let read = match (data_type, logical, physical) {
        (DataType::Dictionary(_, value), _, _) => return stored_read(value, stored),
        (DataType::Timestamp(..), Some(LogicalType::Timestamp(timestamp)), _)
        | (DataType::Time32(_) | DataType::Time64(_), Some(LogicalType::Time(timestamp)), _) => {
            match timestamp.unit {
                ParquetTimeUnit::NANOS => Some(StoredRead::Nanos),
                _ => Some(StoredRead::Plain),
            }
        }
        (DataType::Timestamp(TimeUnit::Nanosecond, _), None, Some(PhysicalType::INT96)) => {
            Some(StoredRead::Nanos)
        }
        (DataType::Timestamp(unit, _), None, Some(PhysicalType::INT64)) => {
            Some(StoredRead::Epoch(match unit {
                TimeUnit::Second => 0,
                TimeUnit::Millisecond => 3,
                TimeUnit::Microsecond => 6,
                TimeUnit::Nanosecond => 9,
            }))
        }
        (DataType::Date64, None, Some(PhysicalType::INT64)) => Some(StoredRead::Epoch(3)),
        (DataType::Date32 | DataType::Date64, Some(LogicalType::Date), _) => {
            Some(StoredRead::Plain)
        }
        (
            DataType::Decimal32(..)
            | DataType::Decimal64(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..),
            Some(LogicalType::Decimal { .. }),
            _,
        ) => Some(StoredRead::Plain),
        (
            DataType::Timestamp(..)
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Date32
            | DataType::Date64
            | DataType::Decimal32(..)
            | DataType::Decimal64(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..),
            _,
            _,
        ) => None,
        _ => Some(StoredRead::Plain),
    };
    read.ok_or_else(|| {
        let held = match (logical, physical) {
            (Some(logical), _) => format!("{logical:?}"),
            (None, Some(physical)) => physical.to_string(),
            (None, None) => match stored {
                Some(column) if column.is_primitive() => {
                    column.get_basic_info().converted_type().to_string()
                }
                _ => "nothing".to_owned(),
            },
        };
        format!("{data_type} stored as {held}")
    })
}

/// `data_type` as text unless a direct `COPY` is known to load it exactly (decision 96; live, 2026-11-05): `Float16` makes Snowflake reject the whole file, a decimal over 38 digits does not fit, and nested and other types not listed have no portable type yet.
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
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(..) => true,
        DataType::Decimal32(precision, _)
        | DataType::Decimal64(precision, _)
        | DataType::Decimal128(precision, _)
        | DataType::Decimal256(precision, _) => *precision <= MAX_DIRECT_DECIMAL_PRECISION,
        DataType::Dictionary(_, value) => return direct_refusal(value),
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
            existing.direct_refusal = match joined_read(existing.stored_read, column.stored_read) {
                Some(_) => column.direct_refusal,
                None => Some(format!(
                    "{} stored as a bare integer in some files only (in {} or newer ones)",
                    column.name, merge.key
                )),
            };
        }
        existing.stored_read =
            joined_read(existing.stored_read, column.stored_read).unwrap_or(existing.stored_read);
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

/// How a column read two ways in different files is read in all of them: nanoseconds where any file has them (a cast keeps shorter values whole), and `None` when only some files hold bare integers, which take their own conversion.
pub(crate) fn joined_read(seen: StoredRead, found: StoredRead) -> Option<StoredRead> {
    match (seen, found) {
        (seen, found) if seen == found => Some(seen),
        (StoredRead::Epoch(_), _) | (_, StoredRead::Epoch(_)) => None,
        _ => Some(StoredRead::Nanos),
    }
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
            stored_read: column.stored_read,
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
            stored_read: column.stored_read,
        })
        .collect()
}

fn describe(column_type: &Result<ColumnType, String>) -> String {
    match column_type {
        Ok(column_type) => column_type.to_string(),
        Err(_) => "unsupported".to_owned(),
    }
}
