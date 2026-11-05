//! `COPY` a files source's files straight from the external stage.

use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::snowflake::_helpers::dialect::json_values::{json_file_format, json_value};
use crate::adapters::snowflake::_helpers::dialect::values::partition_text;
use crate::adapters::snowflake::constants::{
    COPY_OPTIONS, MAX_COPY_SQL_BYTES, MAX_FILES_PER_COPY, PARQUET_COPY_FORMAT,
    UNEXPECTED_FILE_MARKER,
};
use crate::adapters::snowflake::models::SnowflakeSql;
use crate::adapters::snowflake::models::{CopyBatch, CopyColumn, CopyValue, DirectCopy};
use crate::column_types::models::{ColumnType, Warehouse};
use crate::files_source::models::{DataFile, SourceFormat, StoredRead};

/// `COPY` statements loading `request.files` into `request.target`, at most 1,000 files and about 900 KiB of SQL each: data columns read by name and cast (JSON parsed), partition values, source file, version per file (matched on `METADATA$FILENAME`, which on an external stage is the bucket-relative key, failing on any other file) and row number, and literals; always `FORCE = TRUE` and `ON_ERROR = ABORT_STATEMENT`.
pub(in crate::adapters) fn direct_copies(
    request: &DirectCopy<'_>,
) -> Result<Vec<CopyBatch>, String> {
    let folder = request
        .source
        .prefix
        .strip_prefix(&request.stage_url.prefix)
        .ok_or_else(|| {
            format!(
                "{} is not under the stage url {}",
                request.source, request.stage_url
            )
        })?;
    let mut batches: Vec<CopyBatch> = Vec::new();
    for chunk in request.files.chunks(MAX_FILES_PER_COPY) {
        let mut pending: Vec<&[DataFile]> = vec![chunk];
        while let Some(files) = pending.pop() {
            let sql = copy_sql(request, folder, files)?;
            match (sql.len() > MAX_COPY_SQL_BYTES, files.len()) {
                (true, count) if count > 1 => {
                    let (first, second) = files.split_at(count / 2);
                    pending.push(second);
                    pending.push(first);
                }
                _ => batches.push(CopyBatch {
                    sql,
                    keys: files
                        .iter()
                        .map(|data| data.file.relative_key.clone())
                        .collect(),
                }),
            }
        }
    }
    Ok(batches)
}

/// One `COPY` of `files`, which lie in `folder` below the stage URL.
fn copy_sql(request: &DirectCopy<'_>, folder: &str, files: &[DataFile]) -> Result<String, String> {
    let snowflake = Warehouse::Snowflake;
    let literal = |text: &str| sql_adapter(snowflake).render_loader_value_literal(text);
    let matches: Vec<String> = files
        .iter()
        .map(|data| {
            format!(
                "METADATA$FILENAME = {}",
                literal(&format!(
                    "{}{}",
                    request.source.prefix, data.file.relative_key
                ))
            )
        })
        .collect();
    let mut names: Vec<String> = Vec::new();
    let mut values: Vec<String> = Vec::new();
    for column in request.columns {
        let sql_type = sql_adapter(snowflake)
            .render_loader_logical_type(&column.column_type)
            .sql;
        let per_file = |value: &dyn Fn(&DataFile) -> Result<String, String>,
                        otherwise: String|
         -> Result<String, String> {
            let arms: Vec<String> = files
                .iter()
                .zip(&matches)
                .map(|(data, matched)| Ok(format!("WHEN {matched} THEN {}", value(data)?)))
                .collect::<Result<_, String>>()?;
            Ok(format!("CASE {} ELSE {otherwise} END", arms.join(" ")))
        };
        let value = match &column.value {
            CopyValue::File { .. } if request.encoding.format != SourceFormat::Parquet => {
                json_value(
                    (&column.name, &format!("GET($1, {})", literal(&column.name))),
                    (&column.column_type, &sql_type),
                    request.inferred.contains(&column.name),
                )
            }
            CopyValue::File { .. } if column.column_type == ColumnType::Json => format!(
                "PARSE_JSON(CAST(GET($1, {}) AS VARCHAR))",
                literal(&column.name)
            ),
            CopyValue::File { stored, short, .. } => file_value(
                &format!("GET($1, {})", literal(&column.name)),
                (&column.column_type, &sql_type),
                (*stored, *short),
            ),
            CopyValue::SourceRow => "METADATA$FILE_ROW_NUMBER".to_owned(),
            CopyValue::Literal(expression) => expression.clone(),
            CopyValue::SourceFile => per_file(
                &|data| {
                    Ok(literal(&format!(
                        "{}{}",
                        request.source, data.file.relative_key
                    )))
                },
                format!(
                    "TO_VARCHAR(TO_NUMBER({} || METADATA$FILENAME))",
                    literal(UNEXPECTED_FILE_MARKER)
                ),
            )?,
            CopyValue::SourceVersion => {
                per_file(&|data| Ok(literal(&data.file.version())), "NULL".to_owned())?
            }
            CopyValue::Partition => per_file(
                &|data| partition_value(data, column, &sql_type),
                "NULL".to_owned(),
            )?,
        };
        names.push(sql_adapter(snowflake).render_identifier(&column.name));
        values.push(value);
    }
    let keys: Vec<String> = files
        .iter()
        .map(|data| literal(&data.file.relative_key))
        .collect();
    let file_format = match request.encoding.format {
        SourceFormat::Parquet => PARQUET_COPY_FORMAT.to_owned(),
        _ => format!(
            "FILE_FORMAT = ({})",
            json_file_format(request.encoding.compression)
        ),
    };
    Ok(format!(
        "COPY INTO {} ({}) FROM (SELECT {} FROM {}/{folder}) FILES = ({}) {file_format} {COPY_OPTIONS}",
        sql_adapter(snowflake).render_qualified_name(request.target),
        names.join(", "),
        values.join(", "),
        request.stage,
        keys.join(", ")
    ))
}

/// The file's value of partition column `column`, cast to its type; `NULL` when its path has none.
fn partition_value(data: &DataFile, column: &CopyColumn, sql_type: &str) -> Result<String, String> {
    let Some((_, raw)) = data
        .partitions
        .iter()
        .find(|(name, _)| *name == column.name)
    else {
        return Ok("NULL".to_owned());
    };
    let text = partition_text(raw, &column.column_type).map_err(|reason| {
        format!(
            "partition column {} of {}: {reason}",
            column.name, data.file.relative_key
        )
    })?;
    Ok(format!(
        "CAST({} AS {sql_type})",
        SnowflakeSql.render_loader_value_literal(&text)
    ))
}

/// The value of file column `get`, as `column_type` (`sql_type`): a bare integer converted from its unit, and a time or timestamp cut to microseconds toward zero where the live column is `short`, as SQLLoad's own reader does (a cast to fewer digits rounds down instead, which differs before 1970).
fn file_value(
    get: &str,
    (column_type, sql_type): (&ColumnType, &str),
    (stored, short): (StoredRead, bool),
) -> String {
    let read = match (stored, column_type) {
        (StoredRead::Epoch(scale), ColumnType::Date) => {
            format!("TO_DATE(TO_TIMESTAMP_NTZ({get}::NUMBER, {scale}))")
        }
        (StoredRead::Epoch(scale), ColumnType::TimestampTz) => {
            format!("TO_TIMESTAMP_TZ({get}::NUMBER, {scale})")
        }
        (StoredRead::Epoch(scale), _) => format!("TO_TIMESTAMP_NTZ({get}::NUMBER, {scale})"),
        _ => get.to_owned(),
    };
    let value = format!("CAST({read} AS {sql_type})");
    match (short, column_type) {
        (true, ColumnType::TimestampTz) => {
            format!("TO_TIMESTAMP_TZ(TRUNC(DATE_PART(EPOCH_NANOSECOND, {value}) / 1000), 6)")
        }
        (true, ColumnType::TimestampNtz) => {
            format!("TO_TIMESTAMP_NTZ(TRUNC(DATE_PART(EPOCH_NANOSECOND, {value}) / 1000), 6)")
        }
        (true, ColumnType::Time) => {
            format!("TIMEADD(NANOSECOND, -MOD(DATE_PART(NANOSECOND, {value}), 1000), {value})")
        }
        _ => value,
    }
}
