//! `COPY` a files source's files straight from the external stage.

use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::snowflake::_helpers::dialect::values::partition_text;
use crate::adapters::snowflake::constants::{
    COPY_OPTIONS, MAX_COPY_SQL_BYTES, MAX_FILES_PER_COPY, PARQUET_COPY_FORMAT,
    UNEXPECTED_FILE_MARKER,
};
use crate::adapters::snowflake::models::SnowflakeSql;
use crate::adapters::snowflake::models::{CopyBatch, CopyColumn, CopyValue, DirectCopy};
use crate::column_types::models::{ColumnType, Warehouse};
use crate::files_source::models::DataFile;

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
            CopyValue::File { .. } if column.column_type == ColumnType::Json => format!(
                "PARSE_JSON(CAST(GET($1, {}) AS VARCHAR))",
                literal(&column.name)
            ),
            CopyValue::File { .. } => {
                format!("CAST(GET($1, {}) AS {sql_type})", literal(&column.name))
            }
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
    Ok(format!(
        "COPY INTO {} ({}) FROM (SELECT {} FROM {}/{folder}) FILES = ({}) {PARQUET_COPY_FORMAT} {COPY_OPTIONS}",
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
