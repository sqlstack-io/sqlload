//! The `COPY INTO` reading a batch's staged files.

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

use crate::adapters::contract::models::WriteTarget;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::databricks::constants::COPY_OPTIONS;
use crate::adapters::databricks::models::DatabricksSql;
use crate::column_types::models::ColumnType;

/// `COPY INTO` the batch's table (the staging or build table, or the table itself) from `folder`, each column cast to the type it lands as: the table's own types straight into it or a build table, JSON parsed; text for JSON and UUIDs in a staging table.
pub(crate) fn copy_statement(target: &WriteTarget<'_>, folder: &str) -> String {
    let adapter = DatabricksSql;
    let table = target.staging.unwrap_or(target.table);
    let final_types = target.direct || target.builds;
    let selected: Vec<String> = target
        .columns
        .iter()
        .map(|column| {
            let name = adapter.render_identifier(&column.name);
            match (final_types, &column.column_type) {
                (true, ColumnType::Json) => format!("parse_json({name}) AS {name}"),
                (true, column_type) => format!(
                    "CAST({name} AS {}) AS {name}",
                    adapter.render_loader_logical_type(column_type).sql
                ),
                (false, column_type) => format!(
                    "CAST({name} AS {}) AS {name}",
                    adapter.render_staging_type(column_type)
                ),
            }
        })
        .collect();
    format!(
        "COPY INTO {} FROM (SELECT {} FROM {}) {COPY_OPTIONS}",
        adapter.render_qualified_name(table),
        selected.join(", "),
        adapter.render_loader_value_literal(&format!("{folder}/"))
    )
}

/// `batch` with its time-of-day columns as text, since Databricks reads no Parquet `TIME` and stores times as text.
pub(crate) fn readable_batch(batch: RecordBatch) -> Result<RecordBatch, String> {
    let schema = batch.schema();
    let has_time = schema
        .fields()
        .iter()
        .any(|field| matches!(field.data_type(), DataType::Time32(_) | DataType::Time64(_)));
    if !has_time {
        return Ok(batch);
    }
    let mut fields: Vec<Field> = Vec::with_capacity(schema.fields().len());
    let mut columns = Vec::with_capacity(schema.fields().len());
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        match field.data_type() {
            DataType::Time32(_) | DataType::Time64(_) => {
                columns.push(
                    arrow_cast::cast(column, &DataType::Utf8).map_err(|error| error.to_string())?,
                );
                fields.push(field.as_ref().clone().with_data_type(DataType::Utf8));
            }
            _ => {
                columns.push(column.clone());
                fields.push(field.as_ref().clone());
            }
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|error| error.to_string())
}
