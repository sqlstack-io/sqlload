//! Send an Arrow batch through the MotherDuck bridge as a zstd Parquet file.

use std::sync::Arc;

use arrow_array::RecordBatch;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::arrow_writer::{ArrowWriter, ArrowWriterOptions};
use parquet::basic::{Compression, ConvertedType, LogicalType, Type as PhysicalType, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::{SchemaDescriptor, Type};

use crate::adapters::motherduck_bridge::errors::BridgeError;
use crate::adapters::motherduck_bridge::main::run_statement::run_statement;
use crate::adapters::motherduck_bridge::models::BridgeConnection;

/// Run `statement`, whose one `?` is the path of a zstd Parquet file holding `batch` (such as `COPY t (a) FROM ? (FORMAT parquet)`), on `connection`; the columns named in `json` are Parquet `JSON`, so DuckDB parses them into a `VARIANT` column as `CAST(… AS JSON)` would. The file is a temporary one on this machine, which DuckDB reads there and removed once the statement ends: the bridge's DuckDB is another version than SQLLoad's, so Arrow cannot cross between them in memory.
pub(in crate::adapters) async fn append_batch(
    connection: &BridgeConnection,
    statement: String,
    (batch, json): (RecordBatch, Vec<String>),
) -> Result<(), BridgeError> {
    let file = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let file = tempfile::Builder::new()
            .prefix("sqlload-motherduck-")
            .suffix(".parquet")
            .tempfile()
            .map_err(|error| error.to_string())?;
        let options = ArrowWriterOptions::new()
            .with_properties(
                WriterProperties::builder()
                    .set_compression(Compression::ZSTD(ZstdLevel::default()))
                    .build(),
            )
            .with_parquet_schema(json_schema(&batch, &json).map_err(|error| error.to_string())?);
        let mut writer = ArrowWriter::try_new_with_options(
            file.reopen().map_err(|error| error.to_string())?,
            batch.schema(),
            options,
        )
        .map_err(|error| error.to_string())?;
        writer.write(&batch).map_err(|error| error.to_string())?;
        writer.close().map_err(|error| error.to_string())?;
        Ok(file)
    })
    .await
    .map_err(|error| BridgeError::Setup(error.to_string()))?
    .map_err(|error| {
        BridgeError::Setup(format!(
            "cannot write a batch for the MotherDuck bridge: {error}"
        ))
    })?;
    let path = file.path().display().to_string();
    run_statement(connection, statement, vec![path]).await?;
    drop(file);
    Ok(())
}

/// The Parquet schema of `batch`, with its text columns named in `json` typed `JSON`.
fn json_schema(batch: &RecordBatch, json: &[String]) -> parquet::errors::Result<SchemaDescriptor> {
    let converted = ArrowSchemaConverter::new().convert(&batch.schema())?;
    let root = converted.root_schema();
    let mut fields = Vec::with_capacity(root.get_fields().len());
    for field in root.get_fields() {
        let text = field.is_primitive() && field.get_physical_type() == PhysicalType::BYTE_ARRAY;
        if !(text && json.iter().any(|name| name == field.name())) {
            fields.push(field.clone());
            continue;
        }
        let info = field.get_basic_info();
        fields.push(Arc::new(
            Type::primitive_type_builder(field.name(), PhysicalType::BYTE_ARRAY)
                .with_repetition(info.repetition())
                .with_logical_type(Some(LogicalType::Json))
                .with_converted_type(ConvertedType::JSON)
                .build()?,
        ));
    }
    Ok(SchemaDescriptor::new(Arc::new(
        Type::group_type_builder(root.name())
            .with_fields(fields)
            .build()?,
    )))
}
