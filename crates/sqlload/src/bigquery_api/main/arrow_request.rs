//! An append request carrying Arrow rows.

use arrow_array::RecordBatch;
use arrow_ipc::writer::{
    DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions, write_message,
};

use crate::bigquery_api::models::{
    AppendRowsRequest, ArrowData, ArrowRecordBatch, ArrowSchema, Int64Value,
};
use crate::http::errors::ApiError;

/// `batch` as an append to `write_stream` at row `offset`, each part an encapsulated Arrow IPC message (as `pyarrow` serializes them); the stream's first request also carries the schema. Dictionary columns must be decoded first.
pub fn arrow_request(
    write_stream: &str,
    offset: i64,
    batch: &RecordBatch,
    with_schema: bool,
) -> Result<AppendRowsRequest, ApiError> {
    let encoding = |error: arrow_schema::ArrowError| {
        ApiError::Protocol(format!("cannot encode the rows as Arrow IPC: {error}"))
    };
    let generator = IpcDataGenerator::default();
    let options = IpcWriteOptions::default();
    let mut tracker = DictionaryTracker::new(false);
    let writer_schema = match with_schema {
        true => {
            let encoded = generator.schema_to_bytes_with_dictionary_tracker(
                &batch.schema(),
                &mut tracker,
                &options,
            );
            let mut bytes: Vec<u8> = Vec::new();
            write_message(&mut bytes, encoded, &options).map_err(encoding)?;
            Some(ArrowSchema {
                serialized_schema: bytes,
            })
        }
        false => None,
    };
    let (dictionaries, encoded) = generator
        .encode(
            batch,
            &mut tracker,
            &options,
            &mut IpcWriteContext::default(),
        )
        .map_err(encoding)?;
    if !dictionaries.is_empty() {
        return Err(ApiError::Protocol(
            "the Storage Write API takes no dictionary columns".to_owned(),
        ));
    }
    let mut rows: Vec<u8> = Vec::new();
    write_message(&mut rows, encoded, &options).map_err(encoding)?;
    Ok(AppendRowsRequest {
        write_stream: write_stream.to_owned(),
        offset: Some(Int64Value { value: offset }),
        arrow_rows: Some(ArrowData {
            writer_schema,
            rows: Some(ArrowRecordBatch {
                serialized_record_batch: rows,
            }),
        }),
        trace_id: String::new(),
    })
}
