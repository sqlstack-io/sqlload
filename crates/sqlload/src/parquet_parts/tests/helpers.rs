use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use bytes::Bytes;
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::parquet_parts::main::finish_part::finish_part;
use crate::parquet_parts::main::write_part::write_part;
use crate::parquet_parts::models::PartWriter;

/// Write `batches` (column name and rows) and finish; returns each file's row count, read back from its footer.
pub(crate) fn written_files(batches: &[(&str, usize)]) -> Result<Vec<usize>, String> {
    let mut parts = PartWriter::default();
    let mut files: Vec<Vec<u8>> = Vec::new();
    for (column, rows) in batches {
        let values: ArrayRef = Arc::new(Int64Array::from_iter_values(0..*rows as i64));
        let batch =
            RecordBatch::try_from_iter([(*column, values)]).map_err(|error| error.to_string())?;
        let (rest, finished) = write_part(parts, &batch)?;
        parts = rest;
        files.extend(finished);
    }
    let (_, last) = finish_part(parts)?;
    files.extend(last);
    files.iter().map(|body| file_rows(body)).collect()
}

fn file_rows(body: &[u8]) -> Result<usize, String> {
    let reader = SerializedFileReader::new(Bytes::copy_from_slice(body))
        .map_err(|error| error.to_string())?;
    usize::try_from(reader.metadata().file_metadata().num_rows()).map_err(|error| error.to_string())
}
