use std::collections::HashMap;

use arrow_array::{Array, RecordBatch, StringArray};

use crate::column_types::models::ColumnType;
use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::_helpers::plan::columns::SampledColumn;
use crate::files_source::_helpers::text::bytes_reader::open_bytes;
use crate::files_source::_helpers::text::csv_reader::CsvReader;
use crate::files_source::_helpers::text::inference::{
    combine_types, csv_value_type, json_value_type,
};
use crate::files_source::_helpers::text::json_reader::JsonReader;
use crate::files_source::_helpers::text::json_records::JsonRecord;
use crate::files_source::constants::SAMPLE_ROWS;
use crate::files_source::models::{Codec, FilesEncoding, SourceFormat};

/// The columns in the first records of a JSON or CSV `file`, with the types their values suggest.
pub(crate) async fn sample_file(
    folder: &OpenFolder,
    file: &StoredFile,
    codec: Codec,
    encoding: &FilesEncoding,
) -> Result<Vec<SampledColumn>, String> {
    let bytes = open_bytes(folder, file, codec).await?;
    match encoding.format {
        SourceFormat::Csv => {
            let mut reader = CsvReader::open(bytes, &encoding.csv, SAMPLE_ROWS).await?;
            let batch = reader.next_batch().await?.map(|(batch, _)| batch);
            Ok(csv_columns(reader.names(), batch.as_ref()))
        }
        format => {
            let mut reader = JsonReader::new(bytes, format == SourceFormat::Ndjson);
            Ok(json_columns(&reader.next_records(SAMPLE_ROWS).await?))
        }
    }
}

/// Every key in `records`, in first-seen order, with the type its values suggest.
pub(crate) fn json_columns(records: &[JsonRecord]) -> Vec<SampledColumn> {
    let mut columns: Vec<SampledColumn> = Vec::new();
    let mut positions: HashMap<&str, usize> = HashMap::new();
    for record in records {
        for (key, value) in &record.fields {
            let position = *positions.entry(key.as_str()).or_insert_with(|| {
                columns.push(SampledColumn {
                    name: key.clone(),
                    column_type: None,
                });
                columns.len() - 1
            });
            let column = &mut columns[position];
            column.column_type = observe(column.column_type.take(), json_value_type(value));
        }
    }
    columns
}

/// CSV columns `names`, with the type the values in `batch` (text columns) suggest.
pub(crate) fn csv_columns(names: &[String], batch: Option<&RecordBatch>) -> Vec<SampledColumn> {
    let mut columns: Vec<SampledColumn> = Vec::new();
    for (position, name) in names.iter().enumerate() {
        let values = batch.and_then(|batch| {
            batch
                .column(position)
                .as_any()
                .downcast_ref::<StringArray>()
        });
        let mut column_type: Option<Result<ColumnType, String>> = None;
        for value in values.into_iter().flatten().flatten() {
            column_type = observe(column_type, Some(csv_value_type(value)));
        }
        columns.push(SampledColumn {
            name: name.clone(),
            column_type,
        });
    }
    columns
}

/// A column's type after one more value of type `value_type` (`None` for NULL).
fn observe(
    seen: Option<Result<ColumnType, String>>,
    value_type: Option<ColumnType>,
) -> Option<Result<ColumnType, String>> {
    match (seen, value_type) {
        (seen, None) => seen,
        (None, Some(value_type)) => Some(Ok(value_type)),
        (Some(Ok(seen)), Some(value_type)) => Some(Ok(combine_types(seen, value_type))),
        (Some(Err(reason)), Some(_)) => Some(Err(reason)),
    }
}
