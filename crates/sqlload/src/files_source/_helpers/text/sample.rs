use std::collections::HashMap;

use arrow_array::{Array, RecordBatch, StringArray};

use crate::column_types::main::combined_type::combined_type;
use crate::column_types::models::ColumnType;
use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::_helpers::cells::cell_reader::{CellReader, cell_type};
use crate::files_source::_helpers::plan::columns::{SampledColumn, joined_lists, text_lists};
use crate::files_source::_helpers::text::bytes_reader::open_bytes;
use crate::files_source::_helpers::text::csv_reader::CsvReader;
use crate::files_source::_helpers::text::inference::{csv_value_type, json_value_type};
use crate::files_source::_helpers::text::json_reader::JsonReader;
use crate::files_source::_helpers::text::json_records::JsonRecord;
use crate::files_source::constants::{FLATTENED_KEY, SAMPLE_ROWS};
use crate::files_source::models::{CellRecord, Codec, FilesEncoding, SourceFormat, StoredRead};

/// The columns in the first records of a JSON or CSV `file`, with the types their values suggest, and the objects flattening replaced with their keys' columns in them.
pub(crate) async fn sample_file(
    folder: &OpenFolder,
    file: &StoredFile,
    codec: Codec,
    encoding: &FilesEncoding,
) -> Result<(Vec<SampledColumn>, Vec<String>), String> {
    let bytes = open_bytes(folder, file, codec).await?;
    match encoding.format {
        SourceFormat::Xlsx | SourceFormat::Xml => {
            let mut reader = CellReader::open(bytes, encoding).await?;
            let records = reader.next_records(SAMPLE_ROWS);
            Ok((cell_columns(reader.names(), &records), Vec::new()))
        }
        SourceFormat::Csv => {
            let mut reader = CsvReader::open(bytes, &encoding.csv, SAMPLE_ROWS).await?;
            let batch = reader.next_batch().await?.map(|(batch, _)| batch);
            Ok((csv_columns(reader.names(), batch.as_ref()), Vec::new()))
        }
        format => {
            let mut reader = JsonReader::new(bytes, format == SourceFormat::Ndjson)
                .flattening(encoding.flatten_objects);
            let mut columns = json_columns(&reader.next_records(SAMPLE_ROWS).await?);
            for column in &mut columns {
                if reader.flattened().contains(&column.name) {
                    column.direct_refusal = Some(FLATTENED_KEY.to_owned());
                }
            }
            Ok((columns, reader.objects().iter().cloned().collect()))
        }
    }
}

/// Every key in `records`, in first-seen order, with the type its values suggest.
pub(crate) fn json_columns(records: &[JsonRecord]) -> Vec<SampledColumn> {
    let mut columns: Vec<SampledColumn> = Vec::new();
    let mut positions: HashMap<&str, usize> = HashMap::new();
    for record in records {
        for (key, value, nested) in &record.fields {
            let position = *positions.entry(key.as_str()).or_insert_with(|| {
                columns.push(SampledColumn {
                    name: key.clone(),
                    column_type: None,
                    direct_refusal: None,
                    stored_read: StoredRead::Plain,
                    lists: None,
                });
                columns.len() - 1
            });
            let column = &mut columns[position];
            column.column_type = observe(column.column_type.take(), json_value_type(value));
            column.lists = joined_lists(column.lists, value_lists(value, nested.as_deref()));
        }
    }
    columns
}

/// A worksheet's columns `names`, then any other key in `records` in first-seen order, each with the type its cells suggest.
pub(crate) fn cell_columns(names: &[String], records: &[CellRecord]) -> Vec<SampledColumn> {
    let mut columns: Vec<SampledColumn> = names.iter().map(|name| text_column(name)).collect();
    for record in records {
        for (name, cell) in &record.cells {
            let position = match columns.iter().position(|column| column.name == *name) {
                Some(position) => position,
                None => {
                    columns.push(text_column(name));
                    columns.len() - 1
                }
            };
            let column = &mut columns[position];
            column.column_type = match column.column_type.take() {
                Some(Ok(seen)) => Some(Ok(cell_type(seen, cell.kind.clone()))),
                seen => observe(seen, Some(cell.kind.clone())),
            };
            let lists = match cell.kind {
                ColumnType::Json => text_lists(&cell.text),
                _ => Some(false),
            };
            column.lists = joined_lists(column.lists, lists);
        }
    }
    columns
}

/// A sampled column `name` with no value seen yet.
fn text_column(name: &str) -> SampledColumn {
    SampledColumn {
        name: name.to_owned(),
        column_type: None,
        direct_refusal: None,
        stored_read: StoredRead::Plain,
        lists: None,
    }
}

/// What one JSON value says about its column being a list field, from its `nested` JSON text when it is a list or object: `None` for NULL or an empty list.
fn value_lists(value: &serde_json::Value, nested: Option<&str>) -> Option<bool> {
    match (value, nested) {
        (_, Some(text)) => text_lists(text),
        (serde_json::Value::Null, None) => None,
        _ => Some(false),
    }
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
            direct_refusal: None,
            stored_read: StoredRead::Plain,
            lists: Some(false),
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
        (Some(Ok(seen)), Some(value_type)) => Some(Ok(combined_type(seen, value_type))),
        (Some(Err(reason)), Some(_)) => Some(Err(reason)),
    }
}
