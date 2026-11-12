//! Read Excel worksheets and XML documents whole, as records of typed cells.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions, StringArray};
use arrow_schema::{DataType, Field, Schema};

use crate::column_types::main::combined_type::combined_type;
use crate::column_types::models::ColumnType;
use crate::files_source::_helpers::cells::xlsx_records::xlsx_records;
use crate::files_source::_helpers::cells::xml_records::xml_records;
use crate::files_source::_helpers::text::bytes_reader::BytesReader;
use crate::files_source::_helpers::text::checks::{ColumnChecks, misfit, value_fits};
use crate::files_source::models::{CellRecord, FilesEncoding, SourceFormat};

/// A worksheet's or XML document's records, read whole, handed out a batch at a time.
pub(crate) struct CellReader {
    /// A worksheet's column names, in sheet order; empty for XML, whose records name their own.
    names: Vec<String>,
    records: std::vec::IntoIter<CellRecord>,
}

impl CellReader {
    /// The records of the file `bytes` reads, an xlsx workbook or an XML document as `encoding` says.
    pub(crate) async fn open(
        mut bytes: BytesReader,
        encoding: &FilesEncoding,
    ) -> Result<Self, String> {
        let whole = bytes.whole().await?;
        let encoding = encoding.clone();
        let (names, records) = tokio::task::spawn_blocking(move || match encoding.format {
            SourceFormat::Xlsx => xlsx_records(whole, &encoding.xlsx),
            _ => xml_records(&whole, &encoding.xml).map(|records| (Vec::new(), records)),
        })
        .await
        .map_err(|error| error.to_string())??;
        Ok(Self {
            names,
            records: records.into_iter(),
        })
    }

    pub(crate) fn names(&self) -> &[String] {
        &self.names
    }

    /// Up to `count` more records; empty at the end.
    pub(crate) fn next_records(&mut self, count: usize) -> Vec<CellRecord> {
        self.records.by_ref().take(count).collect()
    }
}

/// The type of a column holding cells of types `seen` and `value`: a date-formatted cell at midnight reads as a `date`, so a date and a timestamp make a `timestamp_ntz`; otherwise as for any text format.
pub(crate) fn cell_type(seen: ColumnType, value: ColumnType) -> ColumnType {
    match (seen, value) {
        (ColumnType::Date, ColumnType::TimestampNtz)
        | (ColumnType::TimestampNtz, ColumnType::Date) => ColumnType::TimestampNtz,
        (seen, value) => combined_type(seen, value),
    }
}

/// `records` as text columns `columns`, with the line each starts on; a cell no column takes, or a value an inferred column would not have been inferred from, is an error.
pub(crate) fn cells_batch(
    records: &[CellRecord],
    columns: &[(String, ColumnType)],
    checks: &ColumnChecks,
) -> Result<(RecordBatch, Vec<u64>), String> {
    let index: HashMap<&str, usize> = columns
        .iter()
        .enumerate()
        .map(|(position, (name, _))| (name.as_str(), position))
        .collect();
    let mut values: Vec<Vec<Option<String>>> = vec![vec![None; records.len()]; columns.len()];
    for (row, record) in records.iter().enumerate() {
        for (name, cell) in &record.cells {
            let Some(position) = index.get(name.as_str()) else {
                checks.check_name("column", name, record.line)?;
                continue;
            };
            if let Some(column_type) = checks.inferred_type(name)
                && !value_fits(column_type, Some(cell.kind.clone()))
                && cell_type(column_type.clone(), cell.kind.clone()) != *column_type
            {
                return Err(misfit(record.line, name, &cell.text, column_type));
            }
            values[*position][row] = Some(cell.text.clone());
        }
    }
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, _)| Field::new(name, DataType::Utf8, true))
        .collect();
    let arrays: Vec<ArrayRef> = values
        .into_iter()
        .map(|column| Arc::new(StringArray::from(column)) as ArrayRef)
        .collect();
    let options = RecordBatchOptions::new().with_row_count(Some(records.len()));
    let batch = RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &options)
        .map_err(|error| error.to_string())?;
    Ok((batch, records.iter().map(|record| record.line).collect()))
}
