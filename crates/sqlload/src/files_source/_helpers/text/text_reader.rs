use arrow_array::{Array, RecordBatch, StringArray};

use crate::column_types::models::ColumnType;
use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::_helpers::text::bytes_reader::open_bytes;
use crate::files_source::_helpers::text::checks::{ColumnChecks, misfit, value_fits};
use crate::files_source::_helpers::text::csv_reader::CsvReader;
use crate::files_source::_helpers::text::inference::csv_value_type;
use crate::files_source::_helpers::text::json_reader::{JsonReader, records_batch};
use crate::files_source::models::{Codec, FilesRead, SourceFormat};

/// The reader behind a [`TextReader`].
enum Records {
    Json {
        reader: JsonReader,
        /// The columns to pick out of each object, and their planned types (`json` columns keep JSON text).
        columns: Vec<(String, ColumnType)>,
        batch_rows: usize,
    },
    Csv(Box<CsvReader>),
}

/// Reads a JSON or CSV file as batches of text columns, which the shape step converts to the planned types.
pub(crate) struct TextReader {
    records: Records,
    checks: ColumnChecks,
}

impl TextReader {
    /// A reader of `file` for `request`, producing its columns (JSON) or the file's own columns (CSV); a CSV header the source lacks is an error.
    pub(crate) async fn open(
        folder: &OpenFolder,
        file: &StoredFile,
        codec: Codec,
        request: &FilesRead,
    ) -> Result<Self, String> {
        let checks = ColumnChecks::new(request)?;
        let bytes = open_bytes(folder, file, codec).await?;
        let records = match request.encoding.format {
            SourceFormat::Csv => {
                let reader =
                    CsvReader::open(bytes, &request.encoding.csv, request.batch_rows).await?;
                for name in reader.names() {
                    checks.check_name("header", name, 1)?;
                }
                Records::Csv(Box::new(reader))
            }
            format => Records::Json {
                reader: JsonReader::new(bytes, format == SourceFormat::Ndjson),
                columns: request
                    .columns
                    .iter()
                    .map(|column| (column.name.clone(), column.column_type.clone()))
                    .collect(),
                batch_rows: request.batch_rows,
            },
        };
        Ok(Self { records, checks })
    }

    /// The next batch and the line each of its rows starts on; `None` at the end of the file.
    pub(crate) async fn next_batch(&mut self) -> Result<Option<(RecordBatch, Vec<u64>)>, String> {
        match &mut self.records {
            Records::Json {
                reader,
                columns,
                batch_rows,
            } => {
                let records = reader.next_records(*batch_rows).await?;
                match records.is_empty() {
                    true => Ok(None),
                    false => records_batch(&records, columns, &self.checks).map(Some),
                }
            }
            Records::Csv(reader) => {
                let Some((batch, first)) = reader.next_batch().await? else {
                    return Ok(None);
                };
                let lines: Vec<u64> = (first..first + batch.num_rows() as u64).collect();
                check_csv_values(&batch, &lines, &self.checks)?;
                Ok(Some((batch, lines)))
            }
        }
    }
}

/// An error naming the first CSV value of an inferred column that is not what sampling would have inferred.
fn check_csv_values(
    batch: &RecordBatch,
    lines: &[u64],
    checks: &ColumnChecks,
) -> Result<(), String> {
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        let Some(column_type) = checks.inferred_type(field.name()) else {
            continue;
        };
        let Some(values) = column.as_any().downcast_ref::<StringArray>() else {
            continue;
        };
        let misfit_row = values
            .iter()
            .position(|value| !value_fits(column_type, value.map(csv_value_type)));
        if let Some(row) = misfit_row {
            return Err(misfit(
                lines[row],
                field.name(),
                &format!("{:?}", values.value(row)),
                column_type,
            ));
        }
    }
    Ok(())
}
