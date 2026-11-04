use arrow_array::RecordBatch;

use crate::column_types::models::ColumnType;
use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::_helpers::text::bytes_reader::open_bytes;
use crate::files_source::_helpers::text::csv_reader::CsvReader;
use crate::files_source::_helpers::text::json_reader::{JsonReader, records_batch};
use crate::files_source::models::{Codec, FilesRead, SourceFormat};

/// Reads a JSON or CSV file as batches of text columns, which the shape step converts to the planned types.
pub(crate) enum TextReader {
    Json {
        reader: JsonReader,
        /// The columns to pick out of each object, and their planned types (`json` columns keep JSON text).
        columns: Vec<(String, ColumnType)>,
        batch_rows: usize,
    },
    Csv(Box<CsvReader>),
}

impl TextReader {
    /// A reader of `file` for `request`, producing its columns (JSON) or the file's own columns (CSV).
    pub(crate) async fn open(
        folder: &OpenFolder,
        file: &StoredFile,
        codec: Codec,
        request: &FilesRead,
    ) -> Result<Self, String> {
        let bytes = open_bytes(folder, file, codec).await?;
        match request.encoding.format {
            SourceFormat::Csv => Ok(Self::Csv(Box::new(
                CsvReader::open(bytes, &request.encoding.csv, request.batch_rows).await?,
            ))),
            format => Ok(Self::Json {
                reader: JsonReader::new(bytes, format == SourceFormat::Ndjson),
                columns: request
                    .columns
                    .iter()
                    .map(|column| (column.name.clone(), column.column_type.clone()))
                    .collect(),
                batch_rows: request.batch_rows,
            }),
        }
    }

    /// The next batch and the line each of its rows starts on; `None` at the end of the file.
    pub(crate) async fn next_batch(&mut self) -> Result<Option<(RecordBatch, Vec<u64>)>, String> {
        match self {
            Self::Json {
                reader,
                columns,
                batch_rows,
            } => {
                let records = reader.next_records(*batch_rows).await?;
                match records.is_empty() {
                    true => Ok(None),
                    false => records_batch(&records, columns).map(Some),
                }
            }
            Self::Csv(reader) => Ok(reader.next_batch().await?.map(|(batch, first)| {
                let lines: Vec<u64> = (first..first + batch.num_rows() as u64).collect();
                (batch, lines)
            })),
        }
    }
}
