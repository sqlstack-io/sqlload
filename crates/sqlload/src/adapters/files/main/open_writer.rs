//! Start writing one load's files.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_schema::{Field, Schema};

use crate::adapters::files::models::{FilesWriter, WriterRequest};
use crate::column_types::main::arrow_field::arrow_field;

/// A writer for the table's columns; fails for a column with no Arrow type (`native`).
pub fn open_writer(request: WriterRequest<'_>) -> Result<FilesWriter, String> {
    let fields: Vec<Field> = request
        .columns
        .iter()
        .map(|column| {
            arrow_field(&column.name, &column.column_type).ok_or_else(|| {
                format!(
                    "{} is {}, which Parquet files cannot hold; pin a portable type",
                    column.name, column.column_type
                )
            })
        })
        .collect::<Result<_, String>>()?;
    let loaded_at_micros = chrono::DateTime::parse_from_rfc3339(request.loaded_at)
        .map(|time| time.timestamp_micros())
        .map_err(|error| format!("load time {:?}: {error}", request.loaded_at))?;
    Ok(FilesWriter {
        folder: request.folder,
        partition: request.path.partition.clone(),
        cursor_column: request.cursor_column.map(str::to_owned),
        schema: Arc::new(Schema::new(fields)),
        load_id: request.load_id.to_owned(),
        file_id: request.file_id.to_owned(),
        loaded_at_micros,
        open: BTreeMap::new(),
        written: Vec::new(),
        partitions: BTreeSet::new(),
        rows: 0,
    })
}
