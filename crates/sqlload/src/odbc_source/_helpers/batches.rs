//! The Arrow reader over an ODBC cursor, and what its failures say.

use std::sync::Arc;

use arrow_odbc::odbc_api::Cursor;
use arrow_odbc::{ColumnFailure, OdbcReaderBuilder, arrow_schema_from};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::odbc_source::constants::TOO_LARGE_VALUE;
use crate::odbc_source::errors::OdbcSourceError;
use crate::odbc_source::models::OdbcReader;

/// The reader of `cursor`'s rows in batches of `batch_rows`, decimals as text, each buffer capped by the reader's limits; a column the driver gives no length for is refused, naming the setting that bounds it.
pub(crate) fn batch_reader<C: Cursor>(
    reader: &OdbcReader,
    mut cursor: C,
    (dbms, batch_rows): (String, usize),
) -> Result<arrow_odbc::OdbcReader<C>, OdbcSourceError> {
    let schema = decimals_as_text(&arrow_schema_from(&mut cursor, Some(&dbms), false)?);
    let mut builder = OdbcReaderBuilder::new();
    builder
        .with_schema(schema)
        .with_dbms_name(dbms)
        .with_max_num_rows_per_batch(batch_rows)
        .with_fallibale_allocations(true);
    if let Some(size) = reader.limits.max_text_size {
        builder.with_max_text_size(size);
    }
    if let Some(size) = reader.limits.max_binary_size {
        builder.with_max_binary_size(size);
    }
    builder.build(cursor).map_err(|error| match &error {
        arrow_odbc::Error::ColumnFailure {
            name,
            source: ColumnFailure::ZeroSizedColumn { .. } | ColumnFailure::UnknownStringLength { .. },
            ..
        } => OdbcSourceError::Invalid(format!(
            "the driver gives no length for column {name}; set max_text_size (or max_binary_size, for binary columns) on the source"
        )),
        _ => OdbcSourceError::Arrow(error),
    })
}

/// A failed fetch, with the setting to raise when a value outgrew its buffer.
pub(crate) fn fetch_error(text: &str) -> OdbcSourceError {
    match text.contains(TOO_LARGE_VALUE) {
        true => OdbcSourceError::Invalid(format!(
            "{text}; a value longer than its buffer fails the read rather than being cut, so raise max_text_size or max_binary_size on the source"
        )),
        false => OdbcSourceError::Invalid(text.to_owned()),
    }
}

/// `schema` with every decimal field fetched as text instead.
pub(crate) fn decimals_as_text(schema: &Schema) -> SchemaRef {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Decimal128(..) => field.as_ref().clone().with_data_type(DataType::Utf8),
            _ => field.as_ref().clone(),
        })
        .collect();
    Arc::new(Schema::new(fields))
}
