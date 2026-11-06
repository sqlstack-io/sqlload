//! How a Parquet column's values are read.

use arrow_schema::DataType;

use crate::files_source::_helpers::plan::columns::arrow_read;
use crate::files_source::models::StoredRead;

/// How a column of Arrow type `data_type` is read, from the type alone: nanoseconds or plain.
pub fn stored_read_of(data_type: &DataType) -> StoredRead {
    arrow_read(data_type)
}
