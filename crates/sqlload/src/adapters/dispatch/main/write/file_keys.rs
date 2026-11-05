//! The keys of a JSON batch's records, as the destination reads them.

use crate::adapters::dispatch::models::Writer;
use crate::column_types::models::ColumnType;
use crate::files_source::models::DataFile;
use crate::loader::errors::LoaderError;
use crate::loader::models::TableSpec;

/// Every key a JSON or NDJSON batch's `files` hold and the type its values suggest, where the destination reads those files itself (Snowflake on an external stage); `None` where SQLLoad reads them, which checks every key as it goes.
pub async fn file_keys(
    writer: &Writer,
    spec: &TableSpec,
    files: &[DataFile],
) -> Result<Option<Vec<(String, Option<ColumnType>)>>, LoaderError> {
    match writer {
        Writer::Snowflake(snowflake) => snowflake.file_keys(spec, files).await,
        _ => Ok(None),
    }
}
