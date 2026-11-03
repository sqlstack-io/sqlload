use std::fs::File;

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, Int32Type, Int64Type, TimestampMillisecondType};
use arrow_array::{Array, RecordBatch};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::config::main::parse_config::parse_config;
use crate::config::models::Config;

const BASE: &str = "[source]\nkind = \"kafka\"\nbrokers = \"b\"\ninclude = [\"*\"]\n[destination]\nkind = \"snowflake\"\naccount = \"a\"\nuser = \"u\"\nauth = { method = \"pat\", token = \"t\" }\ndatabase = \"MY_DB\"\nschema = \"RAW\"\n[staging]\nkind = \"s3\"\nformat = \"parquet\"\n[staging.s3]\nbucket = \"b\"\nregion = \"eu-west-2\"\nstage = \"S\"\n";

/// A Parquet `s3` configuration with `extra` appended.
pub(crate) fn parquet_config(extra: &str) -> Result<Config, String> {
    parse_config(&format!("{BASE}{extra}"), &|_| None).map_err(|error| error.to_string())
}

/// The single record batch in a Parquet file body.
pub(crate) fn read_parquet(body: &[u8]) -> Result<RecordBatch, String> {
    let path = std::env::temp_dir().join(format!(
        "sqlload-parquet-test-{}-{}.parquet",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::write(&path, body).map_err(|error| error.to_string())?;
    let file = File::open(&path).map_err(|error| error.to_string())?;
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .and_then(|builder| builder.build())
        .map_err(|error| error.to_string())?;
    let batch = reader
        .next()
        .ok_or("no record batch")?
        .map_err(|error| error.to_string());
    let _ = std::fs::remove_file(&path);
    batch
}

/// The first value of an Int32 column.
pub(crate) fn first_int32(array: &dyn Array) -> String {
    array.as_primitive::<Int32Type>().value(0).to_string()
}

/// The first value of an Int64 column.
pub(crate) fn first_int64(array: &dyn Array) -> String {
    array.as_primitive::<Int64Type>().value(0).to_string()
}

/// The first value of a millisecond timestamp column.
pub(crate) fn first_timestamp(array: &dyn Array) -> String {
    array
        .as_primitive::<TimestampMillisecondType>()
        .value(0)
        .to_string()
}

/// The first value of a Date32 column.
pub(crate) fn first_date(array: &dyn Array) -> String {
    array.as_primitive::<Date32Type>().value(0).to_string()
}

/// The first value of a Utf8 column.
pub(crate) fn first_text(array: &dyn Array) -> String {
    array.as_string::<i32>().value(0).to_owned()
}
