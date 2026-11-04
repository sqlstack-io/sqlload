use arrow_array::RecordBatch;
use arrow_cast::display::{ArrayFormatter, FormatOptions};

/// Run an async test body on a fresh multi-threaded runtime.
pub(crate) fn run_async<F: std::future::Future<Output = Result<(), String>>>(
    body: F,
) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(body)
}

/// Every value of `column` across `batches`, as Arrow displays it.
pub(crate) fn column_values(batches: &[RecordBatch], column: &str) -> Result<Vec<String>, String> {
    let options = FormatOptions::default();
    let mut values = Vec::new();
    for batch in batches {
        let array = batch
            .column_by_name(column)
            .ok_or_else(|| format!("no column {column}"))?;
        let formatter =
            ArrayFormatter::try_new(array.as_ref(), &options).map_err(|error| error.to_string())?;
        values.extend((0..array.len()).map(|row| formatter.value(row).to_string()));
    }
    Ok(values)
}
