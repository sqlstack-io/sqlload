//! A `duckdb` source in the loader: its reader, its tables, its rows and its check; a build without the `duckdb` feature refuses it.

use crate::loader::errors::LoaderError;
use crate::loader::models::SourceReader;
use crate::postgres_source::models::SourceTable;
use crate::project::models::{ProjectConfig, SourceConfig};

/// The source's reader, each of whose connections gets half a table's share of the memory budget.
#[cfg(feature = "duckdb")]
fn duckdb_source_reader(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<crate::duckdb_source::models::DuckDbReader, LoaderError> {
    let Some(connection) = project.connections.get(&source.connection) else {
        return Err(LoaderError::Invalid(format!(
            "connection {:?} is not defined",
            source.connection
        )));
    };
    let memory_limit = project.memory.budget.map(|budget| budget.table_share / 2);
    Ok(crate::duckdb_source::main::duckdb_reader::duckdb_reader(
        connection,
        memory_limit,
    )?)
}

/// The source's reader; nothing is opened until a read.
#[cfg(feature = "duckdb")]
pub(in crate::loader) fn open_duckdb(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<SourceReader, LoaderError> {
    Ok(SourceReader::DuckDb(Box::new(duckdb_source_reader(
        project, source,
    )?)))
}

#[cfg(not(feature = "duckdb"))]
pub(in crate::loader) fn open_duckdb(
    _: &ProjectConfig,
    _: &SourceConfig,
) -> Result<SourceReader, LoaderError> {
    Err(LoaderError::Invalid(
        crate::loader::constants::NO_DUCKDB_SOURCE.to_owned(),
    ))
}

/// The source's tables and views that `include` finds, then one table per query table.
#[cfg(feature = "duckdb")]
pub(in crate::loader) async fn duckdb_tables(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<Vec<SourceTable>, LoaderError> {
    let queries: Vec<(String, String)> = source
        .tables
        .iter()
        .filter_map(|(name, table)| Some((name.clone(), table.sql.clone()?)))
        .collect();
    let reader = duckdb_source_reader(project, source)?;
    Ok(crate::duckdb_source::main::read_catalog::read_catalog(
        &reader,
        (&source.include, &source.exclude),
        &queries,
    )
    .await?)
}

#[cfg(not(feature = "duckdb"))]
pub(in crate::loader) async fn duckdb_tables(
    _: &ProjectConfig,
    _: &SourceConfig,
) -> Result<Vec<SourceTable>, LoaderError> {
    Err(LoaderError::Invalid(
        crate::loader::constants::NO_DUCKDB_SOURCE.to_owned(),
    ))
}

/// Read `table`'s rows that `filter` selects (the plan's filter when `None`), passing each batch to `on_batch`; returns the row count.
#[cfg(feature = "duckdb")]
pub(in crate::loader) async fn read_duckdb(
    reader: &crate::duckdb_source::models::DuckDbReader,
    table: &crate::loader::models::TablePlan,
    filter: Option<String>,
    on_batch: impl AsyncFnMut(arrow_array::RecordBatch) -> Result<(), String>,
) -> Result<u64, LoaderError> {
    let filter = table
        .spec
        .read_filter(filter.or_else(|| table.source_filter.clone()));
    let request = crate::duckdb_source::models::DuckDbReadRequest {
        table: &table.spec.source_table,
        sql: table.spec.query_sql(),
        columns: &table.read_columns,
        filter: filter.as_deref(),
    };
    Ok(crate::duckdb_source::main::read_rows::read_rows(
        reader,
        &request,
        crate::loader::constants::SOURCE_BATCH_ROWS,
        on_batch,
    )
    .await?)
}

/// The database file the source reads and the DuckDB version reading it, for `debug`.
#[cfg(feature = "duckdb")]
pub(in crate::loader) async fn check_duckdb(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<String, LoaderError> {
    let reader = duckdb_source_reader(project, source)?;
    Ok(crate::duckdb_source::main::describe_source::describe_source(&reader).await?)
}

#[cfg(not(feature = "duckdb"))]
pub(in crate::loader) async fn check_duckdb(
    _: &ProjectConfig,
    _: &SourceConfig,
) -> Result<String, LoaderError> {
    Err(LoaderError::Invalid(
        crate::loader::constants::NO_DUCKDB_SOURCE.to_owned(),
    ))
}
