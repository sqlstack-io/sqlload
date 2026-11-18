//! The source check behind `debug`.

use crate::file_store::main::open_folder::open_folder;
use crate::file_store::main::probe_folder::probe_folder;
use crate::loader::_helpers::sources::{
    connect_source, files_location, open_source, sheets_reader,
};
use crate::loader::errors::LoaderError;
use crate::loader::models::SourceReader;
use crate::project::models::{ConnectionConfig, ProjectConfig, SourceConfig, SourceKind};
use crate::sheets_source::main::tab_ranges::tab_ranges;

/// What source `source` reads as, or why it cannot connect; for Postgres, what protects the connection and the bastion it goes through (a failure names the hop: the SSH tunnel, or the database via it).
pub(in crate::loader) async fn check_source(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<String, LoaderError> {
    match source.kind {
        SourceKind::Python => Ok(format!("{} Python tables", source.tables.len())),
        SourceKind::Postgres | SourceKind::PostgresCdc => check_postgres(project, source).await,
        SourceKind::Snowflake => {
            let SourceReader::Snowflake(snowflake) = open_source(project, source, 1).await? else {
                return Err(LoaderError::Invalid(
                    "a snowflake source opens as a Snowflake reader".to_owned(),
                ));
            };
            Ok(crate::snowflake_source::main::describe_source::describe_source(&snowflake).await?)
        }
        SourceKind::BigQuery => {
            let SourceReader::BigQuery(reader) = open_source(project, source, 1).await? else {
                return Err(LoaderError::Invalid(
                    "a bigquery source opens as a BigQuery reader".to_owned(),
                ));
            };
            let datasets = crate::bigquery_api::main::list_datasets::list_datasets(
                &reader.client,
                &reader.project,
            )
            .await
            .map_err(crate::bigquery_source::errors::BigQuerySourceError::from)?;
            Ok(format!(
                "project {} ({} datasets), jobs in {} at {}",
                reader.project,
                datasets.len(),
                reader.client.project,
                reader.client.location()
            ))
        }
        SourceKind::Databricks => check_databricks(project, source).await,
        #[cfg(feature = "duckdb")]
        SourceKind::DuckDb | SourceKind::MotherDuck => {
            let reader = crate::duckdb_source::main::duckdb_reader::duckdb_reader(project, source)?;
            Ok(crate::duckdb_source::main::describe_source::describe_source(&reader).await?)
        }
        #[cfg(not(feature = "duckdb"))]
        SourceKind::DuckDb | SourceKind::MotherDuck => Err(LoaderError::Invalid(
            crate::loader::constants::NO_DUCKDB_SOURCE.to_owned(),
        )),
        #[cfg(feature = "odbc")]
        SourceKind::Odbc => {
            let reader = crate::odbc_source::main::source_reader::source_reader(project, source)
                .map_err(LoaderError::Invalid)?;
            Ok(crate::odbc_source::main::describe_source::describe_source(&reader).await?)
        }
        #[cfg(not(feature = "odbc"))]
        SourceKind::Odbc => Err(LoaderError::Invalid(
            crate::loader::constants::NO_ODBC.to_owned(),
        )),
        SourceKind::GoogleSheets => {
            let (client, spreadsheet_id) = sheets_reader(project, source)?;
            let (title, tabs) = tab_ranges(&client, &spreadsheet_id).await?;
            Ok(format!(
                "{title:?} reads as {} ({} tabs)",
                client.account(),
                tabs.len()
            ))
        }
        SourceKind::Files => {
            let folder = open_folder(&files_location(project, source)?)?;
            let entries = probe_folder(&folder).await?;
            Ok(format!(
                "{} lists ({entries} entries at the top)",
                folder.location
            ))
        }
    }
}

/// Who `source` connects to Postgres as, what protects the connection, and the bastion it goes through.
async fn check_postgres(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<String, LoaderError> {
    let (client, encryption) = connect_source(project, source).await?;
    let row = client
        .query_one(
            "SELECT current_user || '@' || current_database() || ', ' || version()",
            &[],
        )
        .await
        .map_err(|error| LoaderError::Invalid(format!("cannot query Postgres: {error}")))?;
    let answer: String = row.get(0);
    let route = match project.connections.get(&source.connection) {
        Some(ConnectionConfig::Postgres {
            ssh: Some(tunnel), ..
        }) => {
            format!(", via {}", tunnel.route())
        }
        _ => String::new(),
    };
    Ok(format!(
        "{}, {encryption}{route}",
        answer.split(" on ").next().unwrap_or_default()
    ))
}

/// Who a `databricks` source reads as, its warehouse's version and the catalog it reads.
async fn check_databricks(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<String, LoaderError> {
    let reader =
        crate::databricks_source::main::databricks_reader::databricks_reader(project, source)?;
    let answer = crate::databricks_api::main::execute_statement::execute_statement(
            &reader.client,
            "SELECT current_user() || ', catalog ' || current_catalog() || ', Databricks ' || current_version().dbsql_version",
        )
        .await
        .map_err(crate::databricks_source::errors::DatabricksSourceError::from)?;
    Ok(format!(
        "{}; reads catalog {}",
        answer
            .rows
            .first()
            .and_then(|row| row.first().cloned().flatten())
            .unwrap_or_default(),
        reader.catalog
    ))
}
