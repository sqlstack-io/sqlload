//! The source check behind `debug`.

use crate::file_store::main::open_folder::open_folder;
use crate::file_store::main::probe_folder::probe_folder;
use crate::loader::_helpers::sources::{connect_source, files_location, open_source};
use crate::loader::errors::LoaderError;
use crate::loader::models::SourceReader;
use crate::project::models::{ProjectConfig, SourceConfig, SourceKind};
use crate::sql_api::main::execute_sql::execute_sql;

/// What source `source` reads as, or why it cannot connect; for Postgres, what protects the connection.
pub(in crate::loader) async fn check_source(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<String, LoaderError> {
    match source.kind {
        SourceKind::Python => Ok(format!("{} Python tables", source.tables.len())),
        SourceKind::Postgres | SourceKind::PostgresCdc => {
            let (client, encryption) = connect_source(project, source).await?;
            let row = client
                .query_one(
                    "SELECT current_user || '@' || current_database() || ', ' || version()",
                    &[],
                )
                .await
                .map_err(|error| LoaderError::Invalid(format!("cannot query Postgres: {error}")))?;
            let answer: String = row.get(0);
            Ok(format!(
                "{}, {encryption}",
                answer.split(" on ").next().unwrap_or_default()
            ))
        }
        SourceKind::Snowflake => {
            let SourceReader::Snowflake(snowflake) = open_source(project, source, 1).await? else {
                return Err(LoaderError::Invalid(
                    "a snowflake source opens as a Snowflake reader".to_owned(),
                ));
            };
            let answer = execute_sql(
                &snowflake.sql,
                "SELECT CURRENT_USER() || '@' || CURRENT_ACCOUNT() || ', warehouse ' || COALESCE(CURRENT_WAREHOUSE(), 'none') || ', Snowflake ' || CURRENT_VERSION()",
            )
            .await?;
            Ok(answer
                .rows
                .first()
                .and_then(|row| row.first().cloned().flatten())
                .unwrap_or_default())
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
