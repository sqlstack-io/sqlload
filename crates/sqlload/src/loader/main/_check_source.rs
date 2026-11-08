//! The source check behind `debug`.

use crate::file_store::main::open_folder::open_folder;
use crate::file_store::main::probe_folder::probe_folder;
use crate::loader::_helpers::sources::{connect_source, files_location};
use crate::loader::errors::LoaderError;
use crate::project::models::{ProjectConfig, SourceConfig, SourceKind};

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
