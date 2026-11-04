//! The source check behind `debug`.

use crate::file_store::main::probe_folder::probe_folder;
use crate::loader::_helpers::sources::open_source;
use crate::loader::errors::LoaderError;
use crate::loader::models::SourceReader;
use crate::project::models::{ProjectConfig, SourceConfig};

/// What source `source` reads as, or why it cannot connect.
pub(in crate::loader) async fn check_source(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<String, LoaderError> {
    match open_source(project, source).await? {
        SourceReader::Postgres(client) => {
            let row = client
                .query_one(
                    "SELECT current_user || '@' || current_database() || ', ' || version()",
                    &[],
                )
                .await
                .map_err(|error| LoaderError::Invalid(format!("cannot query Postgres: {error}")))?;
            let answer: String = row.get(0);
            Ok(answer.split(" on ").next().unwrap_or_default().to_owned())
        }
        SourceReader::Files(folder) => {
            let entries = probe_folder(&folder).await?;
            Ok(format!(
                "{} lists ({entries} entries at the top)",
                folder.location
            ))
        }
    }
}
