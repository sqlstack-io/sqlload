//! The target check behind `debug`.

use crate::file_store::main::open_folder::open_folder;
use crate::file_store::main::probe_folder::probe_folder;
use crate::loader::_helpers::destination::single_text;
use crate::loader::errors::LoaderError;
use crate::loader::main::_connect_target::connect_target;
use crate::loader::models::Destination;
use crate::project::models::ProjectConfig;
use crate::store_paths::models::Location;

/// What target `name` connects as, or why it cannot connect.
pub(in crate::loader) async fn check_target(
    project: &ProjectConfig,
    name: &str,
) -> Result<String, LoaderError> {
    match connect_target(project, name).await? {
        Destination::Files(files) => {
            let folder = open_folder(&Location {
                store: files.store.clone(),
                prefix: String::new(),
            })?;
            let entries = probe_folder(&folder).await?;
            Ok(format!(
                "{} lists ({entries} entries at the top)",
                folder.location
            ))
        }
        destination => {
            let query = match &destination {
                Destination::Snowflake(_) => {
                    "SELECT CURRENT_USER() || ' as ' || CURRENT_ROLE() || ', warehouse ' || COALESCE(CURRENT_WAREHOUSE(), '-')"
                }
                _ => "SELECT 'DuckDB ' || version()",
            };
            let answer = single_text(&destination, query.to_owned()).await?;
            Ok(answer.unwrap_or_default())
        }
    }
}
