//! The destination check behind `debug`.

use crate::adapters::databricks::main::stage::_check_volume::check_volume;
use crate::adapters::dispatch::main::session::single_text::single_text;
use crate::adapters::dispatch::models::Destination;
use crate::adapters::snowflake::main::stage::_check_stage::check_stage;
use crate::adapters::snowflake::models::ForcedStage;
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::main::probe_folder::probe_folder;
use crate::loader::errors::LoaderError;
use crate::store_paths::models::Location;

/// What an open destination connects as, or why it cannot be used.
pub async fn check_destination(destination: Destination) -> Result<String, LoaderError> {
    match destination {
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
        Destination::Databricks(databricks) => {
            let query = "SELECT current_user() || ' on catalog ' || current_catalog()";
            let destination = Destination::Databricks(databricks.clone());
            let answer = single_text(&destination, query.to_owned())
                .await?
                .unwrap_or_default();
            let checked = check_volume(&databricks).await?;
            Ok(format!("{answer}; {checked}"))
        }
        Destination::Snowflake(snowflake) => {
            let query = "SELECT CURRENT_USER() || ' as ' || CURRENT_ROLE() || ', warehouse ' || COALESCE(CURRENT_WAREHOUSE(), '-')";
            let destination = Destination::Snowflake(snowflake.clone());
            let answer = single_text(&destination, query.to_owned())
                .await?
                .unwrap_or_default();
            let ForcedStage::External(stage) = &snowflake.stage else {
                return Ok(answer);
            };
            let checked = check_stage(&snowflake, stage).await?;
            Ok(format!("{answer}; {checked}"))
        }
        #[cfg(feature = "duckdb")]
        destination @ Destination::DuckDb(_) => {
            let answer =
                single_text(&destination, "SELECT 'DuckDB ' || version()".to_owned()).await?;
            Ok(answer.unwrap_or_default())
        }
    }
}
