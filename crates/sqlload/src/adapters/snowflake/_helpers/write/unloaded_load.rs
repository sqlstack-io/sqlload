//! A Snowflake source's unloaded files loaded by Snowflake `COPY`ing them itself.

use crate::adapters::contract::models::{DirectLoaded, UnloadedBatch};
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::snowflake::_helpers::write::direct_load::staged_as_text;
use crate::adapters::snowflake::main::copy::_copy_unloaded::copy_unloaded;
use crate::adapters::snowflake::main::copy::_stage_covers::stage_covers;
use crate::adapters::snowflake::main::stage::_stage_reference::stage_reference;
use crate::adapters::snowflake::models::{SnowflakeSql, SnowflakeWriter, StageRoute, UnloadedCopy};
use crate::loader::errors::LoaderError;
use crate::sql_api::main::execute_sql::execute_sql;

impl SnowflakeWriter {
    /// Load `batch` by having Snowflake `COPY` the unloaded files from the external stage it loads through, which covers them, where Arrow batches would go: the load's staging table, the build table, or the table itself. The `COPY`s run in one block, in one transaction, that commits only when they load as many rows as the unload reports; otherwise it rolls back and the batch fails.
    pub(in crate::adapters) async fn pull_unloaded(
        &mut self,
        batch: &UnloadedBatch<'_>,
    ) -> Result<DirectLoaded, LoaderError> {
        let target = &batch.target;
        let loaded = |rows: u64| match target.direct {
            true => DirectLoaded::Committed(rows),
            false => DirectLoaded::Staged(rows),
        };
        let files = batch.files;
        if files.keys.is_empty() {
            return Ok(loaded(0));
        }
        let covering = match &self.route {
            Some(StageRoute::External(stage)) if stage_covers(&stage.location, &files.folder) => {
                files
                    .folder
                    .prefix
                    .strip_prefix(&stage.location.prefix)
                    .map(|folder| (stage_reference(stage), folder.to_owned()))
            }
            _ => None,
        };
        let Some((stage, folder)) = covering else {
            return Err(LoaderError::Invalid(format!(
                "Snowflake cannot read the unloaded files in {}: the target's external stage does not cover them",
                files.folder
            )));
        };
        let staged;
        let columns = match (target.staging, target.builds) {
            (Some(_), false) => {
                staged = staged_as_text(target.columns);
                &staged
            }
            _ => batch.write_columns,
        };
        let copies = copy_unloaded(&UnloadedCopy {
            into: target.staging.unwrap_or(target.table),
            columns,
            read: batch.read,
            constants: target.constants,
            compat: target.compat,
            batch: target.batch,
            load_id: target.load_id,
            loaded_at: batch.loaded_at,
            stage: &stage,
            folder: &folder,
            keys: &files.keys,
            filename_prefix: &files.folder.prefix,
        });
        let block = checked_block(&copies, files.rows);
        let answer = execute_sql(&self.destination.sql, &block).await?;
        let returned = answer
            .rows
            .first()
            .and_then(|row| row.first().cloned().flatten())
            .unwrap_or_default();
        let rows: u64 = returned.parse().map_err(|error| {
            LoaderError::Invalid(format!(
                "the unloaded files' COPY returned {returned:?}, not a row count: {error}"
            ))
        })?;
        if rows != files.rows {
            return Err(LoaderError::Invalid(format!(
                "Snowflake loaded {rows} rows from the unloaded files in {}, but the unload reports {}; the batch is not committed",
                files.folder, files.rows
            )));
        }
        Ok(loaded(rows))
    }
}

/// One Snowflake Scripting block running `statements` (`COPY`s) in a transaction and returning the rows they loaded, committed only when that is `expected` and rolled back otherwise.
pub(in crate::adapters) fn checked_block(statements: &[String], expected: u64) -> String {
    let mut body = vec![
        "DECLARE loaded INTEGER DEFAULT 0; copied INTEGER;".to_owned(),
        "BEGIN".to_owned(),
        "BEGIN TRANSACTION;".to_owned(),
    ];
    for statement in statements {
        body.push(format!("{statement};"));
        body.push("SELECT COALESCE(SUM(\"rows_loaded\"), 0) INTO :copied FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));".to_owned());
        body.push("loaded := loaded + copied;".to_owned());
    }
    body.push(format!(
        "IF (loaded = {expected}) THEN COMMIT; ELSE ROLLBACK; END IF;"
    ));
    body.push("RETURN loaded;".to_owned());
    body.push("END;".to_owned());
    format!(
        "EXECUTE IMMEDIATE {}",
        SnowflakeSql.render_loader_value_literal(&body.join("\n"))
    )
}
