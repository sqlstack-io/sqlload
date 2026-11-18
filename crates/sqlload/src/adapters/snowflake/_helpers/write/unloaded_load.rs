//! A Snowflake source's rows loaded by Snowflake itself: `COPY`ing its unloaded files, or selecting them in the source's own session.

use crate::adapters::contract::models::{DirectLoaded, UnloadedBatch, UnloadedSource};
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::snowflake::_helpers::write::direct_load::staged_as_text;
use crate::adapters::snowflake::main::copy::_stage_covers::stage_covers;
use crate::adapters::snowflake::main::copy::_unloaded_sql::unloaded_statements;
use crate::adapters::snowflake::main::stage::_stage_reference::stage_reference;
use crate::adapters::snowflake::models::{
    SnowflakeSql, SnowflakeWriter, StageRoute, UnloadedFrom, UnloadedLoad,
};
use crate::loader::errors::LoaderError;
use crate::sql_api::main::execute_sql::execute_sql;

impl SnowflakeWriter {
    /// Load `batch` inside Snowflake, where Arrow batches would go: the load's staging table, the build table, or the table itself. Unloaded files are `COPY`d from the external stage the writer loads through, which must cover them, in one transaction that commits only when they load as many rows as the unload reports; a query's rows are selected by one `INSERT ... SELECT`.
    pub(in crate::adapters) async fn pull_unloaded(
        &mut self,
        batch: &UnloadedBatch<'_>,
    ) -> Result<DirectLoaded, LoaderError> {
        let target = &batch.target;
        let loaded = |rows: u64| match target.direct {
            true => DirectLoaded::Committed(rows),
            false => DirectLoaded::Staged(rows),
        };
        let staged;
        let columns = match (target.staging, target.builds) {
            (Some(_), false) => {
                staged = staged_as_text(target.columns);
                &staged
            }
            _ => batch.write_columns,
        };
        let place: (String, String);
        let (from, unloaded) = match batch.source {
            UnloadedSource::Files(files) if files.keys.is_empty() => return Ok(loaded(0)),
            UnloadedSource::Files(files) => {
                let covering = match &self.route {
                    Some(StageRoute::External(stage))
                        if stage_covers(&stage.location, &files.folder) =>
                    {
                        files
                            .folder
                            .prefix
                            .strip_prefix(&stage.location.prefix)
                            .map(|folder| (stage_reference(stage), folder.to_owned()))
                    }
                    _ => None,
                };
                let Some(covering) = covering else {
                    return Err(LoaderError::Invalid(format!(
                        "Snowflake cannot read the unloaded files in {}: the target's external stage does not cover them",
                        files.folder
                    )));
                };
                place = covering;
                (
                    UnloadedFrom::Files {
                        stage: &place.0,
                        folder: &place.1,
                        keys: &files.keys,
                        filename_prefix: &files.folder.prefix,
                    },
                    Some(files),
                )
            }
            UnloadedSource::Query(query) => (UnloadedFrom::Query(query), None),
        };
        let statements = unloaded_statements(&UnloadedLoad {
            into: target.staging.unwrap_or(target.table),
            columns,
            read: batch.read,
            constants: target.constants,
            compat: target.compat,
            batch: target.batch,
            load_id: target.load_id,
            loaded_at: batch.loaded_at,
            from,
        });
        let expected = unloaded.map(|files| files.rows);
        let answer =
            execute_sql(&self.destination.sql, &counted_block(&statements, expected)).await?;
        let returned = answer
            .rows
            .first()
            .and_then(|row| row.first().cloned().flatten())
            .unwrap_or_default();
        let rows: u64 = returned.parse().map_err(|error| {
            LoaderError::Invalid(format!(
                "loading the source's rows inside Snowflake returned {returned:?}, not a row count: {error}"
            ))
        })?;
        if let Some(files) = unloaded.filter(|files| files.rows != rows) {
            return Err(LoaderError::Invalid(format!(
                "Snowflake loaded {rows} rows from the unloaded files in {}, but the unload reports {}; the batch is not committed",
                files.folder, files.rows
            )));
        }
        Ok(loaded(rows))
    }
}

/// One Snowflake Scripting block running `statements` in a transaction and returning the rows they loaded: `COPY`s, which commit only when they load `expected` rows and roll back otherwise, or an `INSERT ... SELECT` (`expected` `None`), which commits.
pub(in crate::adapters) fn counted_block(statements: &[String], expected: Option<u64>) -> String {
    let mut body = vec![
        "DECLARE loaded INTEGER DEFAULT 0; copied INTEGER;".to_owned(),
        "BEGIN".to_owned(),
        "BEGIN TRANSACTION;".to_owned(),
    ];
    for statement in statements {
        body.push(format!("{statement};"));
        body.push(match expected {
            Some(_) => "SELECT COALESCE(SUM(\"rows_loaded\"), 0) INTO :copied FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));".to_owned(),
            None => "copied := SQLROWCOUNT;".to_owned(),
        });
        body.push("loaded := loaded + copied;".to_owned());
    }
    body.push(match expected {
        Some(expected) => format!("IF (loaded = {expected}) THEN COMMIT; ELSE ROLLBACK; END IF;"),
        None => "COMMIT;".to_owned(),
    });
    body.push("RETURN loaded;".to_owned());
    body.push("END;".to_owned());
    format!(
        "EXECUTE IMMEDIATE {}",
        SnowflakeSql.render_loader_value_literal(&body.join("\n"))
    )
}
