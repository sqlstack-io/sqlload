//! A Snowflake source's rows loaded by Snowflake itself: `COPY`ing its unloaded files, or selecting them in the source's own session.

use crate::adapters::contract::models::{DirectLoaded, UnloadedBatch, UnloadedSource};
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::snowflake::_helpers::write::direct_load::staged_as_text;
use crate::adapters::snowflake::constants::MAX_COPY_SQL_BYTES;
use crate::adapters::snowflake::main::copy::_stage_covers::stage_covers;
use crate::adapters::snowflake::main::copy::_unloaded_sql::unloaded_statements;
use crate::adapters::snowflake::main::stage::_stage_reference::stage_reference;
use crate::adapters::snowflake::models::{
    SnowflakeSql, SnowflakeWriter, StageRoute, UnloadedFrom, UnloadedLoad,
};
use crate::loader::errors::LoaderError;
use crate::sql_api::main::execute_sql::execute_sql;

impl SnowflakeWriter {
    /// Load `batch` inside Snowflake, where Arrow batches would go: the load's staging table, the build table, or the table itself. Unloaded files are `COPY`d from the external stage the writer loads through, which must cover them, as [`unloaded_requests`] sends them, and the batch fails unless they load as many rows as the unload reports; a query's rows are selected by one `INSERT ... SELECT`.
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
        let requests = unloaded_requests(
            &statements,
            unloaded.map(|files| (files.rows, files.keys.len())),
            target.direct,
        )
        .map_err(LoaderError::Invalid)?;
        let mut rows: u64 = 0;
        for request in &requests {
            let answer = execute_sql(&self.destination.sql, request).await?;
            let returned = answer
                .rows
                .first()
                .and_then(|row| row.first().cloned().flatten())
                .unwrap_or_default();
            rows += returned.parse::<u64>().map_err(|error| {
                LoaderError::Invalid(format!(
                    "loading the source's rows inside Snowflake returned {returned:?}, not a row count: {error}"
                ))
            })?;
        }
        if let Some(files) = unloaded.filter(|files| files.rows != rows) {
            return Err(LoaderError::Invalid(format!(
                "Snowflake loaded {rows} rows from the unloaded files in {}, but the unload reports {}; the batch is not committed",
                files.folder, files.rows
            )));
        }
        Ok(loaded(rows))
    }
}

/// The requests loading `statements`, each a [`counted_block`]: one, whose `COPY`s commit only when they load all of the unload's rows (`unloaded`: its rows and files), when it fits `MAX_COPY_SQL_BYTES`, as does an `INSERT ... SELECT` (`unloaded` `None`); otherwise several, each committing its `COPY`s into the staging or build table, which publishes only once their rows add up to the unload's, as the caller checks. A `direct` batch commits into the table itself, so it fails instead, before loading anything, as a part committed alone could not be taken back.
pub(in crate::adapters) fn unloaded_requests(
    statements: &[String],
    unloaded: Option<(u64, usize)>,
    direct: bool,
) -> Result<Vec<String>, String> {
    let Some((rows, files)) = unloaded else {
        return Ok(vec![counted_block(statements, false, None)]);
    };
    let whole = counted_block(statements, true, Some(rows));
    if whole.len() <= MAX_COPY_SQL_BYTES {
        return Ok(vec![whole]);
    }
    if direct {
        return Err(format!(
            "the unload wrote {files} files, whose COPYs are too long to run as one Snowflake statement, and this append commits into the table itself, so they can't be split. Fix: set cursor_column with a smaller batch_size, so each read unloads fewer files"
        ));
    }
    let mut groups: Vec<Vec<String>> = Vec::new();
    for statement in statements {
        let fits = groups.last().is_some_and(|group| {
            let mut grown = group.clone();
            grown.push(statement.clone());
            counted_block(&grown, true, None).len() <= MAX_COPY_SQL_BYTES
        });
        match (fits, groups.last_mut()) {
            (true, Some(group)) => group.push(statement.clone()),
            _ => groups.push(vec![statement.clone()]),
        }
    }
    Ok(groups
        .iter()
        .map(|group| counted_block(group, true, None))
        .collect())
}

/// One Snowflake Scripting block running `statements` in a transaction and returning the rows they loaded: `COPY`s (`copies`), counted from their results, which commit only when they load `expected` rows (when given) and roll back otherwise, or an `INSERT ... SELECT`, which commits.
fn counted_block(statements: &[String], copies: bool, expected: Option<u64>) -> String {
    let mut body = vec![
        "DECLARE loaded INTEGER DEFAULT 0; copied INTEGER;".to_owned(),
        "BEGIN".to_owned(),
        "BEGIN TRANSACTION;".to_owned(),
    ];
    for statement in statements {
        body.push(format!("{statement};"));
        body.push(match copies {
            true => "SELECT COALESCE(SUM(\"rows_loaded\"), 0) INTO :copied FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));".to_owned(),
            false => "copied := SQLROWCOUNT;".to_owned(),
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
