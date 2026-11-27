//! Statements as the runner carries them out: sent as they are, or a publish step it carries out itself.

use crate::adapters::clickhouse::_helpers::dialect::publish::{
    engine_query, partition_key_columns_query, partition_key_query, partition_rows_query,
    replace_partition, sorted_insert, stageable_query,
};
use crate::adapters::clickhouse::constants::{
    ASIDE_SUFFIX, EMPTY_SORTING_KEY, EXCHANGE_UNSUPPORTED_CODES, LIGHTWEIGHT_DELETE_MODE,
    LIGHTWEIGHT_DELETE_REFUSED_CODE, MEMORY_LIMIT_EXCEEDED_CODE, NULLABLE_KEY_SETTING,
    PUBLISH_PREFIX,
};
use crate::adapters::clickhouse::models::{ClickHouseSql, PublishStep};
use crate::adapters::clickhouse::types::StepRunner;
use crate::adapters::contract::constants::SWAP_SUFFIX;
use crate::adapters::contract::types::SqlAdapter;
use crate::http::errors::ApiError;

/// The publish step `statement` stands for, if it is one.
pub(crate) fn publish_step(statement: &str) -> Result<Option<PublishStep>, ApiError> {
    let Some(json) = statement.strip_prefix(PUBLISH_PREFIX) else {
        return Ok(None);
    };
    serde_json::from_str(json)
        .map(Some)
        .map_err(|error| ApiError::Protocol(format!("a malformed publish step: {error}")))
}

/// Run one statement, or the exchange, partition replace, row delete or sorted insert it stands for; a partition publish here is a mistake, as it only follows from a write.
pub(crate) async fn run_step<R: StepRunner>(
    destination: &R,
    statement: &str,
) -> Result<(), R::Error> {
    match publish_step(statement)? {
        None => destination.run(statement).await,
        Some(PublishStep::Exchange {
            target,
            build,
            aside,
        }) => exchange(destination, (&target, &build, &aside)).await,
        Some(PublishStep::ReplacePartition {
            target,
            copy,
            partition,
        }) => replace_or_drop(destination, (&target, &copy, &partition)).await,
        Some(PublishStep::DeleteRows { table, condition }) => {
            delete_rows(destination, (&table, &condition)).await
        }
        Some(PublishStep::SortedInsert {
            database,
            table,
            columns,
            insert,
        }) => insert_sorted(destination, (&database, &table), Some(&columns), &insert).await,
        Some(PublishStep::FillBuild {
            target,
            build,
            swap,
        }) => fill_build(destination, (&target, &build), &swap).await,
        Some(PublishStep::Partitions { .. }) => Err(ApiError::Protocol(
            "a partition publish cannot run inside another".to_owned(),
        )
        .into()),
    }
}

/// `database`.`table` as SQL.
fn qualified(database: &str, table: &str) -> String {
    format!(
        "{}.{}",
        ClickHouseSql.render_identifier(database),
        ClickHouseSql.render_identifier(table)
    )
}

/// The first value of each row `query` answers.
async fn first_values<R: StepRunner>(
    destination: &R,
    query: String,
) -> Result<Vec<String>, R::Error> {
    Ok(destination
        .rows(&query)
        .await?
        .into_iter()
        .filter_map(|row| row.into_iter().next().flatten())
        .collect())
}

/// `database`.`table`'s partition key (empty when it has none) and the columns it reads.
async fn partition_key<R: StepRunner>(
    destination: &R,
    (database, table): (&str, &str),
) -> Result<(String, Vec<String>), R::Error> {
    let key = first_values(destination, partition_key_query(database, table)).await?;
    let key_columns =
        first_values(destination, partition_key_columns_query(database, table)).await?;
    Ok((key.into_iter().next().unwrap_or_default(), key_columns))
}

/// Run `insert` into `database`.`table`, ordered by the partition key the server reports for the table when every column it reads is among `columns` (every column when `None`; see [`sorted_insert`]); a sort that runs out of memory is replaced by the unsorted insert into the emptied table.
async fn insert_sorted<R: StepRunner>(
    destination: &R,
    (database, table): (&str, &str),
    columns: Option<&[String]>,
    insert: &str,
) -> Result<(), R::Error> {
    let (key, key_columns) = partition_key(destination, (database, table)).await?;
    let sorted = sorted_insert(insert, &key, &key_columns, columns.unwrap_or(&key_columns));
    match destination.run(&sorted).await {
        Ok(()) => Ok(()),
        Err(error)
            if sorted != insert
                && R::code(&error).as_deref() == Some(MEMORY_LIMIT_EXCEEDED_CODE) =>
        {
            tracing::debug!(%error, "the sorted insert ran out of memory; inserting unsorted");
            destination
                .run(&format!("TRUNCATE TABLE {}", qualified(database, table)))
                .await?;
            destination.run(insert).await
        }
        Err(error) => Err(error),
    }
}

/// Before `build`'s first rows (a database and a table): when it is partitioned by columns all among `inserted`, put in its place an unpartitioned table with its columns (`PARTITION BY tuple()`, as `CREATE TABLE ... AS` keeps a partition key the new engine clause does not set), sorted by that partition key (inside `tuple()`, as ClickHouse refuses some single keys bare, such as `substring(code, 1, 1)`, and a key of several expressions needs it), so each insert writes one part rather than one for every partition it fills (unless its storage cannot be stood in for, see [`stageable_query`]), which a large first load into fine partitions would make hundreds of thousands of; [`fill_build`] partitions the rows at the commit.
pub(crate) async fn stage_build<R: StepRunner>(
    destination: &R,
    (database, build): (&str, &str),
    inserted: &[String],
) -> Result<(), R::Error> {
    let (key, key_columns) = partition_key(destination, (database, build)).await?;
    if key_columns.is_empty()
        || !key_columns.iter().all(|column| inserted.contains(column))
        || first_values(destination, stageable_query(database, build))
            .await?
            .is_empty()
    {
        return Ok(());
    }
    let _held = destination.ddl().lock().await;
    let name = qualified(database, build);
    let swap = qualified(database, &format!("{build}{SWAP_SUFFIX}"));
    let aside = qualified(database, &format!("{build}{ASIDE_SUFFIX}"));
    for statement in [
        format!("DROP TABLE IF EXISTS {swap}"),
        format!(
            "CREATE TABLE {swap} AS {name} ENGINE = MergeTree PARTITION BY {EMPTY_SORTING_KEY} ORDER BY tuple({}) SETTINGS {NULLABLE_KEY_SETTING} = 1",
            key.trim()
        ),
    ] {
        destination.run(&statement).await?;
    }
    exchange(destination, (&name, &swap, &aside)).await?;
    destination.run(&format!("DROP TABLE {swap}")).await
}

/// Run outside the destination's schema-change lock (see [`SqlAdapter::unguarded_statements`](crate::adapters::contract::types::SqlAdapter::unguarded_statements)), which it takes only for its `CREATE` and `EXCHANGE`, so the copy holds up no other table. When `build`'s rows wait unpartitioned (see [`stage_build`]: `build`'s partition key reads no column and `target`'s does), move them into `swap`, created with `build`'s columns and `target`'s engine, keys and settings, by one insert ordered by the partition key, and exchange the two, so `build` holds them laid out as `target`; nothing otherwise.
async fn fill_build<R: StepRunner>(
    destination: &R,
    ((target_database, target), (database, build)): (&(String, String), &(String, String)),
    swap: &str,
) -> Result<(), R::Error> {
    let (_, staged) = partition_key(destination, (database, build)).await?;
    let (_, key) = partition_key(destination, (target_database, target)).await?;
    if !staged.is_empty() || key.is_empty() {
        return Ok(());
    }
    let engine = first_values(destination, engine_query(target_database, target)).await?;
    let engine = engine.first().ok_or_else(|| {
        ApiError::Protocol(format!(
            "{target_database}.{target} has no engine to build with"
        ))
    })?;
    let (name, swapped) = (qualified(database, build), qualified(database, swap));
    let held = destination.ddl().lock().await;
    for statement in [
        format!("DROP TABLE IF EXISTS {swapped}"),
        format!("CREATE TABLE {swapped} AS {name} ENGINE = {engine}"),
    ] {
        destination.run(&statement).await?;
    }
    drop(held);
    insert_sorted(
        destination,
        (database, swap),
        None,
        &format!("INSERT INTO {swapped} SELECT * FROM {name}"),
    )
    .await?;
    let aside = qualified(database, &format!("{build}{ASIDE_SUFFIX}"));
    let _held = destination.ddl().lock().await;
    exchange(destination, (&name, &swapped, &aside)).await
}

/// Swap `build` in for `target` with `EXCHANGE TABLES`, atomic in an `Atomic` database; where the database cannot exchange tables, by one `RENAME TABLE` moving `target` to `aside`, `build` to `target` and `aside` to `build`, so `build` holds the old table either way.
pub(crate) async fn exchange<R: StepRunner>(
    destination: &R,
    (target, build, aside): (&str, &str, &str),
) -> Result<(), R::Error> {
    let exchanged = destination
        .run(&format!("EXCHANGE TABLES {target} AND {build}"))
        .await;
    match exchanged {
        Ok(()) => Ok(()),
        Err(error)
            if R::code(&error)
                .is_some_and(|code| EXCHANGE_UNSUPPORTED_CODES.contains(&code.as_str())) =>
        {
            tracing::debug!(%error, "EXCHANGE TABLES is unsupported here; swapping by renames");
            destination
                .run(&format!(
                    "RENAME TABLE {target} TO {aside}, {build} TO {target}, {aside} TO {build}"
                ))
                .await
        }
        Err(error) => Err(error),
    }
}

/// Replace `target`'s `partition` with `copy`'s, or drop it when `copy` holds none of its rows, as ClickHouse 26.6 and later refuse to replace a partition from a table with no parts in it; either way one statement, so readers see the old partition or the new.
async fn replace_or_drop<R: StepRunner>(
    destination: &R,
    (target, copy, partition): (&str, &str, &str),
) -> Result<(), R::Error> {
    let counted = first_values(destination, partition_rows_query(copy, partition)).await?;
    let statement = match counted.first().map(String::as_str) {
        Some("0") => format!(
            "ALTER TABLE {target} DROP PARTITION ID {}",
            ClickHouseSql.render_loader_value_literal(partition)
        ),
        _ => replace_partition(target, copy, partition),
    };
    destination.run(&statement).await
}

/// Delete `table`'s rows matching `condition` with a lightweight `DELETE`, which only marks them (with [`LIGHTWEIGHT_DELETE_MODE`], and finished before it returns), or by rewriting their parts with `ALTER TABLE ... DELETE` where the table refuses one.
async fn delete_rows<R: StepRunner>(
    destination: &R,
    (table, condition): (&str, &str),
) -> Result<(), R::Error> {
    let deleted = destination
        .run(&format!(
            "DELETE FROM {table} WHERE {condition} SETTINGS {} = '{}', lightweight_deletes_sync = 2",
            LIGHTWEIGHT_DELETE_MODE.0, LIGHTWEIGHT_DELETE_MODE.1
        ))
        .await;
    match deleted {
        Ok(()) => Ok(()),
        Err(error) if R::code(&error).as_deref() == Some(LIGHTWEIGHT_DELETE_REFUSED_CODE) => {
            tracing::debug!(%error, "a lightweight DELETE is refused here; rewriting the parts");
            destination
                .run(&format!("ALTER TABLE {table} DELETE WHERE {condition}"))
                .await
        }
        Err(error) => Err(error),
    }
}
