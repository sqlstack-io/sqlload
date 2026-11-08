//! Discover a Snowflake source's tables: catalog tables matching `include`, and query tables described without reading rows.

use futures_util::{StreamExt, TryStreamExt};
use globset::GlobSet;

use crate::http::errors::ApiError;
use crate::postgres_source::models::SourceTable;
use crate::snowflake_source::_helpers::catalog::{
    catalog_tables, described_columns, listed_tables, table_matcher,
};
use crate::snowflake_source::_helpers::type_map::quote_identifier;
use crate::snowflake_source::_helpers::unload_query::subquery;
use crate::snowflake_source::constants::{
    CATALOG_SQL, DESCRIBED_TABLES_MAX, MISSING_OBJECT, PARALLEL_DESCRIBES, SHOW_TABLES_SQL,
    SHOW_VIEWS_SQL,
};
use crate::snowflake_source::errors::SnowflakeSourceError;
use crate::snowflake_source::models::SnowflakeReader;
use crate::sql_api::main::execute_script_rows::execute_script_rows;
use crate::sql_api::main::execute_sql::execute_sql;

/// Every table and view of the connection's database whose `SCHEMA.TABLE` matches `include` and not `exclude` (ignoring case), then one table per query in `queries` (name and SQL), its columns as Snowflake describes the query's result.
pub async fn read_catalog(
    reader: &SnowflakeReader,
    (include, exclude): (&[String], &[String]),
    queries: &[(String, String)],
) -> Result<Vec<SourceTable>, SnowflakeSourceError> {
    let catalog = async {
        match include.is_empty() {
            true => Ok(Vec::new()),
            false => {
                matching_tables(reader, &table_matcher(include)?, &table_matcher(exclude)?).await
            }
        }
    };
    let described = futures_util::stream::iter(queries)
        .map(|(name, sql)| query_table(reader, name, sql))
        .buffered(PARALLEL_DESCRIBES)
        .try_collect::<Vec<SourceTable>>();
    let (mut tables, described) = tokio::try_join!(catalog, described)?;
    tables.extend(described);
    Ok(tables)
}

/// The matching tables, listed with `SHOW` and described a few at a time, which takes no warehouse and is quicker than the catalog views; many matching tables read the catalog views once instead. A table dropped between listing and describing is left out, as the catalog views would.
async fn matching_tables(
    reader: &SnowflakeReader,
    include: &GlobSet,
    exclude: &GlobSet,
) -> Result<Vec<SourceTable>, SnowflakeSourceError> {
    let (shown_tables, shown_views) = tokio::try_join!(
        execute_sql(&reader.sql, SHOW_TABLES_SQL),
        execute_sql(&reader.sql, SHOW_VIEWS_SQL)
    )?;
    let listed = match listed_tables(&[shown_tables, shown_views], include, exclude) {
        Some(listed) if listed.len() <= DESCRIBED_TABLES_MAX => listed,
        _ => {
            let rows = execute_sql(&reader.sql, CATALOG_SQL).await?;
            return Ok(catalog_tables(&rows, include, exclude));
        }
    };
    let described: Vec<Option<SourceTable>> = futures_util::stream::iter(listed)
        .map(|mut table| async move {
            let statement = format!(
                "DESCRIBE TABLE {}.{}",
                quote_identifier(&table.schema),
                quote_identifier(&table.name)
            );
            match execute_sql(&reader.sql, &statement).await {
                Ok(rows) => {
                    table.columns = described_columns(&rows);
                    Ok(Some(table))
                }
                Err(ApiError::Status { code, .. }) if code == MISSING_OBJECT => Ok(None),
                Err(error) => Err(error),
            }
        })
        .buffered(PARALLEL_DESCRIBES)
        .try_collect()
        .await?;
    Ok(described.into_iter().flatten().collect())
}

/// Query table `name`: its columns as Snowflake describes the result of `sql`, without reading rows.
async fn query_table(
    reader: &SnowflakeReader,
    name: &str,
    sql: &str,
) -> Result<SourceTable, SnowflakeSourceError> {
    let statements = vec![
        format!("SELECT * FROM {} LIMIT 0", subquery(sql)),
        "DESCRIBE RESULT LAST_QUERY_ID()".to_owned(),
    ];
    let described = execute_script_rows(&reader.sql, &statements)
        .await
        .map_err(|error| SnowflakeSourceError::Invalid(format!("query table {name}: {error}")))?;
    Ok(SourceTable {
        schema: String::new(),
        name: name.to_owned(),
        columns: described_columns(&described.last().cloned().unwrap_or_default()),
        primary_key: Vec::new(),
        indexed_columns: Vec::new(),
        estimated_rows: -1,
        estimated_bytes: -1,
        comment: None,
    })
}
