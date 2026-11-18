//! Who a Snowflake source reads as, for `debug`.

use crate::snowflake_source::errors::SnowflakeSourceError;
use crate::snowflake_source::models::SnowflakeReader;
use crate::sql_api::main::execute_sql::execute_sql;

/// The reader's user and account, its warehouse and the Snowflake version.
pub async fn describe_source(reader: &SnowflakeReader) -> Result<String, SnowflakeSourceError> {
    let answer = execute_sql(
        &reader.sql,
        "SELECT CURRENT_USER() || '@' || CURRENT_ACCOUNT() || ', warehouse ' || COALESCE(CURRENT_WAREHOUSE(), 'none') || ', Snowflake ' || CURRENT_VERSION()",
    )
    .await?;
    Ok(answer
        .rows
        .first()
        .and_then(|row| row.first().cloned().flatten())
        .unwrap_or_default())
}
