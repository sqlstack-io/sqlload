//! A Snowpipe Streaming client for a schema of this account.

use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::snowflake::models::SnowflakeDestination;
use crate::adapters::snowflake::models::SnowflakeSql;
use crate::streaming_api::models::StreamingClient;

/// A streaming client for the schema of `table`.
pub(in crate::adapters) fn streaming_client(
    snowflake: &SnowflakeDestination,
    table: &TableName,
) -> StreamingClient {
    let stored = |name: &str| SnowflakeSql.stored_name(name);
    let mut config = snowflake.config.clone();
    config.database = stored(table.database.as_deref().unwrap_or(&snowflake.database));
    config.schema = stored(&table.schema);
    StreamingClient::new(snowflake.http.clone(), snowflake.auth.clone(), &config)
}
