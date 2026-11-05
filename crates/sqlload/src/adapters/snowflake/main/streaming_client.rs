//! A Snowpipe Streaming client for a schema of this account.

use crate::column_types::models::Warehouse;
use crate::adapters::snowflake::models::SnowflakeDestination;
use crate::streaming_api::models::StreamingClient;
use crate::adapters::contract::main::stored_name::stored_name;
use crate::adapters::contract::models::TableName;

/// A streaming client for the schema of `table`.
pub fn streaming_client(snowflake: &SnowflakeDestination, table: &TableName) -> StreamingClient {
    let stored = |name: &str| stored_name(name, Warehouse::Snowflake);
    let mut config = snowflake.config.clone();
    config.database = stored(table.database.as_deref().unwrap_or(&snowflake.database));
    config.schema = stored(&table.schema);
    StreamingClient::new(snowflake.http.clone(), snowflake.auth.clone(), &config)
}
