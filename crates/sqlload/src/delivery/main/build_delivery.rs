//! Choose and build a stream's delivery.

use crate::delivery::errors::DeliveryError;
use crate::delivery::models::{Delivery, DeliveryInputs, StageClients, Warehouse};
use crate::naming::models::Stream;
use crate::planning::models::LoadPath;

/// Snowpipe Streaming or Zerobus Ingest for the streaming paths, staged files for every other path.
pub fn build_delivery(inputs: &DeliveryInputs, stream: &Stream) -> Result<Delivery, DeliveryError> {
    let max_request_bytes = inputs.config.batching.max_request_bytes.0;
    match (&inputs.warehouse, inputs.path) {
        (Warehouse::Snowflake(snowflake), LoadPath::Streaming) => Ok(Delivery::streaming(
            snowflake.streaming.clone(),
            stream.clone(),
            max_request_bytes,
            inputs.oversized,
        )),
        (Warehouse::Snowflake(snowflake), path) => Delivery::stage(
            StageClients {
                http: inputs.http.clone(),
                auth: snowflake.auth.clone(),
                sql: snowflake.sql.clone(),
                driver: snowflake.driver.clone(),
                account_url: snowflake.account_url.clone(),
                in_flight: inputs.in_flight.clone(),
            },
            inputs.config.clone(),
            stream.clone(),
            path,
        ),
        (Warehouse::Databricks(databricks), LoadPath::Zerobus) => {
            let settings = databricks.zerobus.clone().ok_or_else(|| {
                DeliveryError::Fatal("Zerobus Ingest needs destination.zerobus_endpoint".to_owned())
            })?;
            Ok(Delivery::zerobus(
                settings,
                &inputs.config,
                stream,
                inputs.oversized,
            ))
        }
        (Warehouse::Databricks(databricks), _) => Delivery::copy(inputs, &databricks.api, stream),
    }
}
