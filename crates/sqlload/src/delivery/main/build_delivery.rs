//! Choose and build a stream's delivery.

use crate::delivery::errors::DeliveryError;
use crate::delivery::models::{Delivery, DeliveryInputs};
use crate::naming::models::Stream;
use crate::planning::models::LoadPath;

/// Streaming channels for [`LoadPath::Streaming`], staged files for every other path.
pub fn build_delivery(inputs: &DeliveryInputs, stream: &Stream) -> Result<Delivery, DeliveryError> {
    match inputs.path {
        LoadPath::Streaming => Ok(Delivery::streaming(
            inputs.streaming.clone(),
            stream.clone(),
            inputs.config.batching.max_request_bytes.0,
            inputs.oversized,
        )),
        path => Delivery::stage(
            inputs.stage.clone(),
            inputs.config.clone(),
            stream.clone(),
            path,
        ),
    }
}
