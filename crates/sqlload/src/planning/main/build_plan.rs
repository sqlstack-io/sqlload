//! Decide which objects every stream needs and whether they already exist.

use crate::config::models::Config;
use crate::naming::models::Stream;
use crate::planning::_helpers::objects::stream_objects;
use crate::planning::main::load_path::load_path;
use crate::planning::models::{ExistingObjects, Plan, StreamPlan};

/// The objects each stream needs, compared with what exists.
pub fn build_plan(
    config: &Config,
    streams: &[Stream],
    existing: &ExistingObjects,
    uses_key_pair: bool,
) -> Plan {
    let path = load_path(config, uses_key_pair);
    Plan {
        streams: streams
            .iter()
            .map(|stream| StreamPlan {
                stream: stream.clone(),
                load_path: path,
                objects: stream_objects(config, stream, existing, path),
            })
            .collect(),
    }
}
