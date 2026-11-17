//! Say when BigQuery's load job pacing, not `max_age`, decides how often a stream loads.

use crate::config::main::stream_settings::stream_settings;
use crate::config::models::Config;
use crate::delivery::constants::LOAD_JOB_INTERVAL;
use crate::planning::models::{LoadPath, StreamPlan};

/// A plan line for a stream loading with BigQuery load jobs whose shortest `max_age` (topic overrides included) is under [`LOAD_JOB_INTERVAL`]: its table takes at most one load job a minute, so that is how often it loads; `None` otherwise.
pub fn load_pacing(config: &Config, plan: &StreamPlan) -> Option<String> {
    if !matches!(plan.load_path, LoadPath::UploadLoad | LoadPath::GcsLoad) {
        return None;
    }
    let shortest = plan
        .stream
        .topics
        .iter()
        .map(|topic| stream_settings(config, topic).max_age)
        .min()
        .unwrap_or(config.batching.max_age);
    (shortest < LOAD_JOB_INTERVAL).then(|| {
        format!(
            "    loads at most every {}s, not every max_age {}s: BigQuery takes 1,500 load jobs per table a day, so one starts at most once a minute; max_age = \"5m\" is recommended",
            LOAD_JOB_INTERVAL.as_secs(),
            shortest.as_secs_f64()
        )
    })
}
