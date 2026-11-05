//! Check a stage object against the connection's settings.

use crate::external_stage::models::{ShownStage, StageLookup};
use crate::snowflake_destination::models::{ExternalStage, StageObject};

/// Why the stage `shown` lists for `lookup` cannot be used: missing, on another URL, or (for the stage SQLLoad creates) with another storage integration.
pub fn stage_problem(
    stage: &ExternalStage,
    lookup: &StageLookup,
    shown: &[ShownStage],
) -> Option<String> {
    let url = stage.location.to_string();
    let Some(found) = shown
        .iter()
        .find(|found| found.name.eq_ignore_ascii_case(&lookup.name))
    else {
        return Some(format!(
            "stage {} does not exist or this role cannot see it ({})",
            lookup.name, lookup.query
        ));
    };
    let folder = |text: &str| format!("{}/", text.trim().trim_end_matches('/'));
    if folder(&found.url) != folder(&url) {
        return Some(format!(
            "stage {} reads {}, but the connection's stage url is {url}; point the stage at it or change the url",
            found.name, found.url
        ));
    }
    match &stage.object {
        StageObject::Managed { integration }
            if !found.storage_integration.eq_ignore_ascii_case(integration) =>
        {
            Some(format!(
                "stage {} uses storage integration {}, but the connection names {integration}; drop the stage so SQLLoad recreates it, or change storage_integration",
                found.name, found.storage_integration
            ))
        }
        _ => None,
    }
}
