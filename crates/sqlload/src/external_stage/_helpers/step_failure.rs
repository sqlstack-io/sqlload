use crate::external_stage::constants::{NOT_FOUND_CODES, PRIVILEGE_CODES, REFUSING_HTTP_STATUSES};
use crate::external_stage::models::StepFailure;
use crate::http::errors::ApiError;

/// What a failed probe says: only a missing privilege or a missing object makes a step unusable (a refusing HTTP status covers Snowpipe Streaming, whose host answers with no Snowflake code); anything else, such as the network or a server failure, fails the plan.
pub(crate) fn step_failure(error: &ApiError) -> StepFailure {
    let refused = match error {
        ApiError::Status { status, code, .. } => {
            PRIVILEGE_CODES.contains(&code.as_str())
                || NOT_FOUND_CODES.contains(&code.as_str())
                || REFUSING_HTTP_STATUSES.contains(status)
        }
        _ => false,
    };
    match refused {
        true => StepFailure::Unusable(error.to_string()),
        false => StepFailure::Failed(error.to_string()),
    }
}
