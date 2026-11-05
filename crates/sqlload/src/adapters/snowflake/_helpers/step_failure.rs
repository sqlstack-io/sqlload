use crate::adapters::snowflake::constants::{
    NOT_FOUND_CODES, PRIVILEGE_CODES, REFUSAL_PHRASES, REFUSING_HTTP_STATUSES,
};
use crate::adapters::snowflake::models::StepFailure;
use crate::http::errors::ApiError;
use crate::stage_api::constants::NON_S3_STAGE;

/// What a failed probe says: a missing privilege, a missing object or a stage not on S3 makes a step unusable (a refusing HTTP status covers Snowpipe Streaming, whose host answers with no Snowflake code, and a refusal's wording covers a code not listed); anything else, such as the network or a server failure, fails the plan.
pub(crate) fn step_failure(error: &ApiError) -> StepFailure {
    let refused = match error {
        ApiError::Status {
            status,
            code,
            message,
        } => {
            let wording = message.to_ascii_lowercase();
            REFUSAL_PHRASES
                .iter()
                .any(|phrase| wording.contains(phrase))
                || PRIVILEGE_CODES.contains(&code.as_str())
                || NOT_FOUND_CODES.contains(&code.as_str())
                || REFUSING_HTTP_STATUSES.contains(status)
        }
        ApiError::Protocol(message) => message.starts_with(NON_S3_STAGE),
        _ => false,
    };
    match refused {
        true => StepFailure::Unusable(error.to_string()),
        false => StepFailure::Failed(error.to_string()),
    }
}
