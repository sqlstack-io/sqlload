//! Recognise an object that is not there.

use crate::external_stage::constants::NOT_FOUND_CODES;
use crate::http::errors::ApiError;

/// Whether `error` says the object asked about does not exist (or this role cannot see it).
pub fn is_not_found(error: &ApiError) -> bool {
    matches!(error, ApiError::Status { code, .. } if NOT_FOUND_CODES.contains(&code.as_str()))
}
