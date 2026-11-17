//! An `AWS_*` variable as SQLLoad's S3 clients read it.

use crate::file_store::_helpers::aws_keys::environment_variable;

/// `name`'s value in the process environment; unset when empty, as SQLLoad's S3 clients read it.
pub fn aws_variable(name: &str) -> Option<String> {
    environment_variable(name)
}
