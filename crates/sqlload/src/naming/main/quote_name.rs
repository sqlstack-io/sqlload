//! Quote object names for SQL.

use crate::naming::_helpers::identifiers::identifier_problem;

/// The name as written in SQL: unchanged when it is a plain upper-case identifier, otherwise double-quoted.
pub fn quote_name(name: &str) -> String {
    match identifier_problem(name) {
        None => name.to_owned(),
        Some(_) => format!("\"{}\"", name.replace('"', "\"\"")),
    }
}
