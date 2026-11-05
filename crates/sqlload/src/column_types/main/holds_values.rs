//! Whether one portable type holds another's values.

use crate::column_types::_helpers::fit::holds;
use crate::column_types::models::ColumnType;

/// Whether every value of `desired` is also a value of `live`.
pub fn holds_values(live: &ColumnType, desired: &ColumnType) -> bool {
    holds(live, desired)
}
