//! The type a live column widens to so it can hold a desired one.

use crate::column_types::_helpers::fit::widened_type as narrowest_wider;
use crate::column_types::models::ColumnType;

/// The narrowest type holding both `live` and `desired` along SQLLoad's widening ladder, `None` when there is none.
pub fn widened_type(desired: &ColumnType, live: &ColumnType) -> Option<ColumnType> {
    narrowest_wider(desired, live)
}
