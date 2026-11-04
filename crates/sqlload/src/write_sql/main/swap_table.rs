//! The table a `replace` builds before swapping it in.

use crate::write_sql::_helpers::promote::swap_table_name;
use crate::write_sql::models::TableName;

/// The swap table of the load staged in `staging`; dropped with the staging table if a load fails.
pub fn swap_table(staging: &TableName) -> TableName {
    swap_table_name(staging)
}
