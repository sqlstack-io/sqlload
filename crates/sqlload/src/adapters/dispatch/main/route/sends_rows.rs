//! Whether a table's rows go in without files.

use crate::adapters::snowflake::main::stage::_sends_rows::sends_rows as snowflake_sends_rows;
use crate::adapters::snowflake::models::StageRoute;

/// Whether the planned `route` sends rows without files, into a staging table that holds them as text, so a `replace` copies them into a new table rather than building it in place.
pub fn sends_rows(route: Option<&StageRoute>) -> bool {
    snowflake_sends_rows(route)
}
