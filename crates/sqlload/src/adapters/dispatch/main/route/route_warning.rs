//! The warning a planned route earns.

use crate::adapters::snowflake::main::stage::_route_warning::route_warning as snowflake_route_warning;
use crate::adapters::snowflake::models::StageRoute;

/// The warning plan shows for the chosen `route`, if any.
pub fn route_warning(route: Option<&StageRoute>) -> Option<&'static str> {
    snowflake_route_warning(route)
}
