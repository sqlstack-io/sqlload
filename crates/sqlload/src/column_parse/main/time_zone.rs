use chrono_tz::Tz;

use crate::column_parse::_helpers::setting::zone_named;

/// The IANA time zone `name` names, such as `Europe/Paris`; or why not, worded to follow the setting's name.
pub fn time_zone(name: &str) -> Result<Tz, String> {
    zone_named(name)
}
