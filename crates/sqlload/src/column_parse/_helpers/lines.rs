//! One parsed column's line in plan.

use chrono_tz::Tz;

use crate::column_parse::_helpers::formats::{format_list, reads_local};
use crate::column_parse::models::{BadValue, ColumnParse, ParseKind};

/// Column `name`'s line, its name padded to `width`: `updated  timestamp_tz from iso, "%m/%d/%Y"; times without an offset in Europe/Paris (its tz)`.
pub(crate) fn parse_line(
    (name, width): (&str, usize),
    parse: &ColumnParse,
    timezone: Option<(Tz, &str)>,
) -> String {
    let output = match parse.kind {
        ParseKind::Timestamp => "timestamp_tz",
        ParseKind::Date => "date",
    };
    let mut line = format!(
        "{name:<width$}  {output} from {}",
        format_list(&parse.formats)
    );
    if reads_local(parse) {
        let (zone, from) = match (parse.tz, timezone) {
            (Some(zone), _) => (zone, "its tz"),
            (None, Some((zone, from))) => (zone, from),
            (None, None) => (Tz::UTC, "the default, as neither tz nor timezone is set"),
        };
        line.push_str(&format!("; times without an offset in {zone} ({from})"));
    }
    if parse.on_error == BadValue::Null {
        line.push_str("; a value no format reads loads NULL");
    }
    line
}
