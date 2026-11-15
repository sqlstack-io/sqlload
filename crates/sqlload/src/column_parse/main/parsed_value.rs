use chrono_tz::Tz;

use crate::column_parse::_helpers::value::value_time;
use crate::column_parse::models::{ColumnParse, ParsedTime, RawTime};

/// `raw` read by the first of `parse`'s formats that reads it, a time without an offset in `parse`'s `tz`, else `zone` (the table's `timezone`, else UTC); or why it doesn't convert, worded to follow the value (`which is not a date in the format "%d/%m/%Y"`).
pub fn parsed_value(raw: RawTime<'_>, parse: &ColumnParse, zone: Tz) -> Result<ParsedTime, String> {
    value_time(raw, parse, zone)
}
