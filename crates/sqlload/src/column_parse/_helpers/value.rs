//! Reading one value with a column's formats in turn.

use chrono_tz::Tz;

use crate::column_parse::_helpers::formats::{expected, shown};
use crate::column_parse::_helpers::read::read_format;
use crate::column_parse::models::{ColumnParse, FormatRead, ParsedTime, RawTime};

/// `raw` read by the first of `parse`'s formats that reads it, a time without an offset in `parse`'s `tz`, else `zone`; or why it doesn't convert, worded to follow the value (`which is not a timestamp in the format "%d/%m/%Y"`).
pub(crate) fn value_time(
    raw: RawTime<'_>,
    parse: &ColumnParse,
    zone: Tz,
) -> Result<ParsedTime, String> {
    let zone = parse.tz.unwrap_or(zone);
    for format in &parse.formats {
        match read_format(raw, format, (parse.kind, zone)) {
            FormatRead::Read(time) => return Ok(time),
            FormatRead::Failed(reason) => return Err(reason),
            FormatRead::NoMatch => {}
        }
    }
    Err(format!("which is not {}", expected(parse)))
}

/// `raw` as an error shows it: text quoted and cut short, a number as it is.
pub(crate) fn raw_shown(raw: RawTime<'_>) -> String {
    match raw {
        RawTime::Text(text) => format!("{:?}", shown(text)),
        RawTime::Int(value) => value.to_string(),
        RawTime::Float(value) => value.to_string(),
    }
}
