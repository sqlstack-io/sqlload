use std::collections::BTreeMap;

use chrono_tz::Tz;

use crate::column_parse::_helpers::lines::parse_line;
use crate::column_parse::models::ColumnParse;

/// The `parse` detail plan shows: a line per parsed column (by its loaded name), with what it loads as, its formats in the order tried, and, when a format reads times without an offset, the zone they are read in and where it comes from: the column's `tz`, the `timezone` setting (`timezone`, with where it is set), or UTC by default.
pub fn parse_lines(
    parses: &BTreeMap<String, ColumnParse>,
    timezone: Option<(Tz, &str)>,
) -> Vec<String> {
    let width = parses
        .keys()
        .map(|name| name.chars().count())
        .max()
        .unwrap_or(0);
    parses
        .iter()
        .map(|(name, parse)| parse_line((name, width), parse, timezone))
        .collect()
}
