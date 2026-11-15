use chrono_tz::Tz;

use crate::column_parse::models::{ColumnParse, ParsedTime, RawTime};

pub(crate) struct ParsedValueTestCase {
    pub(crate) description: &'static str,
    /// The column's parse as written, as JSON.
    pub(crate) setting: &'static str,
    pub(crate) raw: RawTime<'static>,
    /// The table's zone, for a time without an offset when the parse has no `tz`.
    pub(crate) zone: Tz,
    pub(crate) expected_outcome: Result<ParsedTime, &'static str>,
}

pub(crate) struct ColumnParseTestCase {
    pub(crate) description: &'static str,
    pub(crate) setting: &'static str,
    pub(crate) expected_outcome: Result<ColumnParse, &'static str>,
}

pub(crate) struct ParsedBatchTestCase {
    pub(crate) description: &'static str,
    /// Each column's parse as written, as a JSON object by column name.
    pub(crate) parses: &'static str,
    pub(crate) batch: fn() -> arrow_array::RecordBatch,
    pub(crate) rows_before: u64,
    /// The parsed batch as `name:type,...` then each row's values joined by `|`, rows by `; `.
    pub(crate) expected_outcome: Result<&'static str, &'static str>,
}

pub(crate) struct ParseLinesTestCase {
    pub(crate) description: &'static str,
    pub(crate) parses: &'static str,
    pub(crate) timezone: Option<(Tz, &'static str)>,
    pub(crate) expected_lines: &'static [&'static str],
}
