use chrono_tz::Tz;

/// One column's `parse` (a table's `parse` entry, or an `sl.field(parse=...)`): what its values become, the formats tried in turn (the first that reads a value wins), the zone of a time without an offset, and what a value no format reads does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnParse {
    pub kind: ParseKind,
    pub formats: Vec<TimeFormat>,
    /// `tz`: the zone a time without an offset is in; `None` takes the table's `timezone`, then UTC.
    pub tz: Option<Tz>,
    pub on_error: BadValue,
}

/// What a parsed column loads as: an instant (`timestamp_tz`, stored as UTC) or a date.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseKind {
    Timestamp,
    Date,
}

impl ParseKind {
    /// The type a column parsed this way loads as.
    pub fn column_type(self) -> crate::column_types::models::ColumnType {
        match self {
            Self::Timestamp => crate::column_types::models::ColumnType::TimestampTz,
            Self::Date => crate::column_types::models::ColumnType::Date,
        }
    }
}

/// One format a value may be in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeFormat {
    /// `"iso"`: ISO 8601 / RFC 3339 text, a `T` or a space before the time, seconds, a fraction and the offset optional; a date alone is midnight. Under `date`, only `YYYY-MM-DD`.
    Iso,
    /// `"epoch_s"`, `"epoch_ms"` or `"epoch_us"`: a number (or numeric text) of units since 1970-01-01 UTC.
    Epoch(EpochUnit),
    /// A strftime format, such as `"%m/%d/%Y %H:%M"`.
    Strftime(String),
}

/// The unit of an epoch format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochUnit {
    Seconds,
    Milliseconds,
    Microseconds,
}

/// What a value that `parse` can't convert does: fail the read, naming the column, the row and the value, or load NULL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BadValue {
    #[default]
    Fail,
    Null,
}

/// A value as its source holds it, for `parse` to read.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RawTime<'a> {
    Text(&'a str),
    Int(i64),
    Float(f64),
}

/// What a value parsed to: an instant, as microseconds since 1970-01-01 UTC, or a date, as days since then.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParsedTime {
    Instant(i64),
    Day(i32),
}

/// ISO 8601 text as read: a date alone, a wall-clock time, or an instant with its offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsoTime {
    Day(chrono::NaiveDate),
    Local(chrono::NaiveDateTime),
    Offset(chrono::DateTime<chrono::FixedOffset>),
}

/// What one format made of a value: nothing (the next format is tried), a time, or a definite failure, such as a local time its zone skips.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatRead {
    NoMatch,
    Read(ParsedTime),
    Failed(String),
}
