use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_cast::cast;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::DataType;
use chrono::{DateTime, NaiveDate};

use crate::column_parse::main::column_parse::column_parse;
use crate::column_parse::models::{ColumnParse, ParsedTime};

/// RFC 3339 `text` as a parsed instant.
pub(crate) fn instant(text: &str) -> ParsedTime {
    ParsedTime::Instant(
        DateTime::parse_from_rfc3339(text)
            .expect("an RFC 3339 instant")
            .timestamp_micros(),
    )
}

/// `YYYY-MM-DD` as a parsed date.
pub(crate) fn day(text: &str) -> ParsedTime {
    let days = NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .expect("a date")
        .signed_duration_since(DateTime::UNIX_EPOCH.date_naive())
        .num_days();
    ParsedTime::Day(i32::try_from(days).expect("a day in range"))
}

/// A JSON object of each column's parse as written, read.
pub(crate) fn parses(text: &str) -> BTreeMap<String, ColumnParse> {
    let settings: BTreeMap<String, serde_json::Value> =
        serde_json::from_str(text).expect("a JSON object of parses");
    settings
        .into_iter()
        .map(|(name, setting)| (name, column_parse(&setting).expect("a parse")))
        .collect()
}

/// A batch of `columns`.
pub(crate) fn batch_of(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter(columns).expect("a batch")
}

/// `batch` as `name:type,...` then its rows, values joined by `|` (NULL for a null), rows by `; `.
pub(crate) fn rendered(batch: &RecordBatch) -> String {
    let schema = batch.schema();
    let header: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| format!("{}:{}", field.name(), field.data_type()))
        .collect();
    let options = FormatOptions::default().with_null("NULL");
    let texts: Vec<Vec<String>> = batch
        .columns()
        .iter()
        .map(|column| {
            let text: ArrayRef = Arc::new(cast(column, &DataType::Utf8).expect("text"));
            let formatter = ArrayFormatter::try_new(text.as_ref(), &options).expect("a formatter");
            (0..batch.num_rows())
                .map(|row| formatter.value(row).to_string())
                .collect()
        })
        .collect();
    let rows: Vec<String> = (0..batch.num_rows())
        .map(|row| {
            texts
                .iter()
                .map(|column| column[row].clone())
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    format!("{} / {}", header.join(","), rows.join("; "))
}

/// The EAM's formats in Paris, as a JSON object of parses.
pub(crate) const EAM: &str = r#"{"updated": {"timestamp": ["iso", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y"], "tz": "Europe/Paris"}}"#;

/// Text values as a column.
pub(crate) fn text(values: &[Option<&str>]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

/// Ids and times in the EAM's formats, around the March clock change in Paris, and a NULL.
pub(crate) fn eam_rows() -> RecordBatch {
    batch_of(vec![
        (
            "id",
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
        ),
        (
            "updated",
            text(&[
                Some("2026-03-29T00:15:00Z"),
                Some("03/29/2026 01:30:00"),
                Some("03/29/2026 03:30"),
                Some("03/29/2026"),
                None,
            ]),
        ),
    ])
}

/// An ISO instant, then text no EAM format reads.
pub(crate) fn one_bad_row() -> RecordBatch {
    batch_of(vec![(
        "updated",
        text(&[Some("2026-03-29T00:15:00Z"), Some("31.03.2026")]),
    )])
}

/// A files source's batch: its second row, row 7 of `data/a.csv`, holds text no format reads.
pub(crate) fn one_bad_file_row() -> RecordBatch {
    batch_of(vec![
        ("updated", text(&[Some("03/29/2026"), Some("31.03.2026")])),
        (
            "_sqlload_source_file",
            text(&[Some("data/a.csv"), Some("data/a.csv")]),
        ),
        (
            "_sqlload_source_row",
            Arc::new(Int64Array::from(vec![6, 7])) as ArrayRef,
        ),
    ])
}

/// Epoch milliseconds as integers and epoch seconds as floats.
pub(crate) fn epochs() -> RecordBatch {
    batch_of(vec![
        (
            "ms",
            Arc::new(Int64Array::from(vec![Some(1_700_000_000_123), None])) as ArrayRef,
        ),
        (
            "s",
            Arc::new(Float64Array::from(vec![1_700_000_000.5, 0.0])) as ArrayRef,
        ),
    ])
}

/// Dates in a US format and in ISO.
pub(crate) fn dates() -> RecordBatch {
    batch_of(vec![(
        "due",
        text(&[Some("12/31/2026"), Some("2026-01-02")]),
    )])
}

/// An instant already, as the crossing types text with an offset.
pub(crate) fn instants() -> RecordBatch {
    batch_of(vec![(
        "updated",
        Arc::new(TimestampMicrosecondArray::from(vec![0_i64]).with_timezone("+02:00")) as ArrayRef,
    )])
}

/// A time without a zone, as a Python `datetime` crosses.
pub(crate) fn wall_clock() -> RecordBatch {
    batch_of(vec![(
        "updated",
        Arc::new(TimestampMicrosecondArray::from(vec![0_i64])) as ArrayRef,
    )])
}

/// Booleans, which hold no times.
pub(crate) fn flags() -> RecordBatch {
    batch_of(vec![(
        "updated",
        Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
    )])
}
