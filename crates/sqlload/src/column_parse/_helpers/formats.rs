//! What a column's formats are: checking a strftime format, whether it reads an offset or a time of day, and how errors and plan describe them.

use chrono::format::{Fixed, Item, Numeric, StrftimeItems};

use crate::column_parse::constants::{EPOCH_FORMATS, ISO_FORMAT, SHOWN_CHARS};
use crate::column_parse::models::{ColumnParse, EpochUnit, ParseKind, TimeFormat};

/// Why strftime `format` can't read values as written, if it can't: a directive it doesn't know, none at all (a typo for `"iso"` reads nothing), or `%f`, which reads nanoseconds here rather than Python's microseconds.
pub(crate) fn format_problem(format: &str) -> Option<String> {
    if StrftimeItems::new(format).any(|item| item == Item::Error) {
        return Some(format!(
            "format {format:?} has a directive that is not a strftime one such as %Y, %m, %d, %H, %M, %S or %z"
        ));
    }
    let directive = |item: Item<'_>| matches!(item, Item::Numeric(..) | Item::Fixed(_));
    if !StrftimeItems::new(format).any(directive) {
        return Some(format!(
            "format {format:?} has no directive; write \"iso\", an epoch format such as \"epoch_ms\", or a strftime format such as \"%d/%m/%Y\""
        ));
    }
    let nanoseconds = |item: Item<'_>| matches!(item, Item::Numeric(Numeric::Nanosecond, _));
    StrftimeItems::new(format).any(nanoseconds).then(|| {
        format!(
            "format {format:?} has %f, which reads nanoseconds here; write %.f for a fraction after a dot, as in \"%H:%M:%S%.f\" for 08:05:00.123456"
        )
    })
}

/// Whether `format` reads a UTC offset (`%z` and its forms, `%#z` taking `Z` too, or `%+`), so its values are instants rather than wall-clock times.
pub(crate) fn has_offset(format: &str) -> bool {
    format.contains("%#z")
        || StrftimeItems::new(format).any(|item| {
            matches!(
                item,
                Item::Fixed(
                    Fixed::TimezoneOffset
                        | Fixed::TimezoneOffsetColon
                        | Fixed::TimezoneOffsetDoubleColon
                        | Fixed::TimezoneOffsetTripleColon
                        | Fixed::TimezoneOffsetColonZ
                        | Fixed::TimezoneOffsetZ
                        | Fixed::RFC3339
                )
            )
        })
}

/// Whether `format` reads a time of day, rather than a date alone (which a timestamp reads as midnight).
pub(crate) fn has_time(format: &str) -> bool {
    StrftimeItems::new(format).any(|item| {
        matches!(
            item,
            Item::Numeric(
                Numeric::Hour
                    | Numeric::Hour12
                    | Numeric::Minute
                    | Numeric::Second
                    | Numeric::Timestamp,
                _
            ) | Item::Fixed(Fixed::RFC2822 | Fixed::RFC3339)
        )
    })
}

/// Whether some format of `parse` reads a time without an offset, which its zone then places.
pub(crate) fn reads_local(parse: &ColumnParse) -> bool {
    parse.kind == ParseKind::Timestamp
        && parse.formats.iter().any(|format| match format {
            TimeFormat::Iso => true,
            TimeFormat::Epoch(_) => false,
            TimeFormat::Strftime(text) => !has_offset(text),
        })
}

/// `formats` as written, in order: `iso, epoch_ms, "%m/%d/%Y"`.
pub(crate) fn format_list(formats: &[TimeFormat]) -> String {
    let written: Vec<String> = formats
        .iter()
        .map(|format| match format {
            TimeFormat::Iso => ISO_FORMAT.to_owned(),
            TimeFormat::Epoch(unit) => epoch_word(*unit).to_owned(),
            TimeFormat::Strftime(text) => format!("{text:?}"),
        })
        .collect();
    written.join(", ")
}

/// What a value of `parse` should have been, for an error: `a date in the format "%d/%m/%Y"`, or `a timestamp in any of the formats iso, "%m/%d/%Y"`.
pub(crate) fn expected(parse: &ColumnParse) -> String {
    let kind = match parse.kind {
        ParseKind::Timestamp => "timestamp",
        ParseKind::Date => "date",
    };
    match parse.formats.as_slice() {
        [TimeFormat::Epoch(unit)] => format!(
            "a number of {} since 1970-01-01 UTC, in the years 1 to 9999",
            unit_name(*unit)
        ),
        [TimeFormat::Iso] => format!("an ISO 8601 {kind}"),
        [TimeFormat::Strftime(text)] => format!("a {kind} in the format {text:?}"),
        formats => format!("a {kind} in any of the formats {}", format_list(formats)),
    }
}

/// An epoch unit's format word, such as `epoch_ms`.
pub(crate) fn epoch_word(unit: EpochUnit) -> &'static str {
    EPOCH_FORMATS
        .iter()
        .find(|(_, listed, _)| *listed == unit)
        .map_or("epoch_s", |(word, _, _)| word)
}

/// Microseconds per unit of an epoch.
pub(crate) fn micros_per(unit: EpochUnit) -> i64 {
    EPOCH_FORMATS
        .iter()
        .find(|(_, listed, _)| *listed == unit)
        .map_or(1, |(_, _, micros)| *micros)
}

fn unit_name(unit: EpochUnit) -> &'static str {
    match unit {
        EpochUnit::Seconds => "seconds",
        EpochUnit::Milliseconds => "milliseconds",
        EpochUnit::Microseconds => "microseconds",
    }
}

/// `text` as an error shows it, cut short.
pub(crate) fn shown(text: &str) -> String {
    match text.chars().count() > SHOWN_CHARS {
        true => format!("{}...", text.chars().take(SHOWN_CHARS).collect::<String>()),
        false => text.to_owned(),
    }
}
