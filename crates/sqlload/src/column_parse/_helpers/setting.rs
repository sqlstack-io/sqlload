//! Reading one column's `parse` as written: an epoch word, or a table of `timestamp` or `date` formats with `tz` and `on_error`.

use chrono_tz::Tz;
use serde_json::{Map, Value};

use crate::column_parse::_helpers::formats::format_problem;
use crate::column_parse::constants::{EPOCH_FORMATS, ISO_FORMAT, PARSE_KEYS};
use crate::column_parse::models::{BadValue, ColumnParse, ParseKind, TimeFormat};

/// `"epoch_s"`, `"epoch_ms"` or `"epoch_us"` as a timestamp parse; `None` for any other word.
pub(crate) fn epoch_parse(word: &str) -> Option<ColumnParse> {
    let (_, unit, _) = EPOCH_FORMATS
        .iter()
        .find(|(listed, _, _)| *listed == word)?;
    Some(ColumnParse {
        kind: ParseKind::Timestamp,
        formats: vec![TimeFormat::Epoch(*unit)],
        tz: None,
        on_error: BadValue::Fail,
    })
}

/// A `parse` table: exactly one of `timestamp` and `date`, each a format or a list of them, and optionally `tz` (timestamps only) and `on_error`.
pub(crate) fn table_parse(fields: &Map<String, Value>) -> Result<ColumnParse, String> {
    if let Some(key) = fields
        .keys()
        .find(|key| !PARSE_KEYS.contains(&key.as_str()))
    {
        return Err(format!(
            "takes timestamp or date, with tz and on_error, not {key}"
        ));
    }
    let (kind, listed) = match (fields.get("timestamp"), fields.get("date")) {
        (Some(listed), None) => (ParseKind::Timestamp, listed),
        (None, Some(listed)) => (ParseKind::Date, listed),
        (Some(_), Some(_)) => return Err("takes timestamp or date, not both".to_owned()),
        (None, None) => {
            return Err(
                "needs timestamp or date, with the formats to try, such as timestamp = [\"iso\", \"%d/%m/%Y %H:%M\"]"
                    .to_owned(),
            );
        }
    };
    let formats = format_texts(listed)?
        .into_iter()
        .map(|text| time_format(text, kind))
        .collect::<Result<Vec<_>, String>>()?;
    let tz = match (fields.get("tz"), kind) {
        (None, _) => None,
        (Some(_), ParseKind::Date) => {
            return Err(
                "tz is for timestamps; a date has no time of day to place in a zone".to_owned(),
            );
        }
        (Some(Value::String(name)), ParseKind::Timestamp) => {
            Some(zone_named(name).map_err(|problem| format!("tz {problem}"))?)
        }
        (Some(other), ParseKind::Timestamp) => {
            return Err(format!(
                "tz takes a time zone name such as \"Europe/Paris\", not {other}"
            ));
        }
    };
    Ok(ColumnParse {
        kind,
        formats,
        tz,
        on_error: fields
            .get("on_error")
            .map_or(Ok(BadValue::Fail), bad_value)?,
    })
}

/// The time zone `name` names, such as `Europe/Paris`; why not, for an error after the setting's name.
pub(crate) fn zone_named(name: &str) -> Result<Tz, String> {
    name.parse::<Tz>().map_err(|_| {
        format!(
            "{name:?} is not an IANA time zone such as \"Europe/Paris\" or \"America/New_York\""
        )
    })
}

/// Why `setting` can't be a column's parse, for one that is neither a word nor a table.
pub(crate) fn shape_problem(setting: &Value) -> String {
    format!(
        "needs \"epoch_s\", \"epoch_ms\" or \"epoch_us\", or timestamp or date with a format or a list of formats (and tz and on_error), not {setting}"
    )
}

/// A format or a non-empty list of formats, as text.
fn format_texts(listed: &Value) -> Result<Vec<&str>, String> {
    let problem = || {
        format!(
            "takes a format or a list of formats, such as [\"iso\", \"%m/%d/%Y %H:%M\"], not {listed}"
        )
    };
    match listed {
        Value::String(text) => Ok(vec![text.as_str()]),
        Value::Array(items) if !items.is_empty() => items
            .iter()
            .map(|item| item.as_str().ok_or_else(problem))
            .collect(),
        _ => Err(problem()),
    }
}

/// One format of a `kind` parse: `iso`, an epoch word (timestamps only) or a strftime format.
fn time_format(text: &str, kind: ParseKind) -> Result<TimeFormat, String> {
    if text == ISO_FORMAT {
        return Ok(TimeFormat::Iso);
    }
    match (epoch_parse(text), kind) {
        (Some(_), ParseKind::Date) => Err(format!(
            "{text} reads an instant; list it under timestamp, not date"
        )),
        (Some(parse), ParseKind::Timestamp) => Ok(parse.formats[0].clone()),
        (None, _) => match format_problem(text) {
            Some(problem) => Err(problem),
            None => Ok(TimeFormat::Strftime(text.to_owned())),
        },
    }
}

fn bad_value(value: &Value) -> Result<BadValue, String> {
    match value.as_str() {
        Some("fail") => Ok(BadValue::Fail),
        Some("null") => Ok(BadValue::Null),
        _ => Err(format!(
            "on_error must be \"fail\" or \"null\", not {value}"
        )),
    }
}
