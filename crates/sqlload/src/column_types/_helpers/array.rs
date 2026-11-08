//! `array(<element>)` types.

use crate::column_types::constants::{ARRAY_ELEMENTS, ARRAY_NAME};
use crate::column_types::errors::TypeError;
use crate::column_types::models::{ColumnType, ParsedType, Warehouse};

/// A type parser, for the element of an array.
pub(crate) type ParseType = fn(&str, &[Warehouse]) -> Result<ParsedType, TypeError>;

/// `written` as `array(<element>)`, its element parsed by `parse`; `None` when it is not an array type. The element must be a scalar type every destination holds in an array.
pub(crate) fn array_type(
    (written, destinations): (&str, &[Warehouse]),
    parse: ParseType,
) -> Option<Result<ParsedType, TypeError>> {
    let inner = array_inner(written)?;
    Some(element_type(inner, (written, destinations), parse))
}

/// The element type text of `array(<element>)`, written in any case.
fn array_inner(written: &str) -> Option<&str> {
    let open = written.find('(')?;
    (written[..open].trim().eq_ignore_ascii_case(ARRAY_NAME))
        .then(|| written[open + 1..].strip_suffix(')'))
        .flatten()
}

/// `array(<element>)` with `inner` as its element.
fn element_type(
    inner: &str,
    (written, destinations): (&str, &[Warehouse]),
    parse: ParseType,
) -> Result<ParsedType, TypeError> {
    let element = parse(inner, destinations).map_err(|error| TypeError::Malformed {
        written: written.to_owned(),
        reason: format!("its element type: {error}"),
    })?;
    if !ARRAY_ELEMENTS.contains(&element.column_type) && !decimal(&element.column_type) {
        return Err(TypeError::Unsupported {
            written: written.to_owned(),
            reason: format!(
                "cannot hold {} elements; an array holds bool, smallint, int, float, decimal(p,s), text, date, timestamp_tz, timestamp_ntz or uuid (use json for anything else)",
                element.column_type
            ),
        });
    }
    if element.column_type == ColumnType::TimestampTz && destinations.contains(&Warehouse::BigQuery)
    {
        return Err(TypeError::Unsupported {
            written: written.to_owned(),
            reason: "does not load into BigQuery yet, whose write API refuses arrays of zoned timestamps; pin array(timestamp_ntz), which holds UTC times, or json".to_owned(),
        });
    }
    let column_type = ColumnType::Array(Box::new(element.column_type));
    let compact: String = written
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    let written_as = (compact != column_type.to_string()).then(|| written.to_owned());
    Ok(ParsedType {
        column_type,
        written_as,
        warnings: element.warnings,
    })
}

fn decimal(column_type: &ColumnType) -> bool {
    matches!(column_type, ColumnType::Decimal { .. })
}
