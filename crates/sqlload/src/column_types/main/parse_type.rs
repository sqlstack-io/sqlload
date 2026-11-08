//! Parse a declared column type.

use crate::column_types::_helpers::destinations::{check_destinations, native_warnings};
use crate::column_types::_helpers::names::resolve_name;
use crate::column_types::_helpers::native::native_inner;
use crate::column_types::_helpers::suggest::nearest_portable_name;
use crate::column_types::_helpers::syntax::split_type;
use crate::column_types::constants::{ARRAY_ELEMENTS, ARRAY_NAME};
use crate::column_types::errors::TypeError;
use crate::column_types::models::{ColumnType, ParsedType, Warehouse};

/// Parse `text` as a column type for a table that loads into `destinations`.
pub fn parse_type(text: &str, destinations: &[Warehouse]) -> Result<ParsedType, TypeError> {
    let written = text.trim();
    let malformed = |reason: String| TypeError::Malformed {
        written: written.to_owned(),
        reason,
    };
    if let Some(inner) = native_inner(written) {
        let name = inner.map_err(malformed)?;
        let warnings = native_warnings(&name, destinations);
        return Ok(ParsedType {
            column_type: ColumnType::Native(name),
            written_as: None,
            warnings,
        });
    }
    if let Some(inner) = array_inner(written) {
        return array_type(inner, (written, destinations));
    }
    let syntax = split_type(written).map_err(malformed)?;
    let Some((column_type, kind)) = resolve_name(&syntax, written)? else {
        return Err(TypeError::Unknown {
            written: written.to_owned(),
            suggestion: nearest_portable_name(&syntax.name),
        });
    };
    check_destinations(kind, &column_type, written, destinations)?;
    let compact: String = written
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    let written_as = (compact != column_type.to_string()).then(|| written.to_owned());
    Ok(ParsedType {
        column_type,
        written_as,
        warnings: Vec::new(),
    })
}

/// The element type text of `array(<element>)`, written in any case.
fn array_inner(written: &str) -> Option<&str> {
    let open = written.find('(')?;
    (written[..open].trim().eq_ignore_ascii_case(ARRAY_NAME))
        .then(|| written[open + 1..].strip_suffix(')'))
        .flatten()
}

/// `array(<element>)` with `inner` as its element, which must be a scalar type every destination holds in an array.
fn array_type(
    inner: &str,
    (written, destinations): (&str, &[Warehouse]),
) -> Result<ParsedType, TypeError> {
    let element = parse_type(inner, destinations).map_err(|error| TypeError::Malformed {
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
