//! Parse a declared column type.

use crate::column_types::_helpers::array::array_type;
use crate::column_types::_helpers::destinations::{named_type, native_warnings};
use crate::column_types::_helpers::native::native_inner;
use crate::column_types::_helpers::readings::destination_type;
use crate::column_types::_helpers::syntax::split_type;
use crate::column_types::errors::TypeError;
use crate::column_types::models::{ColumnType, ParsedType, Warehouse};

/// Parse `text` as a column type for a table that loads into `destinations`. A name a warehouse reads its own way (`TIMESTAMP`, BigQuery's `BIGNUMERIC(p,s)`) is taken when every destination reads it as the same type.
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
    if let Some(parsed) = array_type((written, destinations), parse_type) {
        return parsed;
    }
    let syntax = split_type(written).map_err(malformed)?;
    let column_type = match destination_type(&syntax, written, destinations)? {
        Some(column_type) => column_type,
        None => named_type(&syntax, written, destinations)?,
    };
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
