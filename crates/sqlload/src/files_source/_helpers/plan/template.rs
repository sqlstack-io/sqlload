//! The pieces of a `path_columns` pattern.

use crate::column_types::main::parse_type::parse_type;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::files_source::constants::DEFAULT_CAPTURE_TYPE;

/// The regex for one folder or file name of a template, and its captures in order.
pub(crate) fn segment_pattern(
    segment: &str,
    destinations: &[Warehouse],
) -> Result<(String, Vec<(String, ColumnType)>), String> {
    let mut pattern = String::new();
    let mut captures: Vec<(String, ColumnType)> = Vec::new();
    let mut rest = segment;
    while let Some(next) = rest.chars().next() {
        match next {
            '*' => {
                pattern.push_str("[^/]*");
                rest = &rest[1..];
            }
            '{' => {
                let Some((inside, after)) = rest[1..].split_once('}') else {
                    return Err(format!("{{ in {segment:?} is never closed"));
                };
                let (name, column_type) = capture(inside, destinations)?;
                pattern.push_str(value_pattern(&column_type));
                captures.push((name, column_type));
                rest = after;
            }
            '}' => return Err(format!("}} in {segment:?} closes nothing")),
            _ => {
                let end = rest.find(['*', '{', '}']).unwrap_or(rest.len());
                pattern.push_str(&regex::escape(&rest[..end]));
                rest = &rest[end..];
            }
        }
    }
    Ok((pattern, captures))
}

/// The column and type of one `{name}` or `{name:type}`.
fn capture(inside: &str, destinations: &[Warehouse]) -> Result<(String, ColumnType), String> {
    let (name, written) = inside
        .split_once(':')
        .unwrap_or((inside, DEFAULT_CAPTURE_TYPE));
    let name = name.trim();
    let valid = !name.is_empty()
        && name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_');
    if !valid {
        return Err(format!(
            "{{{inside}}} must name a column with letters, digits and _"
        ));
    }
    let parsed = parse_type(written, destinations).map_err(|error| format!("{name}: {error}"))?;
    Ok((name.to_owned(), parsed.column_type))
}

/// What a capture of `column_type` matches inside one segment: a date as `YYYY-MM-DD` or `YYYYMMDD`, an integer as ASCII digits (no sign), anything else as the shortest run of characters the rest of the template allows.
fn value_pattern(column_type: &ColumnType) -> &'static str {
    match column_type {
        ColumnType::Date => "([0-9]{4}-[0-9]{2}-[0-9]{2}|[0-9]{8})",
        ColumnType::Int | ColumnType::SmallInt => "([0-9]+)",
        _ => "([^/]+?)",
    }
}
