//! Pivoting each row's list of cells into one column per named column.

use std::collections::HashMap;

use serde_json::Value;

use crate::http_source::_helpers::selector::{selected_node, selected_rows, selector};
use crate::http_source::models::{CellName, CellNames, Cells, Selector};

/// Why `cells` cannot pivot a job's rows, if it cannot: a pointer that is not one field, or a path that is not one value.
pub(crate) fn cells_problem(cells: &Cells) -> Option<String> {
    let named_by = match &cells.name {
        CellName::Listed { from, .. } => Some(from),
        CellName::Field { path } => Some(path),
        CellName::HeaderRow => None,
    };
    let paths = std::iter::once(&cells.pointer)
        .chain(&cells.value)
        .chain(named_by);
    for path in paths {
        let parsed = selector(Some(path)).map_err(|_| {
            format!("cells path {path:?} is not a path such as \"$.cells\" or \"$.value\"")
        });
        match parsed {
            Ok(parsed) if parsed.field.is_none() && parsed.within.is_empty() => {}
            Ok(_) => {
                return Some(format!(
                    "cells path {path:?} cannot pick from a list with [*]"
                ));
            }
            Err(problem) => return Some(problem),
        }
    }
    if cell_field(cells).is_none() {
        return Some(format!(
            "cells pointer {:?} must name one field of the row, such as \"$.cells\"",
            cells.pointer
        ));
    }
    match cells.value.is_empty() {
        true => Some("cells needs at least one value path, such as \"$.value\"".to_owned()),
        false => None,
    }
}

/// The row field `cells.pointer` names, such as `cells` for `$.cells`.
fn cell_field(cells: &Cells) -> Option<String> {
    let Ok(parsed) = selector(Some(&cells.pointer)) else {
        return None;
    };
    match parsed.rows.as_slice() {
        [field] if !field.starts_with('[') => Some(field.clone()),
        _ => None,
    }
}

/// The rows `rows` selects in `document` as a JSON list, each with its cells as columns, and the names they used: this page's own when it lists them, else `names` from an earlier page (for cells that name themselves, empty names once a page had cells). Until then, a page whose rows all lack the cells' field fails, catching a mistyped pointer, while later pages of blank rows load.
pub(crate) fn pivoted_rows(
    document: &Value,
    (rows, cells): (&Selector, &Cells),
    names: Option<CellNames>,
) -> Result<(String, Option<CellNames>), String> {
    let listed = selected_rows(document, rows);
    let Some(first) = listed.first() else {
        return Ok(("[]".to_owned(), names));
    };
    let field = cell_field(cells).unwrap_or_default();
    if names.is_none() && !listed.iter().any(|row| row.get(&field).is_some()) {
        return Err(format!(
            "no row has a {field} field, so cells pointer {:?} finds no cells; check it",
            cells.pointer
        ));
    }
    let (names, skip) = match (&cells.name, names) {
        (CellName::HeaderRow, Some(names)) | (CellName::Field { .. }, Some(names)) => (names, 0),
        (CellName::HeaderRow, None) => (header_names(first, (&field, cells))?, 1),
        (CellName::Field { .. }, None) => (CellNames::default(), 0),
        (CellName::Listed { .. }, previous) => (listed_names(document, cells, previous)?, 0),
    };
    let mut pivoted: Vec<String> = Vec::new();
    for (index, row) in listed.iter().enumerate().skip(skip) {
        let row = match &cells.name {
            CellName::Field { path } => self_named_row(row, (&field, cells), path),
            CellName::Listed { .. } | CellName::HeaderRow => {
                pivoted_row(row, (&field, cells), &names)
            }
        };
        pivoted.push(row.map_err(|problem| format!("row {}: {problem}", index + 1))?);
    }
    Ok((format!("[{}]", pivoted.join(",")), Some(names)))
}

/// The column names the response lists in `document` at `sl.listed`'s path, or `previous` when this page lists none.
fn listed_names(
    document: &Value,
    cells: &Cells,
    previous: Option<CellNames>,
) -> Result<CellNames, String> {
    let from = listed_from(cells);
    match selected_node(document, &selector(Some(&from))?) {
        Some(Value::Array(columns)) => column_names(columns, cells, &from),
        Some(Value::Null) | None => previous.ok_or_else(|| {
            format!("cells name {from}: the response lists no columns, and no page before it did")
        }),
        Some(_) => Err(format!("cells name {from} is not a list of columns")),
    }
}

/// Where `sl.listed` finds the columns, such as `$.columns`; empty for any other name source.
fn listed_from(cells: &Cells) -> String {
    match &cells.name {
        CellName::Listed { from, .. } => from.clone(),
        CellName::HeaderRow | CellName::Field { .. } => String::new(),
    }
}

/// The field of each cell holding its column's id (`sl.listed`'s `key`); `None` when cells match columns by position.
fn cell_key(cells: &Cells) -> Option<&String> {
    match &cells.name {
        CellName::Listed { key, .. } => key.as_ref(),
        CellName::HeaderRow | CellName::Field { .. } => None,
    }
}

/// Each listed column's name (at `name_key`, or the column itself) and id (at `id_key`, or its position when cells match by position).
fn column_names(columns: &[Value], cells: &Cells, source: &str) -> Result<CellNames, String> {
    let CellName::Listed {
        key,
        id_key,
        name_key,
        ..
    } = &cells.name
    else {
        return Ok(CellNames::default());
    };
    let mut found: Vec<(String, String)> = Vec::new();
    for (position, column) in columns.iter().enumerate() {
        let name = match name_key {
            Some(name_key) => column.get(name_key),
            None => Some(column),
        };
        let name = name
            .and_then(text_of)
            .filter(|name| !name.trim().is_empty());
        let at = name_key
            .as_ref()
            .map_or(String::new(), |name_key| format!(" at {name_key:?}"));
        let name =
            name.ok_or_else(|| format!("column {} of {source} has no name{at}", position + 1))?;
        let id = match key {
            Some(_) => column
                .get(id_key)
                .and_then(text_of)
                .ok_or_else(|| format!("column {name} of {source} has no id at {id_key:?}"))?,
            None => position.to_string(),
        };
        found.push((id, name));
    }
    named(found, source)
}

/// The names the header row's cells hold, matched by position; empty cells after the last name (a formatted row) name nothing.
fn header_names(row: &Value, (field, cells): (&str, &Cells)) -> Result<CellNames, String> {
    let mut found: Vec<(String, String)> = Vec::new();
    let mut unnamed: Option<usize> = None;
    for (position, cell) in row_cells(row, field)?.iter().enumerate() {
        let name = text_of(&cell_value(cell, cells)?).filter(|name| !name.trim().is_empty());
        let Some(name) = name else {
            unnamed = unnamed.or(Some(position));
            continue;
        };
        if let Some(blank) = unnamed {
            return Err(format!("the header row's cell {} has no name", blank + 1));
        }
        found.push((position.to_string(), name));
    }
    named(found, "the header row")
}

/// `found` (id, name) pairs as names in order, refusing a repeated id or a name repeated ignoring case.
fn named(found: Vec<(String, String)>, source: &str) -> Result<CellNames, String> {
    let mut names = CellNames::default();
    let mut lowered: HashMap<String, String> = HashMap::new();
    for (id, name) in found {
        if let Some(earlier) = lowered.insert(name.to_ascii_lowercase(), name.clone()) {
            return Err(format!(
                "{source} names two columns {earlier} and {name}, which load as one column"
            ));
        }
        if names
            .positions
            .insert(id.clone(), names.names.len())
            .is_some()
        {
            return Err(format!("{source} gives the id {id} to two columns"));
        }
        names.names.push(name);
    }
    Ok(names)
}

/// `row` as a JSON object: its own fields but `field`, in key order, then one per name in `names`, from its cells, NULL when it has no cell for it.
fn pivoted_row(
    row: &Value,
    (field, cells): (&str, &Cells),
    names: &CellNames,
) -> Result<String, String> {
    let Value::Object(object) = row else {
        return Err("is not an object, so it has no cells".to_owned());
    };
    let mut values: Vec<Option<Value>> = vec![None; names.names.len()];
    for (position, cell) in row_cells(row, field)?.iter().enumerate() {
        let id = cell_id(cell, cells, position)?;
        let value = cell_value(cell, cells)?;
        let Some(at) = names.positions.get(&id) else {
            match value.is_null() {
                true => continue,
                false => return Err(unknown_cell(&id, cells)),
            }
        };
        if values[*at].replace(value).is_some() {
            return Err(format!(
                "it has two cells for the column {}",
                names.names[*at]
            ));
        }
    }
    let cell_columns: Vec<(String, Value)> = names
        .names
        .iter()
        .cloned()
        .zip(values.into_iter().map(|value| value.unwrap_or(Value::Null)))
        .collect();
    joined_row(object, field, cell_columns)
}

/// `row` as a JSON object: its own fields but `field`, in key order, then one per cell, named by the cell's own name at `path`, in cell order. Two cells naming one column (ignoring case) fail, and so does a cell without a name.
fn self_named_row(
    row: &Value,
    (field, cells): (&str, &Cells),
    path: &str,
) -> Result<String, String> {
    let Value::Object(object) = row else {
        return Err("is not an object, so it has no cells".to_owned());
    };
    let at = selector(Some(path))?;
    let mut cell_columns: Vec<(String, Value)> = Vec::new();
    let mut lowered: HashMap<String, String> = HashMap::new();
    for (position, cell) in row_cells(row, field)?.iter().enumerate() {
        let name = selected_node(cell, &at)
            .and_then(text_of)
            .filter(|name| !name.trim().is_empty())
            .ok_or_else(|| format!("its cell {} has no name at {path}", position + 1))?;
        if let Some(earlier) = lowered.insert(name.to_ascii_lowercase(), name.clone()) {
            return Err(match earlier == name {
                true => format!("it has two cells for the column {name}"),
                false => format!(
                    "it has cells for the columns {earlier} and {name}, which load as one column"
                ),
            });
        }
        cell_columns.push((name, cell_value(cell, cells)?));
    }
    joined_row(object, field, cell_columns)
}

/// `object`'s own fields but `field`, in key order, then `cell_columns`, as a JSON object; a cell column named like one of the row's own fields (ignoring case) fails.
fn joined_row(
    object: &serde_json::Map<String, Value>,
    field: &str,
    cell_columns: Vec<(String, Value)>,
) -> Result<String, String> {
    let own: Vec<(&String, &Value)> = object.iter().filter(|(name, _)| *name != field).collect();
    let mut members: Vec<String> = own
        .iter()
        .map(|(name, value)| member(name, value))
        .collect();
    for (name, value) in cell_columns {
        if let Some((clash, _)) = own.iter().find(|(own, _)| own.eq_ignore_ascii_case(&name)) {
            return Err(format!(
                "its own field {clash} has the name of the cells' column {name}, so the two would load as one column"
            ));
        }
        members.push(member(&name, &value));
    }
    Ok(format!("{{{}}}", members.join(",")))
}

/// Why a cell with id `id` cannot load: no listed column has it.
fn unknown_cell(id: &str, cells: &Cells) -> String {
    let source = match &cells.name {
        CellName::Listed { from, .. } => from.clone(),
        CellName::HeaderRow | CellName::Field { .. } => "the header row".to_owned(),
    };
    match cell_key(cells) {
        Some(key) => format!("a cell's {key} {id} is not the id of a column {source} names"),
        None => format!(
            "it has a cell at position {}, past the columns {source} names",
            id.parse::<usize>().map_or(0, |position| position + 1)
        ),
    }
}

/// `"name":value` as JSON text.
fn member(name: &str, value: &Value) -> String {
    format!("{}:{value}", Value::String(name.to_owned()))
}

/// The cells of `row` at its `field`: none when missing or null.
fn row_cells(row: &Value, field: &str) -> Result<Vec<Value>, String> {
    match row.get(field) {
        Some(Value::Array(items)) => Ok(items.clone()),
        Some(Value::Null) | None => Ok(Vec::new()),
        Some(_) => Err(format!("its {field} is not a list of cells")),
    }
}

/// The cell's column id: its `sl.listed` `key` as text, or its `position` when cells match by position.
fn cell_id(cell: &Value, cells: &Cells, position: usize) -> Result<String, String> {
    match cell_key(cells) {
        Some(key) => cell
            .get(key)
            .and_then(text_of)
            .ok_or_else(|| format!("its cell {} has no {key}", position + 1)),
        None => Ok(position.to_string()),
    }
}

/// The cell's value: the first of `cells.value`'s paths present and not null in it, else null.
fn cell_value(cell: &Value, cells: &Cells) -> Result<Value, String> {
    for path in &cells.value {
        let found = selected_node(cell, &selector(Some(path))?).filter(|value| !value.is_null());
        if let Some(found) = found {
            return Ok(found.clone());
        }
    }
    Ok(Value::Null)
}

/// A name or id as text: a string as itself, a number or boolean as JSON; `None` for anything else.
fn text_of(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(_) | Value::Bool(_) => Some(value.to_string()),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}
