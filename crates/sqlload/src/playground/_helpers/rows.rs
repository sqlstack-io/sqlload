//! The bundled CSV files' rows, and how a change's rows differ from the rows before it.

use std::collections::HashMap;

use crate::playground::constants::ROW_NOUN;

/// A bundled CSV file's header and rows, split at commas: the bundled files quote nothing.
fn split_rows(text: &str) -> (Vec<&str>, Vec<Vec<&str>>) {
    let mut lines = text.lines().filter(|line| !line.is_empty());
    let header: Vec<&str> = lines.next().unwrap_or_default().split(',').collect();
    let rows: Vec<Vec<&str>> = lines.map(|line| line.split(',').collect()).collect();
    (header, rows)
}

/// How many rows a bundled CSV file holds, below its header.
pub(crate) fn row_count(text: &str) -> u64 {
    split_rows(text).1.len() as u64
}

/// The columns of `new` that differ from `old`, as `column old → new`.
fn changed_columns(header: &[&str], old: &[&str], new: &[&str]) -> String {
    let mut changed: Vec<String> = Vec::new();
    for ((column, old), new) in header.iter().zip(old).zip(new) {
        if old != new {
            changed.push(format!("{column} {old} → {new}"));
        }
    }
    changed.join(", ")
}

/// A new row's values after its key, as `new: column value, …`.
fn new_row(header: &[&str], row: &[&str]) -> String {
    let values: Vec<String> = header
        .iter()
        .zip(row)
        .skip(1)
        .map(|(column, value)| format!("{column} {value}"))
        .collect();
    format!("new: {}", values.join(", "))
}

/// Each row of `change` as `(label, description)`, against the latest of `before` with its key.
pub(crate) fn changed_rows(before: &[&str], change: &str) -> Vec<(String, String)> {
    let mut known: HashMap<&str, Vec<&str>> = HashMap::new();
    for text in before {
        for row in split_rows(text).1 {
            known.insert(row[0], row);
        }
    }
    let (header, rows) = split_rows(change);
    let mut described: Vec<(String, String)> = Vec::new();
    for row in rows {
        let description = match known.get(row[0]) {
            Some(old) => changed_columns(&header, old, &row),
            None => new_row(&header, &row),
        };
        described.push((format!("{ROW_NOUN} {}", row[0]), description));
    }
    described
}

/// The lines `--change` prints for the rows of the file it added at `path`.
pub(crate) fn change_lines(path: &str, rows: &[(String, String)]) -> Vec<String> {
    let width = rows.iter().map(|(label, _)| label.len()).max().unwrap_or(0);
    let new = rows
        .iter()
        .filter(|(_, description)| description.starts_with("new:"))
        .count();
    let mut lines: Vec<String> = vec![format!(
        "added  {path}  {} changed, {new} new",
        rows.len() - new
    )];
    for (position, (label, description)) in rows.iter().enumerate() {
        let branch = match position + 1 == rows.len() {
            true => "└──",
            false => "├──",
        };
        lines.push(format!("{branch} {label:<width$}  {description}"));
    }
    lines.push(String::new());
    lines.push("Run `sqlload load` to merge it.".to_owned());
    lines
}
