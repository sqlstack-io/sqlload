//! The checks before a Postgres write, apart from how their queries run: on SQLLoad's own session, or a caller's.

use crate::adapters::contract::models::WriteMode;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::postgres::models::{KeyIndexes, PostgresSql, ReferenceRow};

/// The merge key's columns as stored, sorted as `KEY_INDEX_SQL` compares them.
pub(crate) fn sorted_key(unique_key: &[String]) -> Vec<String> {
    let mut key: Vec<String> = unique_key
        .iter()
        .map(|column| PostgresSql.stored_name(column))
        .collect();
    key.sort();
    key
}

/// The key columns of `mode` that may hold NULL: those the source says may, unless the existing table's `indexes` say it declares them NOT NULL.
pub(crate) fn nullable_keys<'a>(mode: &'a WriteMode, indexes: Option<&KeyIndexes>) -> Vec<&'a str> {
    mode.matched_keys()
        .into_iter()
        .filter_map(|(key, nullable)| nullable.then_some(key))
        .filter(|key| {
            indexes.is_none_or(|indexes| !indexes.not_null.contains(&PostgresSql.stored_name(key)))
        })
        .collect()
}

/// The foreign keys in `rows` referencing `schema.name` that a write deleting its rows (`mode`) would carry into their tables (problems), or that would refuse it while their rows reference the rows it deletes (warnings); `replace` skips the table's own.
pub(crate) fn reference_outcome(
    rows: &[ReferenceRow],
    (schema, name): (&str, &str),
    mode: &WriteMode,
) -> (Vec<String>, Vec<String>) {
    let strategy = match mode {
        WriteMode::Replace => "replace",
        _ => "delete_insert",
    };
    let (mut problems, mut warnings) = (Vec::new(), Vec::new());
    for row in rows {
        let carried = match row.action.as_str() {
            "c" => Some("ON DELETE CASCADE deletes"),
            "n" => Some("ON DELETE SET NULL changes"),
            "d" => Some("ON DELETE SET DEFAULT changes"),
            _ => None,
        };
        let (referencing, constraint) = (&row.referencing, &row.constraint);
        match (
            carried,
            row.deferred,
            row.action.as_str(),
            row.own && matches!(mode, WriteMode::Replace),
        ) {
            (_, _, _, true) => {}
            (Some(effect), _, _, _) => problems.push(format!(
                "{referencing} references {schema}.{name} ({constraint}), and its {effect} its rows when {strategy} deletes the rows it replaces; SQLLoad refuses to change another table, so change the constraint's ON DELETE action, or load with merge"
            )),
            (None, true, "a", _) => {}
            (None, _, _, _) => warnings.push(format!(
                "{referencing} references {schema}.{name} ({constraint}), so {strategy} fails while any of its rows reference a row it deletes"
            )),
        }
    }
    (problems, warnings)
}

/// `sql` with each `$N` placeholder replaced by `values[N - 1]`, SQL already: for a session that runs SQL text alone.
pub(crate) fn bound(sql: &str, values: &[String]) -> Result<String, String> {
    let mut out = String::with_capacity(sql.len());
    let mut characters = sql.chars().peekable();
    while let Some(character) = characters.next() {
        if character != '$' || !characters.peek().is_some_and(char::is_ascii_digit) {
            out.push(character);
            continue;
        }
        let mut digits = String::new();
        while let Some(digit) = characters.next_if(char::is_ascii_digit) {
            digits.push(digit);
        }
        let value = digits
            .parse::<usize>()
            .ok()
            .and_then(|number| values.get(number.checked_sub(1)?))
            .ok_or_else(|| format!("${digits} has no value"))?;
        out.push_str(value);
    }
    Ok(out)
}

/// `sql`, which gives `columns` columns, with each value cast to text, so what a caller's driver gives back is Postgres's own text whatever the column's type.
pub(crate) fn as_text(sql: &str, columns: usize) -> String {
    let names: Vec<String> = (1..=columns).map(|index| format!("c{index}")).collect();
    let casts: Vec<String> = names.iter().map(|name| format!("{name}::text")).collect();
    format!(
        "SELECT {} FROM ({sql}) AS q({})",
        casts.join(", "),
        names.join(", ")
    )
}

/// The elements of a one-dimensional Postgres array in its text form (`{a,"b c"}`).
pub(crate) fn text_array(text: &str) -> Result<Vec<String>, String> {
    let inner = text
        .strip_prefix('{')
        .and_then(|rest| rest.strip_suffix('}'))
        .ok_or_else(|| format!("{text:?} is not an array"))?;
    let mut elements: Vec<String> = Vec::new();
    let (mut current, mut quoted, mut escaped) = (String::new(), false, false);
    for character in inner.chars() {
        match (escaped, quoted, character) {
            (true, _, _) => {
                current.push(character);
                escaped = false;
            }
            (false, true, '\\') => escaped = true,
            (false, _, '"') => quoted = !quoted,
            (false, false, ',') => elements.push(std::mem::take(&mut current)),
            _ => current.push(character),
        }
    }
    if !inner.is_empty() {
        elements.push(current);
    }
    Ok(elements)
}
