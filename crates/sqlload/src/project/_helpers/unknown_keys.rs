use toml::Value;

use crate::project::constants::{SOURCE_ONLY_KEYS, TABLE_ONLY_KEYS, TABLE_SETTING_KEYS};
use crate::similar_names::main::nearest_name::nearest_name;

/// Problems for every key under `[sources.*]` and `[sources.*.tables.*]` SQLLoad does not know.
pub(crate) fn unknown_keys(value: &Value) -> Vec<String> {
    let Some(sources) = value.get("sources").and_then(Value::as_table) else {
        return Vec::new();
    };
    let source_keys: Vec<&str> = SOURCE_ONLY_KEYS
        .iter()
        .chain(TABLE_SETTING_KEYS)
        .copied()
        .collect();
    let table_keys: Vec<&str> = TABLE_ONLY_KEYS
        .iter()
        .chain(TABLE_SETTING_KEYS)
        .copied()
        .collect();
    let mut problems: Vec<String> = Vec::new();
    for (source_name, source) in sources {
        let Some(source) = source.as_table() else {
            continue;
        };
        let place = format!("[sources.{source_name}]");
        problems.extend(unknown_in(source.keys(), &source_keys, &place));
        let tables = source.get("tables").and_then(Value::as_table);
        for (table_name, table) in tables.into_iter().flatten() {
            let place = format!("[sources.{source_name}.tables.\"{table_name}\"]");
            let keys = table.as_table().map(|table| table.keys());
            problems.extend(
                keys.map(|keys| unknown_in(keys, &table_keys, &place))
                    .into_iter()
                    .flatten(),
            );
        }
    }
    problems
}

fn unknown_in<'a>(
    keys: impl Iterator<Item = &'a String>,
    known: &[&str],
    place: &str,
) -> Vec<String> {
    keys.filter(|key| !known.contains(&key.as_str()))
        .map(|key| unknown_key_problem(key, known, place))
        .collect()
}

fn unknown_key_problem(key: &str, known: &[&str], place: &str) -> String {
    let hint = nearest_name(key, known.iter().copied())
        .map(|nearest| format!("; did you mean {nearest:?}?"))
        .unwrap_or_default();
    format!("unknown key {key:?} in {place}{hint}")
}
