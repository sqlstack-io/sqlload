use toml::Value;

use crate::project::constants::{
    PYTHON_SOURCE_KEYS, SOURCE_ONLY_KEYS, TABLE_ONLY_KEYS, TABLE_SETTING_KEYS,
};
use crate::similar_names::main::nearest_name::nearest_name;

/// Problems for every key under `[sources.*]` and `[sources.*.tables.*]` SQLLoad does not know.
pub(crate) fn unknown_keys(value: &Value) -> Vec<String> {
    let source_keys: Vec<&str> = SOURCE_ONLY_KEYS
        .iter()
        .chain(TABLE_SETTING_KEYS)
        .copied()
        .collect();
    let place = |source: &str, table: Option<&str>| match table {
        Some(table) => format!("[sources.{source}.tables.\"{table}\"]"),
        None => format!("[sources.{source}]"),
    };
    keys_problems(value, &source_keys, &place)
}

/// Problems for every setting of Python sources and tables (`value` holds them as `sources`) SQLLoad does not know; `module` names each source's file.
pub(crate) fn unknown_python_keys(value: &Value, module: &dyn Fn(&str) -> String) -> Vec<String> {
    let source_keys: Vec<&str> = PYTHON_SOURCE_KEYS
        .iter()
        .chain(TABLE_SETTING_KEYS)
        .copied()
        .collect();
    let place = |source: &str, table: Option<&str>| match table {
        Some(table) => format!("Python table {table} ({})", module(source)),
        None => format!("Python source {source} ({})", module(source)),
    };
    keys_problems(value, &source_keys, &place)
}

fn keys_problems(
    value: &Value,
    source_keys: &[&str],
    place: &dyn Fn(&str, Option<&str>) -> String,
) -> Vec<String> {
    let Some(sources) = value.get("sources").and_then(Value::as_table) else {
        return Vec::new();
    };
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
        problems.extend(unknown_in(
            source.keys(),
            source_keys,
            &place(source_name, None),
        ));
        let tables = source.get("tables").and_then(Value::as_table);
        for (table_name, table) in tables.into_iter().flatten() {
            let keys = table.as_table().map(|table| table.keys());
            let table_place = place(source_name, Some(table_name));
            problems.extend(
                keys.map(|keys| unknown_in(keys, &table_keys, &table_place))
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
