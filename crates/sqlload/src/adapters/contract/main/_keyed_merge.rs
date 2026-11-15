//! A `delete_insert` by `replace_key` in one `MERGE`.

use crate::adapters::contract::_helpers::write::replaced_keys;
use crate::adapters::contract::models::WriteStatementsRequest;
use crate::adapters::contract::types::SqlAdapter;

/// A `delete_insert` by `replace_key` as one `MERGE` from `source` (`values` its columns' expressions, in `request.columns`' order), for a warehouse that commits each statement on its own: every staged row is inserted, the table's rows holding a key value a staged row holds are deleted (matched on each distinct staged key, so a row matches at most one), and so are those in the cursor range `range` covers (`Some(None)` for every row; `None` without a cursor).
pub(in crate::adapters) fn keyed_merge<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
    (source, values): (&str, &[String]),
    (replace_key, range): (&[String], Option<Option<String>>),
) -> String {
    let target = adapter.render_qualified_name(request.table);
    let names: Vec<String> = request
        .columns
        .iter()
        .map(|column| adapter.render_identifier(&column.name))
        .collect();
    let (keys, join) = replaced_keys(adapter, request, values, replace_key);
    let key_names: Vec<String> = replace_key
        .iter()
        .map(|key| adapter.render_identifier(key))
        .collect();
    let selected: Vec<String> = values
        .iter()
        .zip(&names)
        .map(|(value, name)| format!("{value} AS {name}"))
        .collect();
    let keyed: Vec<&str> = names
        .iter()
        .map(|name| match key_names.contains(name) {
            true => name.as_str(),
            false => "NULL",
        })
        .collect();
    let inserted: Vec<String> = names
        .iter()
        .map(|name| format!("__source.{name}"))
        .collect();
    let deleted = match range {
        None => String::new(),
        Some(Some(condition)) => format!(" WHEN NOT MATCHED BY SOURCE AND {condition} THEN DELETE"),
        Some(None) => " WHEN NOT MATCHED BY SOURCE THEN DELETE".to_owned(),
    };
    format!(
        "MERGE INTO {target} AS __target USING (SELECT {}, FALSE AS __sqlload_replaces FROM {source} UNION ALL SELECT {}, TRUE FROM (SELECT DISTINCT {keys} FROM {source}) AS __sqlload_keys) AS __source ON __source.__sqlload_replaces AND {join} WHEN MATCHED THEN DELETE WHEN NOT MATCHED AND NOT __source.__sqlload_replaces THEN INSERT ({}) VALUES ({}){deleted}",
        selected.join(", "),
        keyed.join(", "),
        names.join(", "),
        inserted.join(", ")
    )
}
