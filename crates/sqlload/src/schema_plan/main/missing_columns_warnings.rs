//! What `on_schema_change` makes of declared columns the data lacks: each loads as NULL at its declared type with a warning, unless `"fail"` refuses the table; a declared column is never dropped.

use crate::project::models::OnSchemaChange;

/// What `on_schema_change` makes of the declared columns `missing` that the data lacks (`place`, as `the sampled files`, saying where they were looked for, and `loads` what happens to them): a warning for each, as the column loads as NULL at its declared type; or, under `"fail"`, why the table cannot load. Nothing is missing, nothing is said.
pub fn missing_columns_warnings(
    missing: &[String],
    (place, loads): (&str, &str),
    on_schema_change: OnSchemaChange,
) -> Result<Vec<String>, String> {
    let lines = missing
        .iter()
        .map(|name| format!("declared column {name} is in none of {place}"));
    match on_schema_change {
        _ if missing.is_empty() => Ok(Vec::new()),
        OnSchemaChange::Fail => Err(lines
            .map(|line| {
                format!(
                    "{line} and on_schema_change = \"fail\"; add it to the data, or remove it from columns"
                )
            })
            .collect::<Vec<String>>()
            .join("; ")),
        OnSchemaChange::AppendNewColumns | OnSchemaChange::Ignore => {
            Ok(lines.map(|line| format!("{line}, so {loads}")).collect())
        }
    }
}
