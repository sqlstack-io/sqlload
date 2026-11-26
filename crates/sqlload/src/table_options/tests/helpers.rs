use std::collections::BTreeMap;

use crate::project::main::parse_project::parse_project;
use crate::project::main::parse_python_project::parse_python_project;
use crate::python_sources::models::PythonSource;

/// The connections every test project reads and writes through.
const CONNECTIONS: &str = "[connections.wh]\nkind = \"duckdb\"\ndatabase = \":memory:\"\n[connections.pg]\nkind = \"postgres\"\nhost = \"localhost\"\nuser = \"reader\"\ndatabase = \"app\"\n[connections.files]\nkind = \"filesystem\"\nroot = \"in\"\n[connections.lake]\nkind = \"filesystem\"\nroot = \"lake\"\n";

/// A DuckDB target.
pub(crate) const DUCKDB: &str = "[targets.dev]\nconnection = \"wh\"\nloader_schema = \"raw\"\n";

/// A files target.
pub(crate) const FILES_TARGET: &str = "[targets.dev]\nconnection = \"lake\"\n";

/// A Postgres source's table `public.t`, whose settings follow.
pub(crate) const POSTGRES_TABLE: &str = "kind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"public.t\"]\n[sources.s.tables.\"public.t\"]\n";

/// A Postgres source with its own settings, which follow, and no table entries.
pub(crate) const POSTGRES_SOURCE: &str =
    "kind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"public.*\"]\n";

/// A files source reading CSV files, whose settings follow.
pub(crate) const FILES_SOURCE: &str = "kind = \"files\"\nconnection = \"files\"\npath = \"orders/\"\nformat = \"csv\"\ndescription = \"Orders.\"\n";

/// Parse's error for a project of `target` and source `s` (`source` then `settings`), or nothing when it parses.
pub(crate) fn toml_problems(target: &str, source: &str, settings: &str) -> String {
    let text = format!("{CONNECTIONS}{target}[sources.s]\n{source}{settings}");
    parse_project(&text, &|_| None)
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default()
}

/// Parse's error for a project whose Python source `s` (in `s.py`) declares table `t` with `settings` (TOML), or nothing when it parses.
pub(crate) fn python_problems(settings: &str) -> Result<String, String> {
    let mut table: toml::Table = toml::from_str(settings).map_err(|error| error.to_string())?;
    table.insert("description".to_owned(), toml::Value::from("Table t."));
    let settings = serde_json::json!({"tables": {"t": table}});
    let source = serde_json::json!({"name": "s", "module": "s.py", "settings": settings});
    let python: Vec<PythonSource> =
        serde_json::from_value(serde_json::json!([source])).map_err(|error| error.to_string())?;
    let text = format!("{CONNECTIONS}{DUCKDB}");
    Ok(
        parse_python_project(&text, &|_| None, (&python, BTreeMap::new()))
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default(),
    )
}
