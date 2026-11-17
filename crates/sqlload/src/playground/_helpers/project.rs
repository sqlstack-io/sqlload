//! Write a playground project, and add the bundled changes to its data.

use std::path::Path;

use crate::number_text::main::thousands::thousands;
use crate::playground::_helpers::rows::{change_lines, changed_rows, row_count};
use crate::playground::constants::{
    CHANGES, CUSTOMERS_CSV, DATABASE, NEXT_COMMANDS, ORDERS_CSV, PROJECT_FILES, PROJECT_HEADER,
    SCHEMA,
};

/// Write a playground into `dir` (new or empty, shown as `shown`); the lines saying what it holds and what to run.
pub(crate) fn create_playground(dir: &Path, shown: &str) -> Result<Vec<String>, String> {
    if dir.exists() {
        let empty = std::fs::read_dir(dir)
            .map(|mut entries| entries.next().is_none())
            .unwrap_or(false);
        if !empty {
            return Err(format!(
                "{shown} already exists and is not an empty folder; name a new folder, as in `sqlload playground my-shop`"
            ));
        }
    }
    for file in PROJECT_FILES {
        let path = dir.join(file.path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| format!("cannot create {}: {error}", parent.display()))?;
        }
        std::fs::write(&path, file.text)
            .map_err(|error| format!("cannot write {}: {error}", path.display()))?;
    }
    let mut lines = vec![
        "SQLLoad playground created".to_owned(),
        String::new(),
        format!("  Project  {shown}"),
        format!(
            "  Source   CSV files in {shown}/data/: {} customers, {} orders",
            thousands(row_count(CUSTOMERS_CSV)),
            thousands(row_count(ORDERS_CSV))
        ),
        format!("  Target   DuckDB {shown}/{DATABASE}, schema {SCHEMA}"),
        String::new(),
        "Try:".to_owned(),
        format!("  cd {shown}"),
    ];
    lines.extend(NEXT_COMMANDS.iter().map(|command| format!("  {command}")));
    Ok(lines)
}

/// Add the next bundled change to the playground whose sqlload.toml is `config`, and describe it.
pub(crate) fn apply_change(config: &Path) -> Result<Vec<String>, String> {
    let text = match std::fs::read_to_string(config) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(format!("cannot read {}: {error}", config.display())),
    };
    if !text.starts_with(PROJECT_HEADER) {
        return Err(format!(
            "{} is not a playground's sqlload.toml; run `sqlload playground --change` in a project `sqlload playground` created",
            config.display()
        ));
    }
    let dir = config.parent().unwrap_or(Path::new(""));
    let Some(index) = CHANGES
        .iter()
        .position(|change| !dir.join(change.path).exists())
    else {
        let last = CHANGES.last().map_or("", |change| change.path);
        return Ok(vec![
            format!("Nothing left to change: {last}, the last change, is already in the project."),
            format!("To start again, delete {DATABASE} and {last}."),
        ]);
    };
    let change = CHANGES[index];
    let path = dir.join(change.path);
    std::fs::write(&path, change.text)
        .map_err(|error| format!("cannot write {}: {error}", path.display()))?;
    let mut before: Vec<&str> = vec![ORDERS_CSV];
    before.extend(CHANGES[..index].iter().map(|earlier| earlier.text));
    Ok(change_lines(
        change.path,
        &changed_rows(&before, change.text),
    ))
}
