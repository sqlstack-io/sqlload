//! Every table's options checked together at parse, before anything connects.

use crate::project::models::{ProjectConfig, TableOverride};
use crate::table_options::_helpers::option_target::option_target;
use crate::table_options::main::layered::layered;
use crate::table_options::main::placeholder_problem::placeholder_problem;
use crate::table_options::main::table_options::table_options;
use crate::table_options::main::table_strategy::table_strategy;
use crate::table_options::models::OptionContext;

/// Every problem with how a table's options combine, each led by where the table is set (its `[sources.*.tables.*]` entry, or its Python table); with the wording `plan` gives them. A source listing no tables is checked with its own settings, which every table it finds takes; one listing some is checked table by table, as a table it finds besides them might set nothing.
pub fn option_problems(project: &ProjectConfig) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    for (name, source) in &project.sources {
        let Some(target) = option_target(project, source) else {
            continue;
        };
        let context = OptionContext {
            name,
            source,
            target: &target,
        };
        let entries: Vec<(Option<&String>, TableOverride)> = match source.tables.is_empty() {
            true => vec![(None, TableOverride::default())],
            false => source
                .tables
                .iter()
                .map(|(key, table)| (Some(key), table.clone()))
                .collect(),
        };
        for (key, overrides) in entries {
            let place = place(project, name, key.map(String::as_str));
            let found = table_problems(
                &context,
                key.map_or(name.as_str(), String::as_str),
                &overrides,
            );
            problems.extend(
                found
                    .into_iter()
                    .map(|problem| format!("{place}: {problem}")),
            );
        }
    }
    problems
}

/// The problems of table `qualified`'s options, as the loader finds them: a `unique_key` its settings refuse first, alone, and none when no write strategy is set, which `plan` reports with the source table's key.
fn table_problems(
    context: &OptionContext<'_>,
    qualified: &str,
    overrides: &TableOverride,
) -> Vec<String> {
    let Ok(chosen) = table_strategy(context, overrides) else {
        return Vec::new();
    };
    let set = layered(
        &overrides.settings.unique_key,
        &context.source.defaults.unique_key,
    )
    .map(|(key, _)| key);
    if let Some(problem) = placeholder_problem(
        context.source.kind,
        qualified,
        set.as_deref(),
        chosen.merges,
    ) {
        return vec![problem];
    }
    table_options(context, overrides, &chosen, &set.unwrap_or_default()).1
}

/// Where problems name a table: its TOML entry or Python table, or its source when it lists none.
fn place(project: &ProjectConfig, name: &str, key: Option<&str>) -> String {
    let module = project.python_modules.get(name);
    match (key, module) {
        (Some(key), Some(module)) => format!("Python table {key} ({module})"),
        (Some(key), None) => format!("[sources.{name}.tables.\"{key}\"]"),
        (None, Some(module)) => format!("Python source {name} ({module})"),
        (None, None) => format!("[sources.{name}]"),
    }
}
