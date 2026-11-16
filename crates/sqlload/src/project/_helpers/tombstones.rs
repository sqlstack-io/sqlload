use crate::project::models::{
    ProjectConfig, RenameSetting, SourceConfig, TableOverride, TableSettings, WriteStrategy,
};

/// Why source `source`'s `deletes_from` tables (at `place`) cannot load: each table needs `deletes` to say what a tombstone does, and a tombstone table loads only as one table's tombstones, so it is neither listed under `tables` itself nor named by two tables.
pub(crate) fn tombstone_problems(place: &str, source: &SourceConfig) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    let mut named: Vec<(&str, &str)> = Vec::new();
    for (key, table) in &source.tables {
        let Some(from) = &table.deletes_from else {
            continue;
        };
        let at = format!("{place} table {key:?}");
        if table.settings.deletes.or(source.defaults.deletes).is_none() {
            problems.push(format!(
                "{at}: deletes_from marks the rows its tombstones name, which needs deletes = \"soft\" or \"hard\" to say how"
            ));
        }
        if table.settings.cursor_column.is_none() && source.defaults.cursor_column.is_none() {
            problems.push(format!(
                "{at}: deletes_from marks a row only when its tombstone is not older than the row's cursor_column, so a key deleted and then created again stays; the table needs a cursor_column"
            ));
        }
        if from.table == *key || source.tables.contains_key(&from.table) {
            problems.push(format!(
                "{at}: deletes_from table {:?} is also listed under tables; it loads only as {key:?}'s tombstones. Fix: remove its own entry",
                from.table
            ));
        }
        if let Some((other, _)) = named.iter().find(|(_, each)| *each == from.table) {
            problems.push(format!(
                "{at}: deletes_from table {:?} is also {other:?}'s; each table needs its own tombstone table",
                from.table
            ));
        }
        named.push((key, &from.table));
    }
    problems
}

/// `project` with a table added for each `deletes_from`: its tombstone table, described, loaded by `delete_insert` on its `cursor_column` with its `lookback` (the range read again replaces its rows, so a tombstone that becomes visible late is read and none repeats), named and sent as the table it marks (its `rename` function, not a map naming its own columns), and matched by the source's `include` (so a database source finds it).
pub(crate) fn with_tombstone_tables(mut project: ProjectConfig) -> ProjectConfig {
    for source in project.sources.values_mut() {
        let added: Vec<(String, TableOverride)> = source
            .tables
            .iter()
            .filter_map(|(key, table)| {
                let from = table.deletes_from.as_ref()?;
                let (own, defaults) = (&table.settings, &source.defaults);
                let settings = TableSettings {
                    write_strategy: Some(WriteStrategy::DeleteInsert),
                    cursor_column: Some(vec![from.cursor_column.clone()]),
                    lookback: from.lookback,
                    naming: own.naming.or(defaults.naming),
                    compat: own.compat.or(defaults.compat),
                    send: own.send.or(defaults.send),
                    rename: match &own.rename {
                        Some(RenameSetting::Function(function)) => {
                            Some(RenameSetting::Function(function.clone()))
                        }
                        _ => None,
                    },
                    on_duplicate_names: own.on_duplicate_names.or(defaults.on_duplicate_names),
                    timezone: own.timezone.clone().or(defaults.timezone.clone()),
                    ..TableSettings::default()
                };
                let added = TableOverride {
                    settings,
                    description: Some(format!(
                        "The keys deleted from {key}, as {} records them",
                        from.table
                    )),
                    tombstones_of: Some(key.clone()),
                    ..TableOverride::default()
                };
                Some((from.table.clone(), added))
            })
            .collect();
        for (name, table) in added {
            if source.discovers_tables() && !source.include.contains(&name) {
                source.include.push(name.clone());
            }
            source.tables.insert(name, table);
        }
    }
    project
}
