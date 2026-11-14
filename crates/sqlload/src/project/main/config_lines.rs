//! Where a project's settings are written in its file, for hints such as `sqlload.toml:46`.

use std::collections::BTreeMap;

use toml_edit::{Document, Item, Table};

use crate::project::models::ConfigLines;

/// The lines in the project file `text`, named `file` in hints, of each source's section and `unique_key`, and of each `[sources.*.tables]` entry and its `unique_key`; none for text that does not parse.
pub fn config_lines(file: &str, text: &str) -> ConfigLines {
    let mut lines: BTreeMap<Vec<String>, usize> = BTreeMap::new();
    let Ok(document) = Document::parse(text) else {
        return ConfigLines::default();
    };
    let line = |start: usize| text[..start].matches('\n').count() + 1;
    let place = |item: &Item, key: &toml_edit::Key| {
        item.as_table()
            .and_then(Table::span)
            .or_else(|| key.span())
            .map(|span| line(span.start))
    };
    let Some(sources) = document.get("sources").and_then(Item::as_table_like) else {
        return ConfigLines::default();
    };
    for (name, source) in sources.iter() {
        let Some((key, _)) = sources.get_key_value(name) else {
            continue;
        };
        let path = vec!["sources".to_owned(), name.to_owned()];
        lines.extend(place(source, key).map(|at| (path.clone(), at)));
        let Some(settings) = source.as_table_like() else {
            continue;
        };
        if let Some((key, item)) = settings.get_key_value("unique_key") {
            let path = [path.clone(), vec!["unique_key".to_owned()]].concat();
            lines.extend(place(item, key).map(|at| (path, at)));
        }
        let Some(tables) = settings.get("tables").and_then(Item::as_table_like) else {
            continue;
        };
        for (table, entry) in tables.iter() {
            let Some((key, _)) = tables.get_key_value(table) else {
                continue;
            };
            let path = [path.clone(), vec!["tables".to_owned(), table.to_owned()]].concat();
            lines.extend(place(entry, key).map(|at| (path.clone(), at)));
            if let Some((key, item)) = entry
                .as_table_like()
                .and_then(|entry| entry.get_key_value("unique_key"))
            {
                let path = [path, vec!["unique_key".to_owned()]].concat();
                lines.extend(place(item, key).map(|at| (path, at)));
            }
        }
    }
    ConfigLines {
        file: file.to_owned(),
        lines,
    }
}
