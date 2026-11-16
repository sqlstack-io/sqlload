//! The `Loads` section: each source with its tables, one line each.

use std::collections::BTreeMap;

use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::plan_text::_helpers::rows::{
    action, children, files_read, reads, source_line, table_names,
};
use crate::plan_text::_helpers::tree::{branch, hinted, marks};
use crate::plan_text::models::LoadedTable;
use crate::plan_text::models::{SourceGroup, TableRow};
use crate::project::models::SendMode;

/// The `Loads` section for the tables `ready` to load: its heading, then each source with its tables, then a blank line; nothing without tables.
pub(crate) fn loads_section(ready: &[LoadedTable<'_>]) -> Vec<String> {
    if ready.is_empty() {
        return Vec::new();
    }
    let mut lines: Vec<String> = vec![hinted(&heading(ready), "sqlload load")];
    lines.extend(group_lines(&source_groups(ready)));
    lines.push(String::new());
    lines
}

/// `Loads (15)  into DuckDB warehouse`, every target the tables load into, with ` \u{b7} rows sent at commit` when every table's rows are.
fn heading(ready: &[LoadedTable<'_>]) -> String {
    let mut targets: Vec<String> = Vec::new();
    for loaded in ready {
        let spec = &loaded.table.spec;
        let target = format!("{} {}", spec.warehouse, spec.target);
        if !targets.contains(&target) {
            targets.push(target);
        }
    }
    let at_commit = ready.iter().all(|loaded| {
        let spec = &loaded.table.spec;
        spec.send == SendMode::AtCommit && sql_adapter(spec.warehouse).runs_sql()
    });
    let sent = match at_commit {
        true => " \u{b7} rows sent at commit",
        false => "",
    };
    format!("Loads ({})  into {}{sent}", ready.len(), targets.join(", "))
}

/// The tables `ready` to load by source, in order of the sources' names, each table's row sorted by name.
fn source_groups(ready: &[LoadedTable<'_>]) -> Vec<SourceGroup> {
    let mut by_source: BTreeMap<&str, Vec<&LoadedTable<'_>>> = BTreeMap::new();
    for loaded in ready {
        by_source
            .entry(loaded.table.spec.source_name.as_str())
            .or_default()
            .push(loaded);
    }
    let mut groups: Vec<SourceGroup> = Vec::new();
    for (source, tables) in by_source {
        let (schema, names) = table_names(source, &tables);
        let line = source_line(source, tables[0].table);
        let line = match schema {
            Some(schema) => format!("{line}  into {schema}"),
            None => line,
        };
        let mut rows: Vec<TableRow> = Vec::new();
        for (loaded, name) in tables.iter().zip(names) {
            let (read, details) = files_read(&loaded.item);
            rows.push(TableRow {
                name,
                action: action(loaded),
                reads: read.unwrap_or_else(|| reads(loaded)),
                details: children(loaded.table, &details),
            });
        }
        rows.sort_by(|left, right| left.name.cmp(&right.name));
        groups.push(SourceGroup { line, rows });
    }
    groups
}

/// Each source's line and its tables' lines, names aligned across the section and, where a read has a start, the starts aligned after the actions; a logged state follows its action.
fn group_lines(groups: &[SourceGroup]) -> Vec<String> {
    let rows: Vec<&TableRow> = groups.iter().flat_map(|group| &group.rows).collect();
    let name_width = rows
        .iter()
        .map(|row| row.name.chars().count())
        .max()
        .unwrap_or(0)
        + 3;
    let action_width = rows
        .iter()
        .filter(|row| !row.reads.is_empty() && !row.reads.starts_with("from "))
        .map(|row| row.action.chars().count())
        .max()
        .unwrap_or(0)
        + 3;
    let mut lines: Vec<String> = Vec::new();
    for (group_index, group) in groups.iter().enumerate() {
        let (mark, indent) = marks(group_index + 1 == groups.len());
        lines.push(format!("{mark}{}", group.line));
        for (index, row) in group.rows.iter().enumerate() {
            let TableRow {
                name,
                action,
                reads,
                details,
            } = row;
            let line = match (reads.is_empty(), reads.starts_with("from ")) {
                (true, _) => format!("{name:<name_width$}{action}"),
                (false, true) => format!("{name:<name_width$}{action} {reads}"),
                (false, false) => format!("{name:<name_width$}{action:<action_width$}{reads}"),
            };
            lines.extend(branch(
                line.trim_end(),
                details,
                index + 1 == group.rows.len(),
                indent,
            ));
        }
    }
    lines
}
