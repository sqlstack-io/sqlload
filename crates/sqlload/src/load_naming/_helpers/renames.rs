//! `naming` and `rename` over a table's columns, and the clashes a rename makes.

use std::collections::BTreeMap;

use crate::load_naming::_helpers::numbering::{repeat_of, shown};
use crate::load_naming::main::loaded_name::loaded_name;
use crate::load_naming::models::{
    ColumnName, LoadedColumn, NameChange, NameRules, NumberedName, RenameColumn,
};
use crate::load_naming::types::RenameFunction;

/// `columns` under `naming`, then `rename`, each with what named it; and the problems the rename has: a position no column has, an empty name, or the function's own failure.
pub(crate) fn named_columns(
    columns: &[ColumnName],
    rules: &NameRules,
    rename: RenameFunction<'_>,
) -> (Vec<LoadedColumn>, Vec<String>) {
    let given: Vec<RenameColumn> = columns
        .iter()
        .map(|column| RenameColumn {
            position: column.position,
            source: column.source.clone(),
            default: loaded_name(rules.naming, &column.source, rules.max_length),
        })
        .collect();
    let (renamed, problems) = renamed_positions(&given, rules, rename);
    let loaded: Vec<LoadedColumn> = given
        .iter()
        .zip(columns)
        .map(|(column, read)| loaded_column(column, read, renamed.get(&column.position)))
        .collect();
    (loaded, problems)
}

/// The names `rules.renamed` (keyed by source or default name) and then the rename function give `given`, by position, and the problems they have.
fn renamed_positions(
    given: &[RenameColumn],
    rules: &NameRules,
    rename: RenameFunction<'_>,
) -> (BTreeMap<usize, String>, Vec<String>) {
    let mut renamed: BTreeMap<usize, String> = given
        .iter()
        .filter_map(|column| Some((column.position, mapped_name(column, rules)?)))
        .collect();
    let mut problems: Vec<String> = Vec::new();
    match rename(given) {
        Ok(returned) => {
            for (position, name) in returned {
                match given.iter().any(|column| column.position == position) {
                    true => {
                        renamed.insert(position, name);
                    }
                    false => problems.push(format!(
                        "rename returned position {position}, which is not one of the table's columns"
                    )),
                }
            }
        }
        Err(problem) => problems.push(problem),
    }
    problems.extend(
        given
            .iter()
            .filter(|column| renamed_empty(&renamed, column.position))
            .map(|column| format!("rename gives column {:?} an empty name", column.source)),
    );
    (renamed, problems)
}

/// Whether `renamed` gives the column at `position` an empty name.
fn renamed_empty(renamed: &BTreeMap<usize, String>, position: usize) -> bool {
    renamed
        .get(&position)
        .is_some_and(|name| name.trim().is_empty())
}

/// The name a `rename` map gives `column`, by its source name or else its default one.
fn mapped_name(column: &RenameColumn, rules: &NameRules) -> Option<String> {
    rules
        .renamed
        .get(&column.source)
        .or_else(|| rules.renamed.get(&column.default))
        .cloned()
}

/// `column` under the name `renamed` gives it, else its default name.
fn loaded_column(
    column: &RenameColumn,
    read: &ColumnName,
    renamed: Option<&String>,
) -> LoadedColumn {
    let (loaded, change) = match renamed {
        Some(name) => (name.clone(), Some(NameChange::Renamed)),
        None => (
            column.default.clone(),
            (column.default != column.source).then_some(NameChange::Named),
        ),
    };
    LoadedColumn {
        position: column.position,
        source: column.source.clone(),
        read_as: read.read_as.clone(),
        loaded,
        change,
    }
}

/// A problem for each name a rename gives that another column loads as too: by `naming`, by another rename, or by the number an earlier load gave it (`kept`).
pub(crate) fn rename_clashes(loaded: &[LoadedColumn], kept: &[NumberedName]) -> Vec<String> {
    loaded
        .iter()
        .filter(|column| column.change == Some(NameChange::Renamed))
        .filter_map(|column| rename_clash(column, loaded, kept))
        .collect()
}

/// The problem when another column of `loaded` loads under the name a rename gives `column` (another renamed one only once, from the earlier of the two).
fn rename_clash(
    column: &LoadedColumn,
    loaded: &[LoadedColumn],
    kept: &[NumberedName],
) -> Option<String> {
    let other = loaded.iter().find(|other| {
        other.position != column.position
            && kept_or_loaded(other, loaded, kept).eq_ignore_ascii_case(&column.loaded)
            && (other.position > column.position || other.change != Some(NameChange::Renamed))
    })?;
    Some(format!(
        "rename gives column {} the name {}, which column {} loads as too; give each a name of its own",
        shown(column, loaded),
        column.loaded,
        shown(other, loaded)
    ))
}

/// The name `column` loads under once the numbers earlier loads gave stay: the one `kept` holds for its source name, unless a rename named it.
fn kept_or_loaded(column: &LoadedColumn, loaded: &[LoadedColumn], kept: &[NumberedName]) -> String {
    let repeat = repeat_of(column, loaded);
    kept.iter()
        .find(|kept| kept.source == column.source && kept.repeat == repeat)
        .filter(|_| column.change != Some(NameChange::Renamed))
        .map_or_else(|| column.loaded.clone(), |kept| kept.column.clone())
}
