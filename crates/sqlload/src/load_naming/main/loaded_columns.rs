//! The name each column of a table loads under.

use crate::load_naming::_helpers::numbering::numbered_columns;
use crate::load_naming::_helpers::renames::{named_columns, rename_clashes};
use crate::load_naming::models::{ColumnName, LoadedNames, NameRules};
use crate::load_naming::types::RenameFunction;

/// `columns`, in their order, through the name pipeline: `naming`, then `rename` (`rules.renamed`, keyed by source or named name, then the positions `rename` returns), then `on_duplicate_names` for the names that still meet (ignoring case, as warehouses fold it). Numbering keeps the names `rules.kept` gave source names before; on a new clash the first column keeps the name and the others get `_1`, `_2`… past every name in the table, in source-name order, repeats of one header in their order. The problems: a rename that is empty or makes a clash, a clash under `on_duplicate_names = "fail"`, a clash on a column the table has that no earlier load recorded, and a number given before to a source name the table no longer has that another would take.
pub fn loaded_columns(
    columns: &[ColumnName],
    rules: &NameRules,
    rename: RenameFunction<'_>,
) -> Result<LoadedNames, Vec<String>> {
    let (loaded, mut problems) = named_columns(columns, rules, rename);
    problems.extend(rename_clashes(&loaded, &rules.kept));
    if !problems.is_empty() {
        return Err(problems);
    }
    let (columns, numbered) = numbered_columns(loaded, rules)?;
    Ok(LoadedNames {
        columns,
        numbered,
        kept: rules.kept.clone(),
    })
}
