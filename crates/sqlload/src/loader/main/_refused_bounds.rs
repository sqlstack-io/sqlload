//! The check `plan` makes that an ODBC driver takes a table's cursor bounds.

use crate::adapters::contract::models::BoundParameter;
use crate::column_types::models::ColumnType;
use crate::loader::_helpers::table_plan::{cursor_binding, source_filter};
use crate::loader::constants::EXCLUSIVE_BOUND;
use crate::loader::models::{CursorBinding, TablePlan};

/// Why `table`'s cursor query fails on its ODBC source `source_name`, or `None` when it runs: `run` executes the table's read with a filter and its bound values, here bounds that select no rows (`cursor > v AND cursor < v AND 1 = 0`, `v` the planned lower bound, else the upper, else a value of the cursor's type; `1 = 0` lets the database skip the scan an unindexed cursor would take), first as the table writes them and, when that fails, the other way. When only the other way runs, and the table's own way fails again (so a passing failure gives no advice), the problem names the `cursor_bounds` that fixes it; when both fail, it shows both errors. Only a text, date or timestamp cursor whose bounds bind is checked, since a number, or a bound of a pinned type, is the same literal either way.
pub(in crate::loader) async fn refused_bounds(
    table: &TablePlan,
    source_name: &str,
    mut run: impl AsyncFnMut(String, Vec<BoundParameter>) -> Result<(), String>,
) -> Option<String> {
    let cursor = table.spec.cursor.as_ref()?;
    let column = table
        .read_columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(&cursor.column))?;
    let sentinel = match column.output {
        ColumnType::TimestampNtz => "2000-01-01 00:00:00",
        ColumnType::Date => "2000-01-01",
        ColumnType::Text | ColumnType::Varchar(_) => "",
        _ => return None,
    };
    let value = table
        .window
        .as_ref()
        .and_then(|window| window.from.as_deref().or(window.to.as_deref()))
        .unwrap_or(sentinel);
    let mut attempt = async |escapes: bool| {
        let mut parameters: Vec<BoundParameter> = Vec::new();
        let binding = cursor_binding(&table.spec, Some(escapes), &mut parameters);
        if matches!(binding, CursorBinding::Literals) {
            return None;
        }
        let filter = source_filter(
            &column.expression,
            (Some(&column.output), None),
            (Some((EXCLUSIVE_BOUND, value)), Some(value)),
            binding,
        )?;
        Some(run(format!("{filter} AND 1 = 0"), parameters).await)
    };
    let error = attempt(cursor.literals).await?.err()?;
    let (written, other) = match cursor.literals {
        true => ("ODBC escapes", "parameters"),
        false => ("parameters", "ODBC escapes"),
    };
    let other_way = match attempt(!cursor.literals).await {
        Some(Ok(())) => match attempt(cursor.literals).await? {
            Ok(()) => return None,
            Err(_) => Some(Ok(())),
        },
        other => other,
    };
    Some(match (cursor.literals, other_way) {
        (false, Some(Ok(()))) => format!(
            "the ODBC driver refuses the cursor {}'s bounds as {written} ({error}) but takes them as {other}; set cursor_bounds = \"literals\" on source {source_name} or its table entry",
            cursor.column
        ),
        (true, Some(Ok(()))) => format!(
            "the ODBC driver refuses the cursor {}'s bounds as {written} ({error}) but takes them as {other}; remove cursor_bounds = \"literals\" from source {source_name} and its table entry",
            cursor.column
        ),
        (_, Some(Err(other_error))) => format!(
            "the cursor {}'s query fails with its bounds as {written} ({error}) and as {other} ({other_error})",
            cursor.column
        ),
        (_, None) => format!("the cursor {}'s query fails: {error}", cursor.column),
    })
}
