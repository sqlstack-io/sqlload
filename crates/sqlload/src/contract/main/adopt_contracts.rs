//! `contract generate --write`: adoptions written into sqlload.toml (`sqb contract generate --write`).

use toml_edit::{DocumentMut, InlineTable, Item, Table, Value};

use crate::contract::constants::{MISSING_DECLARATION, MISSING_PHYSICAL_COLUMN, TYPE_MISMATCH};
use crate::contract::models::{Adoption, ContractFinding, ContractReport};

/// `text` (sqlload.toml) with each adoption of a table declared there written into its `[sources.<source>.tables.<table>]` `columns`, which are created when missing, keeping the rest of the file as it was; the report then lists only the findings left. A Python table's adoption is left to paste. `None` when nothing changes.
pub fn adopt_contracts(text: &str, report: &mut ContractReport) -> Result<Option<String>, String> {
    let mut document: DocumentMut = text
        .parse()
        .map_err(|error| format!("cannot edit sqlload.toml: {error}"))?;
    let mut changed = false;
    for resource in &mut report.resources {
        let Some(adoption) = resource
            .adoption
            .as_ref()
            .filter(|adoption| adoption.paste.is_none())
        else {
            continue;
        };
        let Some(columns) = columns_of(document.as_item_mut(), adoption)? else {
            report.problems.push(format!(
                "{}: columns not written: a table entry needs a description and this table has none; add description = \"...\" there, then rerun contract generate --write",
                adoption.location
            ));
            continue;
        };
        let columns = columns.as_table_like_mut().ok_or_else(|| {
            format!(
                "{} has a columns setting that is not a table",
                adoption.location
            )
        })?;
        for column in &adoption.columns {
            columns.insert(
                &column.column,
                Item::Value(Value::from(column.physical_type.as_str())),
            );
        }
        for name in &adoption.removed {
            columns.remove(name);
        }
        resource
            .findings
            .retain(|finding| !resolved(finding, adoption));
        changed = true;
    }
    report.differences = report
        .resources
        .iter()
        .map(|resource| resource.findings.len())
        .sum();
    Ok(changed.then(|| document.to_string()))
}

/// The `columns` item of `[sources.<source>.tables.<table>]`, adding the tables on the way when missing, with the database table's description in a new entry; `None` when the entry is missing and the table has no description to give it. The source itself must be in the file.
fn columns_of<'d>(root: &'d mut Item, adoption: &Adoption) -> Result<Option<&'d mut Item>, String> {
    let (source, table) = &adoption.toml_key;
    let source = root
        .get_mut("sources")
        .and_then(|sources| sources.get_mut(source.as_str()))
        .filter(|item| item.is_table_like())
        .ok_or_else(|| format!("[sources.{source}] is not a table in sqlload.toml"))?;
    let tables = child(source, "tables", true)?;
    let created = tables.get(table.as_str()).is_none();
    let description = adoption.description.as_deref().filter(|_| created);
    if description.is_some_and(|text| text.trim().is_empty()) {
        return Ok(None);
    }
    let entry = child(tables, table, true)?;
    if let Some((text, entry)) = description.zip(entry.as_table_like_mut()) {
        entry.insert("description", Item::Value(Value::from(text)));
    }
    child(entry, "columns", false).map(Some)
}

/// `parent`'s table `key`, added when missing: inline inside an inline table, otherwise a `[...]` table, implicit (no header of its own) when `implicit`.
fn child<'i>(parent: &'i mut Item, key: &str, implicit: bool) -> Result<&'i mut Item, String> {
    let inline = parent.is_inline_table();
    let table = parent
        .as_table_like_mut()
        .ok_or_else(|| format!("{key}: its parent in sqlload.toml is not a table"))?;
    let item = table.entry(key).or_insert_with(|| match inline {
        true => Item::Value(Value::InlineTable(InlineTable::new())),
        false => {
            let mut created = Table::new();
            created.set_implicit(implicit);
            Item::Table(created)
        }
    });
    match item.is_table_like() {
        true => Ok(item),
        false => Err(format!("{key} in sqlload.toml is not a table")),
    }
}

/// Whether writing `adoption` settles `finding`.
fn resolved(finding: &ContractFinding, adoption: &Adoption) -> bool {
    let Some(column) = finding.column.as_deref() else {
        return false;
    };
    let declared = adoption
        .columns
        .iter()
        .any(|adopted| adopted.column.eq_ignore_ascii_case(column));
    let removed = adoption
        .removed
        .iter()
        .any(|name| name.eq_ignore_ascii_case(column));
    match finding.kind {
        MISSING_DECLARATION | TYPE_MISMATCH => declared,
        MISSING_PHYSICAL_COLUMN => removed,
        _ => false,
    }
}
