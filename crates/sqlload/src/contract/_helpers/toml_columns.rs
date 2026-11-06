//! Finding and creating a table's `columns` in sqlload.toml, and what writing an adoption settles.

use toml_edit::{DocumentMut, InlineTable, Item, Table, Value};

use crate::contract::constants::{MISSING_DECLARATION, MISSING_PHYSICAL_COLUMN, TYPE_MISMATCH};
use crate::contract::models::{Adoption, ContractFinding};

/// Whether writing `adoption` would create a `[sources.*.tables.*]` entry for a database table with no description to give it, which would then block the table.
pub(crate) fn lacks_description(root: &Item, adoption: &Adoption) -> bool {
    let (source, table) = &adoption.toml_key;
    let exists = root
        .get("sources")
        .and_then(|sources| sources.get(source.as_str()))
        .and_then(|source| source.get("tables"))
        .and_then(|tables| tables.get(table.as_str()))
        .is_some();
    !exists
        && adoption
            .description
            .as_deref()
            .is_some_and(|text| text.trim().is_empty())
}

/// `document` with `adoption` written into the `columns` of `[sources.<source>.tables.<table>]`, adding the tables on the way when missing, and a new database table entry's description; the source must be in the file.
pub(crate) fn write_adoption(
    mut document: DocumentMut,
    adoption: &Adoption,
) -> Result<DocumentMut, String> {
    let (source, table) = &adoption.toml_key;
    let source = document
        .as_item_mut()
        .get_mut("sources")
        .and_then(|sources| sources.get_mut(source.as_str()))
        .filter(|item| item.is_table_like())
        .ok_or_else(|| format!("[sources.{source}] is not a table in sqlload.toml"))?;
    let tables = child(source, "tables", true)?;
    let description = adoption
        .description
        .as_deref()
        .filter(|_| tables.get(table.as_str()).is_none());
    let entry = child(tables, table, true)?;
    if let Some((text, entry)) = description.zip(entry.as_table_like_mut()) {
        entry.insert("description", Item::Value(Value::from(text)));
    }
    let columns = child(entry, "columns", false)?
        .as_table_like_mut()
        .ok_or_else(|| {
            format!(
                "{} has a columns setting that is not a table",
                adoption.location
            )
        })?;
    for column in &adoption.columns {
        let mut typed = Value::from(column.physical_type.as_str());
        match columns.get_mut(&column.column).and_then(Item::as_value_mut) {
            Some(existing) => {
                *typed.decor_mut() = existing.decor().clone();
                *existing = typed;
            }
            None => {
                columns.insert(&column.column, Item::Value(typed));
            }
        }
    }
    for name in &adoption.removed {
        columns.remove(name);
    }
    Ok(document)
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
pub(crate) fn resolved(finding: &ContractFinding, adoption: &Adoption) -> bool {
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
