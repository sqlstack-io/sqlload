//! `contract generate --write`: adoptions written into sqlload.toml (`sqb contract generate --write`).

use toml_edit::DocumentMut;

use crate::contract::_helpers::toml_columns::{lacks_description, resolved, write_adoption};
use crate::contract::models::ContractReport;

/// `text` (sqlload.toml) with each adoption of a table declared there written into its `[sources.<source>.tables.<table>]` `columns`, which are created when missing, keeping the rest of the file as it was (`None` when nothing changes); and the report listing only the findings left. A Python table's adoption is left to paste, and a database table that would need a new entry but has no description is a problem instead.
pub fn adopt_contracts(
    text: &str,
    mut report: ContractReport,
) -> Result<(Option<String>, ContractReport), String> {
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
        if lacks_description(document.as_item(), adoption) {
            report.problems.push(format!(
                "{}: columns not written: a table entry needs a description and this table has none; add description = \"...\" there, then rerun contract generate --write",
                adoption.location
            ));
            continue;
        }
        document = write_adoption(document, adoption)?;
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
    Ok((changed.then(|| document.to_string()), report))
}
