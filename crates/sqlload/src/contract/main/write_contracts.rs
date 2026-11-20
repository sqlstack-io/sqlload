//! `contract generate --write`: adoptions written into sqlload.toml (`sqb contract generate --write`).

use toml_edit::DocumentMut;

use crate::contract::_helpers::toml_columns::{resolved, write_adoption};
use crate::contract::models::ContractReport;

/// `text` (sqlload.toml) with each adoption of a table declared there written into its `[sources.<source>.tables.<table>]` `columns`, which are created when missing, keeping the rest of the file as it was (`None` when nothing changes); and the report listing only the findings left, without the adoptions it wrote. A Python table's adoption is left to paste.
pub fn write_contracts(
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
            .take_if(|adoption| adoption.paste.is_none())
        else {
            continue;
        };
        document = write_adoption(document, &adoption)?;
        resource
            .findings
            .retain(|finding| !resolved(finding, &adoption));
        changed = true;
    }
    report.differences = report
        .resources
        .iter()
        .map(|resource| resource.findings.len())
        .sum();
    Ok((changed.then(|| document.to_string()), report))
}
