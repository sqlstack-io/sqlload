//! A contract report as `sqb contract diff` and `sqb contract generate` print it.

use crate::contract::models::{Adoption, ContractReport};

/// The report's lines (one JSON document with `json`), and whether everything agrees: no difference and no problem.
pub fn render_contract(report: &ContractReport, json: bool) -> (Vec<String>, bool) {
    let agrees = report.differences == 0 && report.problems.is_empty();
    if json {
        let text = serde_json::to_string_pretty(report).unwrap_or_default();
        return (vec![text], agrees);
    }
    let mut lines: Vec<String> = vec![match &report.from_target {
        Some(target) => format!("Contract comparison from target '{target}'"),
        None => "Contract comparison".to_owned(),
    }];
    for resource in &report.resources {
        let label = format!("{}:{}", resource.resource_type, resource.resource_name);
        match resource.findings.is_empty() {
            true => lines.push(format!("  OK    {label}")),
            false => lines.push(format!("  DIFF  {label}")),
        }
        lines.extend(
            resource
                .findings
                .iter()
                .map(|finding| format!("        {}: {}", finding.kind, finding.message)),
        );
        lines.extend(resource.adoption.iter().flat_map(adoption_lines));
    }
    lines.extend(
        report
            .problems
            .iter()
            .map(|problem| format!("  ERROR {problem}")),
    );
    let unwritten = report
        .resources
        .iter()
        .filter_map(|resource| resource.adoption.as_ref())
        .any(|adoption| adoption.paste.is_none());
    if unwritten {
        lines.push(String::new());
        lines.push("Run contract generate --write to update sqlload.toml.".to_owned());
    }
    if !report.written_paths.is_empty() {
        lines.push(String::new());
        lines.push("Updated repository declarations:".to_owned());
        lines.extend(report.written_paths.iter().map(|path| format!("  {path}")));
    }
    lines.push(String::new());
    lines.push(format!("{} contract difference(s)", report.differences));
    (lines, agrees)
}

/// What an adoption declares: one line per column for sqlload.toml, the `columns={...}` to paste for a Python table.
fn adoption_lines(adoption: &Adoption) -> Vec<String> {
    if let Some(paste) = &adoption.paste {
        return vec![
            format!(
                "        adopt: paste into the table's declaration in {}",
                adoption.location
            ),
            format!("          {paste}"),
        ];
    }
    let mut lines = vec![format!("        adopt: columns of {}", adoption.location)];
    lines.extend(
        adoption
            .columns
            .iter()
            .map(|column| match &column.declared_type {
                Some(declared) => format!(
                    "          ~ {} = \"{}\" (declared {declared})",
                    column.column, column.physical_type
                ),
                None => format!(
                    "          + {} = \"{}\"",
                    column.column, column.physical_type
                ),
            }),
    );
    lines.extend(
        adoption
            .removed
            .iter()
            .map(|name| format!("          - {name} (not in the live table)")),
    );
    lines
}
