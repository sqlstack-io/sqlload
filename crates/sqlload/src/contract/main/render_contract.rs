//! A contract report as `sqb contract diff` prints it.

use crate::contract::models::ContractReport;

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
    }
    lines.extend(
        report
            .problems
            .iter()
            .map(|problem| format!("  ERROR {problem}")),
    );
    lines.push(String::new());
    lines.push(format!("{} contract difference(s)", report.differences));
    (lines, agrees)
}
