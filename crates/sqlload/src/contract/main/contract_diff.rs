//! Compare every selected table's declaration with its live table.

use std::collections::BTreeMap;

use crate::adapters::dispatch::main::session::live_columns::live_columns;
use crate::adapters::dispatch::models::Destination;
use crate::contract::_helpers::compare::findings;
use crate::contract::constants::INSPECTION_UNAVAILABLE;
use crate::contract::models::{ContractFinding, ContractReport, ContractResource};
use crate::loader::errors::LoaderError;
use crate::loader::main::selected_specs::selected_specs;
use crate::loader::models::{PlanOptions, TableSpec};
use crate::project::models::ProjectConfig;

/// Each selected table's differences, reading only the destinations (sources are listed for their tables, never read); `from` reads every table from that target instead of its own.
pub async fn contract_diff(
    project: &ProjectConfig,
    destinations: &BTreeMap<String, Destination>,
    options: &PlanOptions,
    from: Option<&str>,
) -> Result<ContractReport, LoaderError> {
    if let Some(target) = from.filter(|target| !destinations.contains_key(*target)) {
        let known: Vec<&str> = destinations.keys().map(String::as_str).collect();
        return Err(LoaderError::Invalid(format!(
            "--from {target} is not a target in sqlload.toml (it has: {})",
            known.join(", ")
        )));
    }
    let mut report = ContractReport {
        from_target: from.map(str::to_owned),
        ..ContractReport::default()
    };
    let chosen = selected_specs(project, options).await?;
    report.problems = chosen.problems;
    for spec in &chosen.specs {
        let target = from.unwrap_or(&spec.target);
        let destination = destinations
            .get(target)
            .ok_or_else(|| LoaderError::Invalid(format!("target {target:?} is not connected")))?;
        report.resources.push(resource(spec, destination).await);
    }
    report.differences = report
        .resources
        .iter()
        .map(|resource| resource.findings.len())
        .sum();
    Ok(report)
}

/// One table's comparison; a table whose columns cannot be read is one `inspection_unavailable` finding.
async fn resource(spec: &TableSpec, destination: &Destination) -> ContractResource {
    let relation = format!("{}.{}", spec.destination.schema, spec.destination.table);
    let findings = match live_columns(destination, &spec.destination).await {
        Ok(live) => findings(&spec.pinned, spec.contract, live.as_deref(), &relation),
        Err(error) => vec![ContractFinding {
            column: None,
            declared_type: None,
            kind: INSPECTION_UNAVAILABLE,
            message: format!("cannot read the columns of {relation}: {error}"),
            physical_type: None,
        }],
    };
    ContractResource {
        findings,
        relation,
        resource_name: spec.destination.table.clone(),
        resource_type: "table",
    }
}
