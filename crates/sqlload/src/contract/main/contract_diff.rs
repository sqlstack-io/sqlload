//! Compare every selected table's declaration with its live table.

use std::collections::BTreeMap;

use crate::adapters::dispatch::main::session::live_columns::live_columns;
use crate::adapters::dispatch::models::Destination;
use crate::contract::_helpers::compare::findings;
use crate::contract::constants::INSPECTION_UNAVAILABLE;
use crate::contract::models::{ContractFinding, ContractReport, ContractResource};
use crate::loader::_helpers::resolve::resolve_tables;
use crate::loader::_helpers::sources::{selected, source_tables, unmatched_selectors};
use crate::loader::errors::LoaderError;
use crate::loader::models::{PlanOptions, TableSpec};
use crate::project::models::ProjectConfig;

/// Each selected table's differences, reading only the destinations (sources are listed for their tables, never read); `from` reads every table from that target instead of its own.
pub async fn contract_diff(
    project: &ProjectConfig,
    destinations: &BTreeMap<String, Destination>,
    options: &PlanOptions,
    from: Option<&str>,
) -> Result<ContractReport, LoaderError> {
    let mut report = ContractReport {
        from_target: from.map(str::to_owned),
        ..ContractReport::default()
    };
    let mut names: Vec<(String, String)> = Vec::new();
    for (source_name, source) in &project.sources {
        let discovered = match source_tables(project, source_name, source).await? {
            Ok((discovered, _)) => discovered,
            Err(problem) => {
                report.problems.push(format!("[sources.{source_name}]: {problem}"));
                continue;
            }
        };
        let resolved = resolve_tables(project, source_name, &discovered);
        report.problems.extend(resolved.problems);
        for spec in resolved.specs {
            let qualified = spec.source_table.qualified_name();
            names.push((spec.destination.table.clone(), qualified.clone()));
            if !selected(options, (source_name, &spec.destination.table, &qualified)) {
                continue;
            }
            let target = from.unwrap_or(&spec.target);
            let destination = destinations.get(target).ok_or_else(|| {
                LoaderError::Invalid(format!("target {target:?} is not in sqlload.toml"))
            })?;
            report.resources.push(resource(&spec, destination).await);
        }
    }
    report.problems.extend(unmatched_selectors(project, options, &names));
    report.differences = report.resources.iter().map(|resource| resource.findings.len()).sum();
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
