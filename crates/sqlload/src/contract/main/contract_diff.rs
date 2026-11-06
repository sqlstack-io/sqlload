//! Compare every selected table's declaration with its live table.

use std::collections::BTreeMap;

use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::session::live_columns::live_columns;
use crate::adapters::dispatch::models::Destination;
use crate::contract::_helpers::compare::findings;
use crate::contract::_helpers::read_from::read_from;
use crate::contract::constants::INSPECTION_UNAVAILABLE;
use crate::contract::models::{ContractFinding, ContractReport, ContractResource};
use crate::loader::errors::LoaderError;
use crate::loader::main::selected_specs::selected_specs;
use crate::loader::models::{PlanOptions, TableSpec};
use crate::project::models::ProjectConfig;

/// Each selected table's differences, reading only the destinations (sources are listed for their tables, never read); `from` reads every table from that target instead of its own, resolving its database and schema there as if the source loaded into it. Tables on a files target are problems: they have no SQL catalog to compare.
pub async fn contract_diff(
    project: &ProjectConfig,
    destinations: &BTreeMap<String, Destination>,
    options: &PlanOptions,
    from: Option<&str>,
) -> Result<ContractReport, LoaderError> {
    let project = read_from(project, destinations, from)?;
    let mut report = ContractReport {
        from_target: from.map(str::to_owned),
        ..ContractReport::default()
    };
    let chosen = selected_specs(&project, options).await?;
    report.problems = chosen.problems;
    for spec in &chosen.specs {
        if !sql_adapter(spec.warehouse).runs_sql() {
            report.problems.push(format!(
                "{}: target {} writes files; contract diff compares warehouse tables (DuckDB, Snowflake) only",
                spec.destination.table, spec.target
            ));
            continue;
        }
        let destination = destinations.get(&spec.target).ok_or_else(|| {
            LoaderError::Invalid(format!("target {:?} is not connected", spec.target))
        })?;
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
        Ok(live) => findings(
            &spec.pinned,
            spec.contract,
            live.as_deref(),
            (spec.warehouse, &relation),
        ),
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
