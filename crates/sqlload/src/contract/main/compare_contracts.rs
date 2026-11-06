//! Compare every selected table's declaration with its live table (`sqb contract diff`), and propose the declarations that adopt it (`sqb contract generate`).

use std::collections::BTreeMap;

use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::session::live_columns::live_columns;
use crate::adapters::dispatch::models::Destination;
use crate::contract::_helpers::adopt::adoption;
use crate::contract::_helpers::compare::findings;
use crate::contract::_helpers::read_from::read_from;
use crate::contract::constants::INSPECTION_UNAVAILABLE;
use crate::contract::models::{ContractFinding, ContractReport, ContractRequest, ContractResource};
use crate::loader::errors::LoaderError;
use crate::loader::main::selected_specs::selected_specs;
use crate::loader::models::{PlanOptions, TableSpec};
use crate::project::models::ProjectConfig;

/// Each selected table's differences, reading only the destinations (sources are listed for their tables, never read); `request.from` reads every table from that target instead of its own, resolving its database and schema there as if the source loaded into it, and `request.generate` adds each table's adoption. Tables on a files target are problems: they have no SQL catalog to compare.
pub async fn compare_contracts(
    project: &ProjectConfig,
    destinations: &BTreeMap<String, Destination>,
    options: &PlanOptions,
    request: &ContractRequest,
) -> Result<ContractReport, LoaderError> {
    let project = read_from(project, destinations, request.from.as_deref())?;
    let mut report = ContractReport {
        from_target: request.from.clone(),
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
        report
            .resources
            .push(resource(spec, destination, request.generate).await);
    }
    report.differences = report
        .resources
        .iter()
        .map(|resource| resource.findings.len())
        .sum();
    Ok(report)
}

/// One table's comparison, with its adoption under `generate`; a table whose columns cannot be read is one `inspection_unavailable` finding.
async fn resource(
    spec: &TableSpec,
    destination: &Destination,
    generate: Option<bool>,
) -> ContractResource {
    let relation = format!("{}.{}", spec.destination.schema, spec.destination.table);
    let (findings, adoption) = match live_columns(destination, &spec.destination).await {
        Ok(live) => (
            findings(
                &spec.pinned,
                spec.contract,
                live.as_deref(),
                (spec.warehouse, &relation),
            ),
            live.zip(generate)
                .and_then(|(live, overwrite)| adoption(spec, &live, overwrite)),
        ),
        Err(error) => (
            vec![ContractFinding {
                column: None,
                declared_type: None,
                kind: INSPECTION_UNAVAILABLE,
                message: format!("cannot read the columns of {relation}: {error}"),
                physical_type: None,
            }],
            None,
        ),
    };
    ContractResource {
        findings,
        relation,
        resource_name: spec.destination.table.clone(),
        resource_type: "table",
        adoption,
    }
}
