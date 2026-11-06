//! Compare every selected table's declaration with its live table (`sqb contract diff`), and propose the declarations that adopt it (`sqb contract generate`).

use std::collections::BTreeMap;

use crate::adapters::dispatch::main::dialect::files_destination::files_destination;
use crate::adapters::dispatch::main::session::live_columns::live_columns;
use crate::adapters::dispatch::models::Destination;
use crate::adapters::files::models::FilesDestination;
use crate::column_types::models::ColumnType;
use crate::contract::_helpers::adopt::adoption;
use crate::contract::_helpers::compare::findings;
use crate::contract::_helpers::file_drift::file_drift;
use crate::contract::_helpers::read_from::{other_warehouse, read_from};
use crate::contract::_helpers::source_drift::source_drift;
use crate::contract::constants::INSPECTION_UNAVAILABLE;
use crate::contract::models::{ContractFinding, ContractReport, ContractRequest, ContractResource};
use crate::loader::errors::LoaderError;
use crate::loader::main::files_columns::files_columns;
use crate::loader::main::plan_project::plan_project;
use crate::loader::main::selected_specs::selected_specs;
use crate::loader::models::{FilesColumns, PlanOptions, TablePlan, TableSpec};
use crate::project::models::ProjectConfig;

/// Each selected table's differences, reading only the destinations (sources are listed for their tables, never read); `request.from` reads every table from that target instead of its own, resolving its database and schema there as if the source loaded into it, and `request.generate` adds each table's adoption, except for tables whose own target is another kind of warehouse than `from`, which are problems. A table on a files target is compared with its newest load's footers, as `plan` reads them, and with `request.all_files` every file's footer is compared with that load too. With `request.source` the tables are also planned as `plan` plans them (reading their sources), and what each would load is compared with its live table for undeclared columns.
pub async fn compare_contracts(
    project: &ProjectConfig,
    destinations: &BTreeMap<String, Destination>,
    options: &PlanOptions,
    request: &ContractRequest,
) -> Result<ContractReport, LoaderError> {
    let elsewhere = other_warehouse(project, request.from.as_deref());
    let project = read_from(project, destinations, request.from.as_deref())?;
    let mut report = ContractReport {
        from_target: request.from.clone(),
        ..ContractReport::default()
    };
    let planned = match request.source {
        true => plan_project(&project, destinations, options).await?.tables,
        false => Vec::new(),
    };
    let chosen = selected_specs(&project, options).await?;
    report.problems = chosen.problems;
    for spec in &chosen.specs {
        let destination = destinations.get(&spec.target).ok_or_else(|| {
            LoaderError::Invalid(format!("target {:?} is not connected", spec.target))
        })?;
        let generate = match elsewhere.contains(&spec.source_name) {
            true => {
                report.problems.extend(request.generate.map(|_| format!(
                    "{}: not adopted: its own target is another kind of warehouse than --from {}, whose types it may not take",
                    spec.destination.table, spec.target
                )));
                None
            }
            false => request.generate,
        };
        let resource = match files_destination(destination) {
            Some(files) => {
                files_resource((spec, &planned), files, (generate, request.all_files)).await
            }
            None => resource((spec, &planned), destination, generate).await,
        };
        report.resources.push(resource);
    }
    report.differences = report
        .resources
        .iter()
        .map(|resource| resource.findings.len())
        .sum();
    Ok(report)
}

/// One table's comparison, with its adoption under `generate` and its source drift from its plan among `planned`; a table whose columns cannot be read is one `inspection_unavailable` finding.
async fn resource(
    (spec, planned): (&TableSpec, &[TablePlan]),
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
                source_type: None,
            }],
            None,
        ),
    };
    let mut findings = findings;
    findings.extend(source_drift(spec, planned, &relation));
    ContractResource {
        findings,
        relation,
        resource_name: spec.destination.table.clone(),
        resource_type: "table",
        adoption,
    }
}

/// One files table's comparison, with its source drift from its plan among `planned`: declared columns against the newest load's footers, `relation_missing` before any file; with `all_files`, also each column on which older files differ from that load. Folder, listing and footer problems (an unreadable file, a store error) are `inspection_unavailable` findings; footer types with no portable type are never adopted, since their type would be the reason, not a type.
async fn files_resource(
    (spec, planned): (&TableSpec, &[TablePlan]),
    destination: &FilesDestination,
    (generate, all_files): (Option<bool>, bool),
) -> ContractResource {
    let read = match files_columns(spec, destination, all_files).await {
        Ok(read) => read,
        Err(error) => FilesColumns {
            problems: vec![error.to_string()],
            ..FilesColumns::default()
        },
    };
    let relation = match read.folder.is_empty() {
        true => spec.destination.table.clone(),
        false => read.folder.clone(),
    };
    let mut found: Vec<ContractFinding> = read
        .problems
        .iter()
        .map(|problem| ContractFinding {
            column: None,
            declared_type: None,
            kind: INSPECTION_UNAVAILABLE,
            message: format!("cannot read the columns of {relation}: {problem}"),
            physical_type: None,
            source_type: None,
        })
        .collect();
    let readable = read.problems.is_empty();
    if readable {
        found.extend(findings(
            &spec.pinned,
            spec.contract,
            read.live.as_deref(),
            (spec.warehouse, &relation),
        ));
    }
    if let Some(live) = read.live.as_deref().filter(|_| readable) {
        found.extend(file_drift(live, &read.files));
    }
    let adoption = match (read.live.filter(|_| readable), generate) {
        (Some(mut live), Some(overwrite)) => {
            live.retain(|column| !matches!(column.column_type, ColumnType::Native(_)));
            adoption(spec, &live, overwrite)
        }
        _ => None,
    };
    found.extend(source_drift(spec, planned, &relation));
    ContractResource {
        findings: found,
        relation,
        resource_name: spec.destination.table.clone(),
        resource_type: "table",
        adoption,
    }
}
