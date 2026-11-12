//! What a table's source would load against its live table, for columns no declaration names.

use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::column_types::models::TypeFit;
use crate::contract::_helpers::compare::is_own;
use crate::contract::constants::{
    INSPECTION_UNAVAILABLE, SOURCE_MISSING_COLUMN, SOURCE_NEW_COLUMN, SOURCE_TYPE_MISMATCH,
};
use crate::contract::models::ContractFinding;
use crate::loader::models::TableSpec;
use crate::project::models::Contract;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn, LiveColumn};

/// The findings for `spec`, whose source would load `source_columns`, against `live` (`None` before the table exists, which gives none): source columns the live table lacks or holds as a type they do not fit, then undeclared live columns the source no longer has. Declared, constant, SQLLoad's own and enforced tables' columns are left to the declaration comparison; a Python table, whose columns are inferred only when it runs, and a source whose columns cannot be read are one `inspection_unavailable`.
pub(crate) fn source_drift(
    (spec, source_columns): (&TableSpec, &Result<Vec<DesiredColumn>, Vec<String>>),
    live: Option<&[LiveColumn]>,
    relation: &str,
) -> Vec<ContractFinding> {
    let Some(live) = live.filter(|_| spec.contract == Contract::None) else {
        return Vec::new();
    };
    let unavailable = |reason: String| {
        vec![ContractFinding {
            column: None,
            declared_type: None,
            kind: INSPECTION_UNAVAILABLE,
            message: format!("cannot read what the source of {relation} would load: {reason}"),
            physical_type: None,
            source_type: None,
        }]
    };
    if spec.python.is_some() {
        return unavailable(
            "a Python table's columns are known only when it runs, so declare them or compare after a load".to_owned(),
        );
    }
    let desired = match source_columns {
        Ok(desired) => desired,
        Err(problems) => return unavailable(problems.join("; ")),
    };
    let undeclared = |name: &str| {
        !is_own(name)
            && !spec
                .pinned
                .keys()
                .chain(spec.constants.iter().map(|(constant, _, _)| constant))
                .any(|declared| declared.eq_ignore_ascii_case(name))
    };
    let loaded: Vec<&DesiredColumn> = desired
        .iter()
        .filter(|column| matches!(column.origin, ColumnOrigin::Source | ColumnOrigin::Inferred))
        .filter(|column| undeclared(&column.name))
        .collect();
    let mut found: Vec<ContractFinding> = loaded
        .iter()
        .filter_map(|column| column_finding(spec, column, live))
        .collect();
    found.extend(
        live.iter()
            .filter(|held| undeclared(&held.name) && !is_loaded(&loaded, &held.name))
            .map(|held| ContractFinding {
                column: Some(held.name.clone()),
                declared_type: None,
                kind: SOURCE_MISSING_COLUMN,
                message: format!(
                    "physical column '{}' ({}) is not in the source",
                    held.name, held.column_type
                ),
                physical_type: Some(held.column_type.to_string()),
                source_type: None,
            }),
    );
    found
}

/// The finding for one column the source would load: absent from `live`, or of a type the live column does not hold as it is.
fn column_finding(
    spec: &TableSpec,
    column: &DesiredColumn,
    live: &[LiveColumn],
) -> Option<ContractFinding> {
    let source = column.column_type.to_string();
    let Some(held) = live
        .iter()
        .find(|held| held.name.eq_ignore_ascii_case(&column.name))
    else {
        return Some(ContractFinding {
            column: Some(column.name.clone()),
            declared_type: None,
            kind: SOURCE_NEW_COLUMN,
            message: format!(
                "column '{}' ({source}) is in the source but not in the physical relation",
                column.name
            ),
            physical_type: None,
            source_type: Some(source),
        });
    };
    let fit = sql_adapter(spec.warehouse).type_fit(&column.column_type, &held.column_type);
    let physical = held.column_type.to_string();
    (!matches!(fit, TypeFit::Fits)).then(|| ContractFinding {
        column: Some(held.name.clone()),
        declared_type: None,
        kind: SOURCE_TYPE_MISMATCH,
        message: format!(
            "column '{}' would load as {source} but physical type is {physical}",
            held.name
        ),
        physical_type: Some(physical),
        source_type: Some(source),
    })
}

/// Whether the source would load a column named `name`, ignoring case.
fn is_loaded(loaded: &[&DesiredColumn], name: &str) -> bool {
    loaded
        .iter()
        .any(|column| column.name.eq_ignore_ascii_case(name))
}
