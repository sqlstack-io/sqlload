//! What a table's source would load against its live table, for columns no declaration names.

use crate::contract::_helpers::compare::{agrees, is_own};
use crate::contract::constants::{
    INSPECTION_UNAVAILABLE, SOURCE_MISSING_COLUMN, SOURCE_NEW_COLUMN, SOURCE_TYPE_MISMATCH,
};
use crate::contract::models::ContractFinding;
use crate::loader::models::{TablePlan, TableSpec};
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};

/// The findings for `spec` from its plan among `planned`: source columns the live table lacks or holds as another type, then undeclared live columns the source no longer has; nothing before the table exists, and one `inspection_unavailable` for a Python table, whose columns are inferred only when it runs, or when planning stopped before the source's columns were known. Declared and SQLLoad's own columns are left to the declaration comparison.
pub(crate) fn source_drift(
    spec: &TableSpec,
    planned: &[TablePlan],
    relation: &str,
) -> Vec<ContractFinding> {
    let Some(plan) = planned.iter().find(|plan| {
        plan.spec.source_name == spec.source_name && plan.spec.destination == spec.destination
    }) else {
        return Vec::new();
    };
    if spec.python.is_some() {
        return vec![ContractFinding {
            column: None,
            declared_type: None,
            kind: INSPECTION_UNAVAILABLE,
            message: format!(
                "cannot read what the source of {relation} would load: a Python table's columns are known only when it runs, so declare them or compare after a load"
            ),
            physical_type: None,
            source_type: None,
        }];
    }
    if plan.schema.desired.is_empty() {
        return plan
            .schema
            .blocked
            .iter()
            .map(|blocked| ContractFinding {
                column: None,
                declared_type: None,
                kind: INSPECTION_UNAVAILABLE,
                message: format!(
                    "cannot read what the source of {relation} would load: {}",
                    blocked.reasons.join("; ")
                ),
                physical_type: None,
                source_type: None,
            })
            .collect();
    }
    let Some(live) = plan.live.as_deref() else {
        return Vec::new();
    };
    let undeclared = |name: &str| {
        !is_own(name)
            && !spec
                .pinned
                .keys()
                .any(|declared| declared.eq_ignore_ascii_case(name))
    };
    let loaded: Vec<&DesiredColumn> = plan
        .schema
        .desired
        .iter()
        .filter(|column| matches!(column.origin, ColumnOrigin::Source | ColumnOrigin::Inferred))
        .filter(|column| undeclared(&column.name))
        .collect();
    let mut found: Vec<ContractFinding> = Vec::new();
    for column in &loaded {
        let source = column.column_type.to_string();
        match live
            .iter()
            .find(|held| held.name.eq_ignore_ascii_case(&column.name))
        {
            None => found.push(ContractFinding {
                column: Some(column.name.clone()),
                declared_type: None,
                kind: SOURCE_NEW_COLUMN,
                message: format!(
                    "column '{}' ({source}) is in the source but not in the physical relation",
                    column.name
                ),
                physical_type: None,
                source_type: Some(source),
            }),
            Some(held) if !agrees(spec.warehouse, &column.column_type, &held.column_type) => {
                let physical = held.column_type.to_string();
                found.push(ContractFinding {
                    column: Some(held.name.clone()),
                    declared_type: None,
                    kind: SOURCE_TYPE_MISMATCH,
                    message: format!(
                        "column '{}' would load as {source} but physical type is {physical}",
                        held.name
                    ),
                    physical_type: Some(physical),
                    source_type: Some(source),
                });
            }
            Some(_) => {}
        }
    }
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

/// Whether the source would load a column named `name`, ignoring case.
fn is_loaded(loaded: &[&DesiredColumn], name: &str) -> bool {
    loaded
        .iter()
        .any(|column| column.name.eq_ignore_ascii_case(name))
}
