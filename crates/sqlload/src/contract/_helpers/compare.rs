//! A table's declaration against its live columns.

use std::collections::BTreeMap;

use crate::column_types::models::ColumnType;
use crate::contract::constants::{
    MISSING_DECLARATION, MISSING_PHYSICAL_COLUMN, RELATION_MISSING, SQLLOAD_COLUMN_PREFIX,
    TYPE_MISMATCH,
};
use crate::contract::models::ContractFinding;
use crate::project::models::Contract;
use crate::schema_plan::models::LiveColumn;

/// The differences between `declared` and `live` (`None` when the table does not exist): live columns first in table order, then declared columns the table lacks. Undeclared live columns count only under `contract = "enforced"`; SQLLoad's own columns never count.
pub(crate) fn findings(
    declared: &BTreeMap<String, ColumnType>,
    contract: Contract,
    live: Option<&[LiveColumn]>,
    relation: &str,
) -> Vec<ContractFinding> {
    let Some(live) = live else {
        return vec![ContractFinding {
            column: None,
            declared_type: None,
            kind: RELATION_MISSING,
            message: format!("physical relation {relation} does not exist"),
            physical_type: None,
        }];
    };
    let own = |name: &str| {
        name.to_ascii_lowercase()
            .starts_with(SQLLOAD_COLUMN_PREFIX)
    };
    let declared_type = |name: &str| {
        declared
            .iter()
            .find(|(declared, _)| declared.eq_ignore_ascii_case(name))
    };
    let mut found: Vec<ContractFinding> = live
        .iter()
        .filter(|column| !own(&column.name))
        .filter_map(|column| live_finding(column, declared_type(&column.name), contract))
        .collect();
    found.extend(
        declared
            .iter()
            .filter(|(name, _)| !live.iter().any(|column| column.name.eq_ignore_ascii_case(name)))
            .map(|(name, column_type)| ContractFinding {
                column: Some(name.clone()),
                declared_type: Some(column_type.to_string()),
                kind: MISSING_PHYSICAL_COLUMN,
                message: format!("declared column '{name}' is absent from the physical relation"),
                physical_type: None,
            }),
    );
    found
}

/// The finding for one live column, if it has one.
fn live_finding(
    column: &LiveColumn,
    declared: Option<(&String, &ColumnType)>,
    contract: Contract,
) -> Option<ContractFinding> {
    let physical = column.column_type.to_string();
    match (declared, contract) {
        (Some((_, declared)), _) if *declared == column.column_type => None,
        (Some((name, declared)), _) => Some(ContractFinding {
            column: Some(name.clone()),
            declared_type: Some(declared.to_string()),
            kind: TYPE_MISMATCH,
            message: format!("column '{name}' is declared {declared} but physical type is {physical}"),
            physical_type: Some(physical),
        }),
        (None, Contract::Enforced) => Some(ContractFinding {
            column: Some(column.name.clone()),
            declared_type: None,
            kind: MISSING_DECLARATION,
            message: format!("physical column '{}' ({physical}) is not declared", column.name),
            physical_type: Some(physical),
        }),
        (None, Contract::None) => None,
    }
}
