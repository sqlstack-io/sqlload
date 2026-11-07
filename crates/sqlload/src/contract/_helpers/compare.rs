//! A table's declaration against its live columns.

use std::collections::BTreeMap;

use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::contract::constants::{
    MISSING_DECLARATION, MISSING_PHYSICAL_COLUMN, RELATION_MISSING, SQLLOAD_COLUMN_PREFIX,
    TYPE_MISMATCH,
};
use crate::contract::models::ContractFinding;
use crate::project::models::Contract;
use crate::schema_plan::models::LiveColumn;

/// The differences between `declared` and `live` (`None` when the table does not exist) for `relation` on `warehouse`: live columns first in table order, then declared columns the table lacks. A type agrees when it is the same or the warehouse stores both alike (`varchar(50)` as DuckDB's `VARCHAR`); a wider live type is still a difference. Undeclared live columns count only under `contract = "enforced"`; SQLLoad's own columns never count.
pub(crate) fn findings(
    declared: &BTreeMap<String, ColumnType>,
    contract: Contract,
    live: Option<&[LiveColumn]>,
    (warehouse, relation): (Warehouse, &str),
) -> Vec<ContractFinding> {
    let Some(live) = live else {
        return vec![ContractFinding {
            column: None,
            declared_type: None,
            kind: RELATION_MISSING,
            message: format!("physical relation {relation} does not exist"),
            physical_type: None,
            source_type: None,
        }];
    };
    let declared_type = |name: &str| {
        declared
            .iter()
            .find(|(declared, _)| declared.eq_ignore_ascii_case(name))
    };
    let mut found: Vec<ContractFinding> = live
        .iter()
        .filter(|column| !is_own(&column.name))
        .filter_map(|column| live_finding(column, declared_type(&column.name), contract, warehouse))
        .collect();
    found.extend(
        declared
            .iter()
            .filter(|(name, _)| !is_live(live, name))
            .map(|(name, column_type)| ContractFinding {
                column: Some(name.clone()),
                declared_type: Some(column_type.to_string()),
                kind: MISSING_PHYSICAL_COLUMN,
                message: format!("declared column '{name}' is absent from the physical relation"),
                physical_type: None,
                source_type: None,
            }),
    );
    found
}

/// Whether SQLLoad added the column itself (`_sqlload_*`).
pub(crate) fn is_own(name: &str) -> bool {
    name.to_ascii_lowercase().starts_with(SQLLOAD_COLUMN_PREFIX)
}

/// Whether `live` has a column named `name`, ignoring case.
pub(crate) fn is_live(live: &[LiveColumn], name: &str) -> bool {
    live.iter()
        .any(|column| column.name.eq_ignore_ascii_case(name))
}

/// Whether `live` stores what `declared` declares: the same type, or one the warehouse renders the same way.
pub(crate) fn agrees(warehouse: Warehouse, declared: &ColumnType, live: &ColumnType) -> bool {
    let sql = |column_type: &ColumnType| {
        sql_adapter(warehouse)
            .render_loader_logical_type(column_type)
            .sql
    };
    declared == live || sql(declared).eq_ignore_ascii_case(&sql(live))
}

/// The finding for one live column, if it has one.
fn live_finding(
    column: &LiveColumn,
    declared: Option<(&String, &ColumnType)>,
    contract: Contract,
    warehouse: Warehouse,
) -> Option<ContractFinding> {
    let physical = column.column_type.to_string();
    match (declared, contract) {
        (Some((_, declared)), _) if agrees(warehouse, declared, &column.column_type) => None,
        (Some((name, declared)), _) => Some(ContractFinding {
            column: Some(name.clone()),
            declared_type: Some(declared.to_string()),
            kind: TYPE_MISMATCH,
            message: format!(
                "column '{name}' is declared {declared} but physical type is {physical}"
            ),
            physical_type: Some(physical),
            source_type: None,
        }),
        (None, Contract::Enforced) => Some(ContractFinding {
            column: Some(column.name.clone()),
            declared_type: None,
            kind: MISSING_DECLARATION,
            message: format!(
                "physical column '{}' ({physical}) is not declared",
                column.name
            ),
            physical_type: Some(physical),
            source_type: None,
        }),
        (None, Contract::None) => None,
    }
}
