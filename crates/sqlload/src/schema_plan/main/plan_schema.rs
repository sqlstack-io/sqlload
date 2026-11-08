//! Decide a table's schema changes for one load.

use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::project::models::Contract;
use crate::schema_plan::_helpers::changes::{ColumnDecision, column_decision};
use crate::schema_plan::constants::LOAD_ID_COLUMN;
use crate::schema_plan::models::{
    Blocked, ColumnChange, ColumnOrigin, DesiredColumn, LiveColumn, SchemaPlan, SchemaRules,
    TableAction,
};

/// Compare `desired` with the `live` table (`None` when it does not exist yet) under `rules`.
pub fn plan_schema(
    desired: &[DesiredColumn],
    live: Option<&[LiveColumn]>,
    rules: SchemaRules,
) -> SchemaPlan {
    let mut warnings: Vec<String> = Vec::new();
    let mut kept: Vec<&DesiredColumn> = Vec::with_capacity(desired.len());
    for column in desired {
        let undeclared = matches!(column.origin, ColumnOrigin::Source | ColumnOrigin::Inferred);
        if rules.contract == Contract::Enforced && undeclared {
            warnings.push(format!(
                "{} is not in the enforced contract and is not loaded",
                column.name
            ));
        } else {
            kept.push(column);
        }
    }
    let Some(live) = live else {
        return SchemaPlan {
            action: TableAction::Create,
            write_columns: kept.into_iter().cloned().collect(),
            warnings,
            blocked: None,
        };
    };
    let owned = live
        .iter()
        .any(|live| live.name.eq_ignore_ascii_case(LOAD_ID_COLUMN));
    let kept_as_is = !owned && sql_adapter(rules.warehouse).keeps_tables();
    let mut changes: Vec<ColumnChange> = Vec::new();
    let mut write_columns: Vec<DesiredColumn> = Vec::new();
    let mut problems: Vec<String> = Vec::new();
    let mut fix_flag: Option<&'static str> = None;
    for column in kept {
        let existing = live
            .iter()
            .find(|live| live.name.eq_ignore_ascii_case(&column.name));
        let decision: ColumnDecision = column_decision(column, (existing, kept_as_is), rules);
        changes.extend(decision.change);
        warnings.extend(decision.warning);
        problems.extend(decision.problem);
        fix_flag = fix_flag.or(decision.fix_flag);
        if decision.write {
            write_columns.push(column.clone());
        }
    }
    SchemaPlan {
        action: match changes.is_empty() {
            true => TableAction::Unchanged,
            false => TableAction::Alter(changes),
        },
        write_columns,
        warnings,
        blocked: (!problems.is_empty()).then_some(Blocked {
            reasons: problems,
            fix_flag,
        }),
    }
}
