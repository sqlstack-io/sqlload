use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::column_types::models::{ColumnType, TypeFit};
use crate::project::models::{Contract, OnSchemaChange, Policy};
use crate::schema_plan::constants::ALLOW_REBUILD_FLAG;
use crate::schema_plan::models::{
    ColumnChange, ColumnOrigin, DesiredColumn, LiveColumn, SchemaRules,
};

/// What one desired column needs from the live table.
#[derive(Debug, Default)]
pub(crate) struct ColumnDecision {
    pub(crate) change: Option<ColumnChange>,
    /// Write the column's values (false when `on_schema_change = "ignore"` drops a new field).
    pub(crate) write: bool,
    pub(crate) warning: Option<String>,
    pub(crate) problem: Option<String>,
    pub(crate) fix_flag: Option<&'static str>,
}

/// The decision for a desired column the live table lacks.
pub(crate) fn missing_column(column: &DesiredColumn, rules: SchemaRules) -> ColumnDecision {
    let add = || ColumnChange::Add {
        column: column.clone(),
        sql_type: sql_adapter(rules.warehouse)
            .render_loader_logical_type(&column.column_type)
            .sql,
    };
    let declared = matches!(column.origin, ColumnOrigin::Pinned | ColumnOrigin::Metadata);
    match rules.on_schema_change {
        _ if declared => ColumnDecision {
            change: Some(add()),
            write: true,
            ..ColumnDecision::default()
        },
        OnSchemaChange::AppendNewColumns => ColumnDecision {
            change: Some(add()),
            write: true,
            ..ColumnDecision::default()
        },
        OnSchemaChange::Ignore => ColumnDecision {
            warning: Some(format!(
                "new field {} is not loaded (on_schema_change = \"ignore\")",
                column.name
            )),
            ..ColumnDecision::default()
        },
        OnSchemaChange::Fail => ColumnDecision {
            problem: Some(format!(
                "new field {} is not in the table and on_schema_change = \"fail\"",
                column.name
            )),
            ..ColumnDecision::default()
        },
    }
}

/// The decision for a desired column the live table has.
pub(crate) fn existing_column(
    column: &DesiredColumn,
    live: &LiveColumn,
    rules: SchemaRules,
) -> ColumnDecision {
    let written = ColumnDecision {
        write: true,
        ..ColumnDecision::default()
    };
    let adapter = sql_adapter(rules.warehouse);
    let fit = adapter.type_fit(&column.column_type, &live.column_type);
    let widens = matches!(fit, TypeFit::WidenInPlace(_) | TypeFit::NeedsRebuild(_));
    if widens && column.origin == ColumnOrigin::Inferred && adapter.runs_sql() {
        let wider = format!(
            "{} is {} in the table but {} in the source; pin the wider type to widen it, for example columns = {{ {} = \"{}\" }}",
            live.name, live.column_type, column.column_type, column.name, column.column_type
        );
        let refusal = match (rules.auto_widen, &column.column_type) {
            (false, _) => Some(format!("{wider}, or set auto_widen = true")),
            (
                true,
                ColumnType::SmallInt
                | ColumnType::Int
                | ColumnType::Decimal { .. }
                | ColumnType::TimestampTz,
            ) => None,
            (true, _) => Some(format!(
                "{wider}; auto_widen never widens to {}",
                column.column_type
            )),
        };
        if let Some(problem) = refusal {
            return ColumnDecision {
                problem: Some(problem),
                ..ColumnDecision::default()
            };
        }
    }
    match (fit, adapter.runs_sql()) {
        (TypeFit::Fits, _) => written,
        (_, false) => ColumnDecision {
            problem: Some(format!(
                "{} is {} in the existing files but {} in the source; existing files cannot be altered, so pin the files' type (columns = {{ {} = \"{}\" }}) or rewrite every file with --reload (--resnapshot for a postgres_cdc table)",
                live.name, live.column_type, column.column_type, column.name, live.column_type
            )),
            ..ColumnDecision::default()
        },
        (TypeFit::WidenInPlace(sql_type), _) => ColumnDecision {
            change: Some(ColumnChange::Widen {
                column: live.name.clone(),
                from: live.column_type.clone(),
                to: column.column_type.clone(),
                sql_type,
            }),
            ..written
        },
        (TypeFit::NeedsRebuild(sql_type), _) => rebuild(column, live, rules, sql_type),
        (TypeFit::Incompatible, _) => ColumnDecision {
            problem: Some(mismatch(column, live)),
            ..ColumnDecision::default()
        },
    }
}

fn rebuild(
    column: &DesiredColumn,
    live: &LiveColumn,
    rules: SchemaRules,
    sql_type: String,
) -> ColumnDecision {
    let change = ColumnChange::Rebuild {
        column: live.name.clone(),
        from: live.column_type.clone(),
        to: column.column_type.clone(),
        sql_type,
    };
    let description = format!(
        "{} {} -> {} needs a table rebuild",
        live.name, live.column_type, column.column_type
    );
    match (rules.type_change_rebuild, rules.rebuild_allowed) {
        (Policy::Allow, _) | (Policy::RequireConfirmation, true) => ColumnDecision {
            change: Some(change),
            write: true,
            ..ColumnDecision::default()
        },
        (Policy::RequireConfirmation, false) => ColumnDecision {
            problem: Some(format!(
                "{description} (type_change_rebuild = \"require_confirmation\")"
            )),
            fix_flag: Some(ALLOW_REBUILD_FLAG),
            ..ColumnDecision::default()
        },
        (Policy::Deny, _) => ColumnDecision {
            problem: Some(format!("{description}, and type_change_rebuild = \"deny\"")),
            ..ColumnDecision::default()
        },
    }
}

fn mismatch(column: &DesiredColumn, live: &LiveColumn) -> String {
    let fix = match column.origin {
        ColumnOrigin::Pinned => "change the pinned type, or migrate the column by hand".to_owned(),
        _ => format!(
            "pin a type the table can hold, for example columns = {{ {} = \"{}\" }}",
            column.name, live.column_type
        ),
    };
    format!(
        "{} is {} in the table but {} in the source; {fix}",
        live.name, live.column_type, column.column_type
    )
}

/// The decision for `column`, given the live table's column of its name (`None` when it lacks one); `kept_as_is` when the table is one SQLLoad did not create on a warehouse that keeps such tables as they are.
pub(crate) fn column_decision(
    column: &DesiredColumn,
    (existing, kept_as_is): (Option<&LiveColumn>, bool),
    rules: SchemaRules,
) -> ColumnDecision {
    match (existing, kept_as_is) {
        (Some(live), true) => existing_as_is(column, live, rules),
        (None, true) => missing_as_is(column, rules),
        (Some(live), false) => existing_column(column, live, rules),
        (None, false) => missing_column(column, rules),
    }
}

/// The decision for a column of a table SQLLoad did not create, on a warehouse that keeps such tables as they are: written when the table's column can take its values by an assignment cast (a value that does not fit then fails the commit, changing nothing), never widened.
fn existing_as_is(column: &DesiredColumn, live: &LiveColumn, rules: SchemaRules) -> ColumnDecision {
    match sql_adapter(rules.warehouse).type_fit(&column.column_type, &live.column_type) {
        TypeFit::Incompatible => ColumnDecision {
            problem: Some(mismatch(column, live)),
            ..ColumnDecision::default()
        },
        TypeFit::Fits | TypeFit::WidenInPlace(_) | TypeFit::NeedsRebuild(_) => ColumnDecision {
            write: true,
            ..ColumnDecision::default()
        },
    }
}

/// The decision for a column a table SQLLoad did not create lacks: never added; the `_sqlload_*` columns are left out quietly, a source column with a warning, unless the contract or `on_schema_change = "fail"` needs it.
fn missing_as_is(column: &DesiredColumn, rules: SchemaRules) -> ColumnDecision {
    let place = format!(
        "{} is not in the table, which SQLLoad did not create and does not alter",
        column.name
    );
    match (column.origin, rules.contract, rules.on_schema_change) {
        (ColumnOrigin::Metadata, _, _) => ColumnDecision::default(),
        (ColumnOrigin::Pinned, Contract::Enforced, _) | (_, _, OnSchemaChange::Fail) => {
            ColumnDecision {
                problem: Some(format!("{place}; add it to the table, or exclude it")),
                ..ColumnDecision::default()
            }
        }
        _ => ColumnDecision {
            warning: Some(format!("{place}, so it is not loaded")),
            ..ColumnDecision::default()
        },
    }
}
