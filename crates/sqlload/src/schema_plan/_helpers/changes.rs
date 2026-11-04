use crate::column_types::main::type_fit::type_fit;
use crate::column_types::main::warehouse_type::warehouse_type;
use crate::column_types::models::{TypeFit, Warehouse};
use crate::project::models::{OnSchemaChange, Policy};
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
        sql_type: warehouse_type(&column.column_type, rules.warehouse).sql,
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
    match (
        type_fit(&column.column_type, &live.column_type, rules.warehouse),
        rules.warehouse,
    ) {
        (TypeFit::Fits, _) => written,
        (_, Warehouse::Parquet) => ColumnDecision {
            problem: Some(format!(
                "{} is {} in the existing files but {} in the source; existing files cannot be altered, so pin the files' type (columns = {{ {} = \"{}\" }}) or rewrite every file with --reload",
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
