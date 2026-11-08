use crate::column_types::models::{ColumnType, Warehouse};
use crate::project::models::{Contract, OnSchemaChange, Policy};
use crate::schema_plan::main::plan_schema::plan_schema;
use crate::schema_plan::models::SchemaRules;
use crate::schema_plan::tests::helpers::{
    inferred, live, pinned, postgres_rules, snowflake_rules, source,
};
use crate::schema_plan::tests::test_types::PlanSchemaTestCase;

const DECIMAL: ColumnType = ColumnType::Decimal {
    precision: 18,
    scale: 2,
};

#[test]
fn given_desired_and_live_columns_when_planning_then_changes_follow_the_policies() {
    let test_cases = [
        PlanSchemaTestCase {
            description: "a missing table is created with every column",
            desired: vec![
                source("id", ColumnType::Int),
                source("name", ColumnType::Text),
            ],
            live: None,
            rules: snowflake_rules(),
            expected_action: &["create"],
            expected_written: &["id", "name"],
            expected_blocked: "",
            expected_fix_flag: None,
            expected_warnings: 0,
        },
        PlanSchemaTestCase {
            description: "a matching table is unchanged, matching names case-insensitively",
            desired: vec![source("id", ColumnType::Int)],
            live: Some(vec![
                live("ID", ColumnType::Int),
                live("EXTRA", ColumnType::Date),
            ]),
            rules: snowflake_rules(),
            expected_action: &["unchanged"],
            expected_written: &["id"],
            expected_blocked: "",
            expected_fix_flag: None,
            expected_warnings: 0,
        },
        PlanSchemaTestCase {
            description: "a new source column is added",
            desired: vec![
                source("id", ColumnType::Int),
                source("tier", ColumnType::Int),
            ],
            live: Some(vec![live("ID", ColumnType::Int)]),
            rules: snowflake_rules(),
            expected_action: &["add column  tier int -> NUMBER(38,0)"],
            expected_written: &["id", "tier"],
            expected_blocked: "",
            expected_fix_flag: None,
            expected_warnings: 0,
        },
        PlanSchemaTestCase {
            description: "on_schema_change = ignore drops the new column with a warning",
            desired: vec![
                source("id", ColumnType::Int),
                source("tier", ColumnType::Int),
            ],
            live: Some(vec![live("ID", ColumnType::Int)]),
            rules: SchemaRules {
                on_schema_change: OnSchemaChange::Ignore,
                ..snowflake_rules()
            },
            expected_action: &["unchanged"],
            expected_written: &["id"],
            expected_blocked: "",
            expected_fix_flag: None,
            expected_warnings: 1,
        },
        PlanSchemaTestCase {
            description: "on_schema_change = fail blocks the table",
            desired: vec![
                source("id", ColumnType::Int),
                source("tier", ColumnType::Int),
            ],
            live: Some(vec![live("ID", ColumnType::Int)]),
            rules: SchemaRules {
                on_schema_change: OnSchemaChange::Fail,
                ..snowflake_rules()
            },
            expected_action: &["unchanged"],
            expected_written: &["id"],
            expected_blocked: "on_schema_change = \"fail\"",
            expected_fix_flag: None,
            expected_warnings: 0,
        },
        PlanSchemaTestCase {
            description: "a pinned column is added even when new fields are ignored",
            desired: vec![pinned("amount", DECIMAL)],
            live: Some(vec![]),
            rules: SchemaRules {
                on_schema_change: OnSchemaChange::Ignore,
                ..snowflake_rules()
            },
            expected_action: &["add column  amount decimal(18,2) -> NUMBER(18,2)"],
            expected_written: &["amount"],
            expected_blocked: "",
            expected_fix_flag: None,
            expected_warnings: 0,
        },
        PlanSchemaTestCase {
            description: "a longer varchar widens in place",
            desired: vec![source("name", ColumnType::Varchar(128))],
            live: Some(vec![live("NAME", ColumnType::Varchar(64))]),
            rules: snowflake_rules(),
            expected_action: &["widen  NAME varchar(64) -> varchar(128) (VARCHAR(128))  in place"],
            expected_written: &["name"],
            expected_blocked: "",
            expected_fix_flag: None,
            expected_warnings: 0,
        },
        PlanSchemaTestCase {
            description: "a rebuild waits for confirmation by default",
            desired: vec![pinned("amount", DECIMAL)],
            live: Some(vec![live("AMOUNT", ColumnType::Int)]),
            rules: snowflake_rules(),
            expected_action: &["unchanged"],
            expected_written: &[],
            expected_blocked: "needs a table rebuild (type_change_rebuild = \"require_confirmation\")",
            expected_fix_flag: Some("--allow-type-change-rebuild"),
            expected_warnings: 0,
        },
        PlanSchemaTestCase {
            description: "the flag confirms a rebuild",
            desired: vec![pinned("amount", DECIMAL)],
            live: Some(vec![live("AMOUNT", ColumnType::Int)]),
            rules: SchemaRules {
                rebuild_allowed: true,
                ..snowflake_rules()
            },
            expected_action: &["rebuild  AMOUNT int -> decimal(18,2) (NUMBER(21,2))"],
            expected_written: &["amount"],
            expected_blocked: "",
            expected_fix_flag: None,
            expected_warnings: 0,
        },
        PlanSchemaTestCase {
            description: "deny blocks a rebuild even with the flag",
            desired: vec![pinned("amount", DECIMAL)],
            live: Some(vec![live("AMOUNT", ColumnType::Int)]),
            rules: SchemaRules {
                type_change_rebuild: Policy::Deny,
                rebuild_allowed: true,
                ..snowflake_rules()
            },
            expected_action: &["unchanged"],
            expected_written: &[],
            expected_blocked: "type_change_rebuild = \"deny\"",
            expected_fix_flag: None,
            expected_warnings: 0,
        },
        PlanSchemaTestCase {
            description: "an unrelated type change blocks with the pinning fix",
            desired: vec![source("code", ColumnType::Text)],
            live: Some(vec![live("CODE", ColumnType::Int)]),
            rules: snowflake_rules(),
            expected_action: &["unchanged"],
            expected_written: &[],
            expected_blocked: "CODE is int in the table but text in the source; pin a type the table can hold, for example columns = { code = \"int\" }",
            expected_fix_flag: None,
            expected_warnings: 0,
        },
        PlanSchemaTestCase {
            description: "an inferred column that outgrows the table fails without auto_widen",
            desired: vec![inferred("amount", DECIMAL)],
            live: Some(vec![live("AMOUNT", ColumnType::Int)]),
            rules: SchemaRules {
                rebuild_allowed: true,
                ..snowflake_rules()
            },
            expected_action: &["unchanged"],
            expected_written: &[],
            expected_blocked: "AMOUNT is int in the table but decimal(18,2) in the source; pin the wider type to widen it, for example columns = { amount = \"decimal(18,2)\" }, or set auto_widen = true",
            expected_fix_flag: None,
            expected_warnings: 0,
        },
        PlanSchemaTestCase {
            description: "auto_widen widens an inferred int to decimal and shows it",
            desired: vec![inferred("amount", DECIMAL)],
            live: Some(vec![live("AMOUNT", ColumnType::Int)]),
            rules: SchemaRules {
                auto_widen: true,
                rebuild_allowed: true,
                ..snowflake_rules()
            },
            expected_action: &["rebuild  AMOUNT int -> decimal(18,2) (NUMBER(21,2))"],
            expected_written: &["amount"],
            expected_blocked: "",
            expected_fix_flag: None,
            expected_warnings: 0,
        },
        PlanSchemaTestCase {
            description: "auto_widen still waits for rebuild confirmation",
            desired: vec![inferred("amount", DECIMAL)],
            live: Some(vec![live("AMOUNT", ColumnType::Int)]),
            rules: SchemaRules {
                auto_widen: true,
                ..snowflake_rules()
            },
            expected_action: &["unchanged"],
            expected_written: &[],
            expected_blocked: "needs a table rebuild (type_change_rebuild = \"require_confirmation\")",
            expected_fix_flag: Some("--allow-type-change-rebuild"),
            expected_warnings: 0,
        },
        PlanSchemaTestCase {
            description: "auto_widen widens an inferred timestamp_ntz to timestamp_tz",
            desired: vec![inferred("seen_at", ColumnType::TimestampTz)],
            live: Some(vec![live("SEEN_AT", ColumnType::TimestampNtz)]),
            rules: SchemaRules {
                auto_widen: true,
                rebuild_allowed: true,
                ..snowflake_rules()
            },
            expected_action: &["rebuild  SEEN_AT timestamp_ntz -> timestamp_tz (TIMESTAMP_TZ(9))"],
            expected_written: &["seen_at"],
            expected_blocked: "",
            expected_fix_flag: None,
            expected_warnings: 0,
        },
        PlanSchemaTestCase {
            description: "auto_widen never widens to text",
            desired: vec![inferred("name", ColumnType::Varchar(128))],
            live: Some(vec![live("NAME", ColumnType::Varchar(64))]),
            rules: SchemaRules {
                auto_widen: true,
                ..snowflake_rules()
            },
            expected_action: &["unchanged"],
            expected_written: &[],
            expected_blocked: "auto_widen never widens to varchar(128)",
            expected_fix_flag: None,
            expected_warnings: 0,
        },
        PlanSchemaTestCase {
            description: "an enforced contract loads only declared columns",
            desired: vec![
                pinned("id", ColumnType::Int),
                source("other", ColumnType::Text),
            ],
            live: None,
            rules: SchemaRules {
                contract: Contract::Enforced,
                ..snowflake_rules()
            },
            expected_action: &["create"],
            expected_written: &["id"],
            expected_blocked: "",
            expected_fix_flag: None,
            expected_warnings: 1,
        },
        PlanSchemaTestCase {
            description: "a type change in Parquet files is blocked, even a widening",
            desired: vec![source("qty", ColumnType::Int)],
            live: Some(vec![live("qty", ColumnType::SmallInt)]),
            rules: SchemaRules {
                warehouse: Warehouse::Parquet,
                ..snowflake_rules()
            },
            expected_action: &["unchanged"],
            expected_written: &[],
            expected_blocked: "qty is smallint in the existing files but int in the source; existing files cannot be altered",
            expected_fix_flag: None,
            expected_warnings: 0,
        },
        PlanSchemaTestCase {
            description: "a new column in Parquet files is added to new files",
            desired: vec![
                source("id", ColumnType::Int),
                source("tier", ColumnType::Text),
            ],
            live: Some(vec![live("id", ColumnType::Int)]),
            rules: SchemaRules {
                warehouse: Warehouse::Parquet,
                ..snowflake_rules()
            },
            expected_action: &["add column  tier text -> Utf8"],
            expected_written: &["id", "tier"],
            expected_blocked: "",
            expected_fix_flag: None,
            expected_warnings: 0,
        },
        PlanSchemaTestCase {
            description: "a Postgres table SQLLoad did not create gets no column, loads narrower integers, shorter varchars and plain numeric (which refuse a value that does not fit), and warns about the source column it lacks",
            desired: vec![
                source("id", ColumnType::Int),
                source("code", ColumnType::Text),
                source("amount", DECIMAL),
                source("extra", ColumnType::Date),
            ],
            live: Some(vec![
                live("id", ColumnType::SmallInt),
                live("code", ColumnType::Varchar(5)),
                live("amount", ColumnType::Native("numeric".to_owned())),
                live("note", ColumnType::Text),
            ]),
            rules: postgres_rules(),
            expected_action: &["unchanged"],
            expected_written: &["id", "code", "amount"],
            expected_blocked: "",
            expected_fix_flag: None,
            expected_warnings: 1,
        },
        PlanSchemaTestCase {
            description: "a Postgres table SQLLoad did not create refuses a column that would round values, fewer digits after the point",
            desired: vec![source("amount", DECIMAL)],
            live: Some(vec![live(
                "amount",
                ColumnType::Decimal {
                    precision: 18,
                    scale: 1,
                },
            )]),
            rules: postgres_rules(),
            expected_action: &["unchanged"],
            expected_written: &[],
            expected_blocked: "amount is decimal(18,1) in the table, which SQLLoad did not create and does not alter, and decimal(18,2) values would not all load exactly there",
            expected_fix_flag: None,
            expected_warnings: 0,
        },
        PlanSchemaTestCase {
            description: "a Postgres table SQLLoad did not create refuses a decimal into an integer column, which would round it",
            desired: vec![source("amount", DECIMAL)],
            live: Some(vec![live("amount", ColumnType::Int)]),
            rules: postgres_rules(),
            expected_action: &["unchanged"],
            expected_written: &[],
            expected_blocked: "amount is int in the table",
            expected_fix_flag: None,
            expected_warnings: 0,
        },
        PlanSchemaTestCase {
            description: "a Postgres table SQLLoad did not create refuses a column whose name matches only when case is ignored",
            desired: vec![source("OrderId", ColumnType::Int)],
            live: Some(vec![live("orderid", ColumnType::Int)]),
            rules: postgres_rules(),
            expected_action: &["unchanged"],
            expected_written: &[],
            expected_blocked: "OrderId matches the table's column orderid only when case is ignored",
            expected_fix_flag: None,
            expected_warnings: 0,
        },
        PlanSchemaTestCase {
            description: "a Postgres table SQLLoad created (it has _sqlload_load_id) is widened as on any warehouse",
            desired: vec![source("amount", DECIMAL)],
            live: Some(vec![
                live("amount", ColumnType::Int),
                live("_sqlload_load_id", ColumnType::Text),
            ]),
            rules: postgres_rules(),
            expected_action: &["widen  amount int -> decimal(18,2) (numeric(21,2))  in place"],
            expected_written: &["amount"],
            expected_blocked: "",
            expected_fix_flag: None,
            expected_warnings: 0,
        },
    ];
    for test_case in test_cases {
        let plan = plan_schema(
            &test_case.desired,
            test_case.live.as_deref(),
            test_case.rules,
        );
        let written: Vec<&str> = plan
            .write_columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        let blocked = plan
            .blocked
            .as_ref()
            .map(|blocked| blocked.reasons.join("; "))
            .unwrap_or_default();
        assert_eq!(
            plan.action.summary(),
            test_case.expected_action,
            "{}",
            test_case.description
        );
        assert_eq!(
            written, test_case.expected_written,
            "{}",
            test_case.description
        );
        assert!(
            blocked.contains(test_case.expected_blocked)
                && blocked.is_empty() == test_case.expected_blocked.is_empty(),
            "{}: {blocked}",
            test_case.description
        );
        assert_eq!(
            plan.blocked.as_ref().and_then(|blocked| blocked.fix_flag),
            test_case.expected_fix_flag,
            "{}",
            test_case.description
        );
        assert_eq!(
            plan.warnings.len(),
            test_case.expected_warnings,
            "{}",
            test_case.description
        );
    }
}
