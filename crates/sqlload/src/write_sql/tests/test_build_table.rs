use crate::column_types::models::{ColumnType, Warehouse};
use crate::schema_plan::models::{DesiredColumn, LiveColumn};
use crate::write_sql::_helpers::promote::promote_statements;
use crate::write_sql::main::build_table::build_table;
use crate::write_sql::main::staging_changes::staging_changes;
use crate::write_sql::tests::helpers::{accounts, columns};
use crate::write_sql::tests::test_types::{BuildTableTestCase, PromoteTestCase};

const SF_BUILD: &str = "\"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1\"";
const DUCK_BUILD: &str = "\"raw\".\"app__accounts__sqlload_staging_load1\"";

#[test]
fn given_a_replace_when_building_the_table_then_it_has_the_tables_own_types_and_grants() {
    let live = |name: &str, column_type: ColumnType| LiveColumn {
        name: name.to_owned(),
        column_type,
    };
    let same = vec![
        live("ID", ColumnType::Int),
        live("PROFILE", ColumnType::Json),
        live("UPDATED_AT", ColumnType::TimestampTz),
        live("_SQLLOAD_LOAD_ID", ColumnType::Text),
        live("_SQLLOAD_LOADED_AT", ColumnType::TimestampTz),
    ];
    let mut changed = same.clone();
    changed[0].column_type = ColumnType::Text;
    changed.remove(2);
    changed.push(live("GONE", ColumnType::Text));
    let like =
        format!("CREATE OR REPLACE TABLE {SF_BUILD} LIKE \"RAW\".\"APP__ACCOUNTS\" COPY GRANTS");
    let test_cases = [
        BuildTableTestCase {
            description: "Snowflake starts like the table, keeping its grants",
            warehouse: Warehouse::Snowflake,
            live: None,
            definition: None,
            expected_statements: vec![like.clone()],
        },
        BuildTableTestCase {
            description: "a reload with the same columns changes nothing",
            warehouse: Warehouse::Snowflake,
            live: Some(same),
            definition: None,
            expected_statements: vec![like.clone()],
        },
        BuildTableTestCase {
            description: "a reload drops and re-adds what differs while the table is empty",
            warehouse: Warehouse::Snowflake,
            live: Some(changed),
            definition: None,
            expected_statements: vec![
                like,
                format!("ALTER TABLE {SF_BUILD} DROP COLUMN \"ID\""),
                format!("ALTER TABLE {SF_BUILD} DROP COLUMN \"GONE\""),
                format!("ALTER TABLE {SF_BUILD} ADD COLUMN \"ID\" NUMBER(38,0)"),
                format!("ALTER TABLE {SF_BUILD} ADD COLUMN \"UPDATED_AT\" TIMESTAMP_TZ(6)"),
            ],
        },
        BuildTableTestCase {
            description: "DuckDB starts from the table's definition, keeping what the destination added",
            warehouse: Warehouse::DuckDb,
            live: None,
            definition: Some("(id BIGINT PRIMARY KEY, note VARCHAR NOT NULL)"),
            expected_statements: vec![format!(
                "CREATE OR REPLACE TABLE {DUCK_BUILD}(id BIGINT PRIMARY KEY, note VARCHAR NOT NULL)"
            )],
        },
        BuildTableTestCase {
            description: "a DuckDB reload creates the current columns' final types",
            warehouse: Warehouse::DuckDb,
            live: Some(Vec::new()),
            definition: None,
            expected_statements: vec![format!(
                "CREATE OR REPLACE TABLE {DUCK_BUILD} (\"id\" BIGINT, \"profile\" JSON, \"updated_at\" TIMESTAMP WITH TIME ZONE, \"_sqlload_load_id\" VARCHAR, \"_sqlload_loaded_at\" TIMESTAMP WITH TIME ZONE)"
            )],
        },
    ];
    for test_case in test_cases {
        let (_, statements) = build_table(
            &accounts(),
            (&columns(), test_case.live.as_deref(), test_case.definition),
            "load-1",
            test_case.warehouse,
        );
        assert_eq!(
            statements, test_case.expected_statements,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_finished_build_table_when_promoting_then_no_row_is_copied() {
    let test_cases = [
        PromoteTestCase {
            description: "Snowflake swaps",
            warehouse: Warehouse::Snowflake,
            expected_statements: vec![
                format!("ALTER TABLE \"RAW\".\"APP__ACCOUNTS\" SWAP WITH {SF_BUILD}"),
                format!("DROP TABLE IF EXISTS {SF_BUILD}"),
            ],
        },
        PromoteTestCase {
            description: "DuckDB drops, renames and re-creates the indexes in one transaction",
            warehouse: Warehouse::DuckDb,
            expected_statements: vec![
                "BEGIN".to_owned(),
                "DROP TABLE IF EXISTS \"raw\".\"app__accounts\"".to_owned(),
                format!("ALTER TABLE {DUCK_BUILD} RENAME TO \"app__accounts\""),
                "CREATE INDEX ix ON raw.app__accounts(id)".to_owned(),
                "COMMIT".to_owned(),
            ],
        },
    ];
    for test_case in test_cases {
        let warehouse = test_case.warehouse;
        let (build, _) = build_table(&accounts(), (&columns(), None, None), "load-1", warehouse);
        assert_eq!(
            promote_statements(
                &accounts(),
                &build,
                &["CREATE INDEX ix ON raw.app__accounts(id)".to_owned()],
                warehouse
            ),
            test_case.expected_statements,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_build_table_when_a_column_arrives_mid_load_then_it_takes_the_final_type() {
    let test_cases = [
        PromoteTestCase {
            description: "DuckDB adds JSON as JSON, not the staging table's text",
            warehouse: Warehouse::DuckDb,
            expected_statements: vec![format!(
                "ALTER TABLE {DUCK_BUILD} ADD COLUMN \"profile\" JSON"
            )],
        },
        PromoteTestCase {
            description: "Snowflake adds JSON as VARIANT",
            warehouse: Warehouse::Snowflake,
            expected_statements: vec![format!(
                "ALTER TABLE {SF_BUILD} ADD COLUMN \"PROFILE\" VARIANT"
            )],
        },
    ];
    for test_case in test_cases {
        let warehouse = test_case.warehouse;
        let (build, _) = build_table(&accounts(), (&columns(), None, None), "load-1", warehouse);
        let new = columns();
        let old: Vec<DesiredColumn> = new
            .iter()
            .filter(|column| column.name != "profile")
            .cloned()
            .collect();
        assert_eq!(
            staging_changes(&build, (&old, &new), warehouse, true),
            test_case.expected_statements,
            "{}",
            test_case.description
        );
    }
}
