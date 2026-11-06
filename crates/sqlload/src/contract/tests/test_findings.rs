use std::collections::BTreeMap;

use crate::column_types::models::{ColumnType, Warehouse};
use crate::contract::_helpers::compare::findings;
use crate::contract::tests::test_types::FindingsTestCase;
use crate::project::models::Contract;
use crate::schema_plan::models::LiveColumn;

#[test]
fn given_declared_and_live_columns_when_compared_then_each_difference_is_one_finding() {
    let test_cases = [
        FindingsTestCase {
            description: "matching names in another case and equal types agree, and SQLLoad's own columns never count",
            declared: vec![
                ("ID", ColumnType::Int),
                (
                    "amount",
                    ColumnType::Decimal {
                        precision: 12,
                        scale: 2,
                    },
                ),
            ],
            contract: Contract::Enforced,
            warehouse: Warehouse::DuckDb,
            live: Some(vec![
                ("id", ColumnType::Int),
                (
                    "AMOUNT",
                    ColumnType::Decimal {
                        precision: 12,
                        scale: 2,
                    },
                ),
                ("_sqlload_load_id", ColumnType::Text),
            ]),
            expected_messages: &[],
        },
        FindingsTestCase {
            description: "a decimal's scale differing is a mismatch; a declared column the table lacks comes after the live ones",
            declared: vec![
                (
                    "amount",
                    ColumnType::Decimal {
                        precision: 12,
                        scale: 2,
                    },
                ),
                ("gone", ColumnType::Text),
            ],
            contract: Contract::None,
            warehouse: Warehouse::DuckDb,
            live: Some(vec![
                (
                    "amount",
                    ColumnType::Decimal {
                        precision: 12,
                        scale: 4,
                    },
                ),
                ("extra", ColumnType::Bool),
            ]),
            expected_messages: &[
                "type_mismatch: column 'amount' is declared decimal(12,2) but physical type is decimal(12,4)",
                "missing_physical_column: declared column 'gone' is absent from the physical relation",
            ],
        },
        FindingsTestCase {
            description: "undeclared live columns count only under contract = \"enforced\"",
            declared: vec![("id", ColumnType::Int)],
            contract: Contract::Enforced,
            warehouse: Warehouse::DuckDb,
            live: Some(vec![("id", ColumnType::Int), ("extra", ColumnType::Bool)]),
            expected_messages: &[
                "missing_declaration: physical column 'extra' (bool) is not declared",
            ],
        },
        FindingsTestCase {
            description: "a table that does not exist is one finding",
            declared: vec![("id", ColumnType::Int)],
            contract: Contract::None,
            warehouse: Warehouse::DuckDb,
            live: None,
            expected_messages: &["relation_missing: physical relation RAW.orders does not exist"],
        },
        FindingsTestCase {
            description: "on DuckDB a declared varchar(n) is stored as VARCHAR and read back as text, and a native type matches in any case",
            declared: vec![
                ("note", ColumnType::Varchar(50)),
                ("big", ColumnType::Native("hugeint".to_owned())),
            ],
            contract: Contract::None,
            warehouse: Warehouse::DuckDb,
            live: Some(vec![
                ("note", ColumnType::Text),
                ("big", ColumnType::Native("HUGEINT".to_owned())),
            ]),
            expected_messages: &[],
        },
        FindingsTestCase {
            description: "on Snowflake smallint, uuid, decimal(38,0) and native NUMBER read back as their stored types and agree",
            declared: vec![
                ("a", ColumnType::SmallInt),
                ("b", ColumnType::Uuid),
                (
                    "c",
                    ColumnType::Decimal {
                        precision: 38,
                        scale: 0,
                    },
                ),
                ("d", ColumnType::Native("NUMBER(10,0)".to_owned())),
            ],
            contract: Contract::None,
            warehouse: Warehouse::Snowflake,
            live: Some(vec![
                ("a", ColumnType::Int),
                ("b", ColumnType::Varchar(36)),
                ("c", ColumnType::Int),
                (
                    "d",
                    ColumnType::Decimal {
                        precision: 10,
                        scale: 0,
                    },
                ),
            ]),
            expected_messages: &[],
        },
        FindingsTestCase {
            description: "a live type the warehouse stores differently is a mismatch, wider or not",
            declared: vec![("a", ColumnType::Varchar(50)), ("b", ColumnType::Text)],
            contract: Contract::None,
            warehouse: Warehouse::Snowflake,
            live: Some(vec![
                ("a", ColumnType::Varchar(100)),
                ("b", ColumnType::Varchar(50)),
            ]),
            expected_messages: &[
                "type_mismatch: column 'a' is declared varchar(50) but physical type is varchar(100)",
                "type_mismatch: column 'b' is declared text but physical type is varchar(50)",
            ],
        },
        FindingsTestCase {
            description: "on DuckDB a declared smallint in a BIGINT column is a mismatch",
            declared: vec![("a", ColumnType::SmallInt)],
            contract: Contract::None,
            warehouse: Warehouse::DuckDb,
            live: Some(vec![("a", ColumnType::Int)]),
            expected_messages: &[
                "type_mismatch: column 'a' is declared smallint but physical type is int",
            ],
        },
    ];
    for test_case in test_cases {
        let declared: BTreeMap<String, ColumnType> = test_case
            .declared
            .iter()
            .map(|(name, column_type)| ((*name).to_owned(), column_type.clone()))
            .collect();
        let live: Option<Vec<LiveColumn>> = test_case.live.map(|columns| {
            columns
                .iter()
                .map(|(name, column_type)| LiveColumn {
                    name: (*name).to_owned(),
                    column_type: column_type.clone(),
                    fraction_digits: None,
                })
                .collect()
        });
        let messages: Vec<String> = findings(
            &declared,
            test_case.contract,
            live.as_deref(),
            (test_case.warehouse, "RAW.orders"),
        )
        .iter()
        .map(|finding| format!("{}: {}", finding.kind, finding.message))
        .collect();
        assert_eq!(
            messages, test_case.expected_messages,
            "{}",
            test_case.description
        );
    }
}
