use std::collections::BTreeMap;

use crate::column_types::models::ColumnType;
use crate::contract::_helpers::compare::findings;
use crate::contract::tests::test_types::FindingsTestCase;
use crate::project::models::Contract;
use crate::schema_plan::models::LiveColumn;

#[test]
fn given_declared_and_live_columns_when_compared_then_each_difference_is_one_finding() {
    let test_cases = [
        FindingsTestCase {
            description: "matching names in another case and equal types agree, and SQLLoad's own columns never count",
            declared: &[
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
            live: Some(&[
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
            declared: &[
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
            live: Some(&[
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
            declared: &[("id", ColumnType::Int)],
            contract: Contract::Enforced,
            live: Some(&[("id", ColumnType::Int), ("extra", ColumnType::Bool)]),
            expected_messages: &[
                "missing_declaration: physical column 'extra' (bool) is not declared",
            ],
        },
        FindingsTestCase {
            description: "a table that does not exist is one finding",
            declared: &[("id", ColumnType::Int)],
            contract: Contract::None,
            live: None,
            expected_messages: &["relation_missing: physical relation RAW.orders does not exist"],
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
        let messages: Vec<String> =
            findings(&declared, test_case.contract, live.as_deref(), "RAW.orders")
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
