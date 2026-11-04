use crate::column_types::models::ColumnType;
use crate::files_destination::main::fit_live_types::fit_live_types;
use crate::files_destination::tests::test_types::FitLiveTypesTestCase;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn, LiveColumn};

#[test]
fn given_a_source_type_the_files_type_holds_when_planning_then_new_files_keep_the_files_type() {
    let test_cases = [
        FitLiveTypesTestCase {
            description: "a smallint source is written as the files' int",
            desired: ColumnType::SmallInt,
            live: ColumnType::Int,
            expected_type: ColumnType::Int,
        },
        FitLiveTypesTestCase {
            description: "a narrower decimal is written as the files' wider one",
            desired: ColumnType::Decimal {
                precision: 10,
                scale: 2,
            },
            live: ColumnType::Decimal {
                precision: 12,
                scale: 2,
            },
            expected_type: ColumnType::Decimal {
                precision: 12,
                scale: 2,
            },
        },
        FitLiveTypesTestCase {
            description: "a uuid source is written as the files' text",
            desired: ColumnType::Uuid,
            live: ColumnType::Text,
            expected_type: ColumnType::Text,
        },
        FitLiveTypesTestCase {
            description: "a text source next to uuid files keeps the files' uuid marker",
            desired: ColumnType::Text,
            live: ColumnType::Uuid,
            expected_type: ColumnType::Uuid,
        },
        FitLiveTypesTestCase {
            description: "a type the files cannot hold is left for the plan to block",
            desired: ColumnType::Int,
            live: ColumnType::SmallInt,
            expected_type: ColumnType::Int,
        },
    ];
    for test_case in test_cases {
        let desired = vec![DesiredColumn {
            name: "qty".to_owned(),
            column_type: test_case.desired.clone(),
            origin: ColumnOrigin::Source,
        }];
        let live = [LiveColumn {
            name: "QTY".to_owned(),
            column_type: test_case.live.clone(),
        }];
        let fitted = fit_live_types(desired, &live);
        assert_eq!(
            fitted.first().map(|column| &column.column_type),
            Some(&test_case.expected_type),
            "{}",
            test_case.description
        );
    }
}
