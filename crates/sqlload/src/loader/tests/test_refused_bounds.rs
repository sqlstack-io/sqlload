use arrow_schema::{DataType, TimeUnit};

use crate::column_types::models::ColumnType;
use crate::loader::tests::helpers::refused_bounds_runs;
use crate::loader::tests::test_types::RefusedBoundsTestCase;

const PARAMETER_REFUSED: Result<(), &str> = Err("[HY000] Invalid parameter type");

const ESCAPE_REFUSED: Result<(), &str> = Err("[42000] Syntax error at {ts");

const TIMED_OUT: Result<(), &str> = Err("[HYT00] Timeout expired");

#[test]
fn given_an_odbc_cursor_table_when_planning_then_a_driver_refusing_its_bounds_fails_the_plan_naming_the_cursor_bounds_that_runs()
-> Result<(), String> {
    let millisecond = DataType::Timestamp(TimeUnit::Millisecond, None);
    let test_cases = [
        RefusedBoundsTestCase {
            description: "a driver refusing timestamp parameters, which takes {ts} escapes, gets the setting that writes them",
            source_settings: "",
            cursor_type: millisecond.clone(),
            column_type: ColumnType::TimestampNtz,
            from: None,
            responses: &[PARAMETER_REFUSED, Ok(()), PARAMETER_REFUSED],
            expected_problem: Some(
                "the ODBC driver refuses the cursor updated's bounds as parameters ([HY000] Invalid parameter type) but takes them as ODBC escapes; set cursor_bounds = \"literals\" on source erp or its table entry",
            ),
            expected_runs: &[
                "\"updated\" > ? AND \"updated\" < ? AND 1 = 0 [Timestamp(2000-01-01T00:00:00, Some(3)), Timestamp(2000-01-01T00:00:00, Some(3))]",
                "\"updated\" > {ts '2000-01-01 00:00:00.000'} AND \"updated\" < {ts '2000-01-01 00:00:00.000'} AND 1 = 0 []",
                "\"updated\" > ? AND \"updated\" < ? AND 1 = 0 [Timestamp(2000-01-01T00:00:00, Some(3)), Timestamp(2000-01-01T00:00:00, Some(3))]",
            ],
        },
        RefusedBoundsTestCase {
            description: "with cursor_bounds = \"literals\" the same driver plans, running the query once",
            source_settings: "cursor_bounds = \"literals\"",
            cursor_type: millisecond.clone(),
            column_type: ColumnType::TimestampNtz,
            from: None,
            responses: &[],
            expected_problem: None,
            expected_runs: &[
                "\"updated\" > {ts '2000-01-01 00:00:00.000'} AND \"updated\" < {ts '2000-01-01 00:00:00.000'} AND 1 = 0 []",
            ],
        },
        RefusedBoundsTestCase {
            description: "a driver taking parameters but not {ts} escapes, under literals, is told to drop the setting",
            source_settings: "cursor_bounds = \"literals\"",
            cursor_type: millisecond.clone(),
            column_type: ColumnType::TimestampNtz,
            from: None,
            responses: &[ESCAPE_REFUSED, Ok(()), ESCAPE_REFUSED],
            expected_problem: Some(
                "the ODBC driver refuses the cursor updated's bounds as ODBC escapes ([42000] Syntax error at {ts) but takes them as parameters; remove cursor_bounds = \"literals\" from source erp and its table entry",
            ),
            expected_runs: &[
                "\"updated\" > {ts '2000-01-01 00:00:00.000'} AND \"updated\" < {ts '2000-01-01 00:00:00.000'} AND 1 = 0 []",
                "\"updated\" > ? AND \"updated\" < ? AND 1 = 0 [Timestamp(2000-01-01T00:00:00, Some(3)), Timestamp(2000-01-01T00:00:00, Some(3))]",
                "\"updated\" > {ts '2000-01-01 00:00:00.000'} AND \"updated\" < {ts '2000-01-01 00:00:00.000'} AND 1 = 0 []",
            ],
        },
        RefusedBoundsTestCase {
            description: "a query failing once for another reason, which runs the other way and then its own, gives no advice and plans",
            source_settings: "",
            cursor_type: millisecond.clone(),
            column_type: ColumnType::TimestampNtz,
            from: None,
            responses: &[TIMED_OUT],
            expected_problem: None,
            expected_runs: &[
                "\"updated\" > ? AND \"updated\" < ? AND 1 = 0 [Timestamp(2000-01-01T00:00:00, Some(3)), Timestamp(2000-01-01T00:00:00, Some(3))]",
                "\"updated\" > {ts '2000-01-01 00:00:00.000'} AND \"updated\" < {ts '2000-01-01 00:00:00.000'} AND 1 = 0 []",
                "\"updated\" > ? AND \"updated\" < ? AND 1 = 0 [Timestamp(2000-01-01T00:00:00, Some(3)), Timestamp(2000-01-01T00:00:00, Some(3))]",
            ],
        },
        RefusedBoundsTestCase {
            description: "a driver refusing both shows both errors",
            source_settings: "",
            cursor_type: millisecond.clone(),
            column_type: ColumnType::TimestampNtz,
            from: None,
            responses: &[PARAMETER_REFUSED, ESCAPE_REFUSED],
            expected_problem: Some(
                "the cursor updated's query fails with its bounds as parameters ([HY000] Invalid parameter type) and as ODBC escapes ([42000] Syntax error at {ts)",
            ),
            expected_runs: &[
                "\"updated\" > ? AND \"updated\" < ? AND 1 = 0 [Timestamp(2000-01-01T00:00:00, Some(3)), Timestamp(2000-01-01T00:00:00, Some(3))]",
                "\"updated\" > {ts '2000-01-01 00:00:00.000'} AND \"updated\" < {ts '2000-01-01 00:00:00.000'} AND 1 = 0 []",
            ],
        },
        RefusedBoundsTestCase {
            description: "a table resuming from its millisecond maximum checks with that bound, at the column's three digits",
            source_settings: "",
            cursor_type: millisecond,
            column_type: ColumnType::TimestampNtz,
            from: Some("2026-01-31 23:59:59.997"),
            responses: &[],
            expected_problem: None,
            expected_runs: &[
                "\"updated\" > ? AND \"updated\" < ? AND 1 = 0 [Timestamp(2026-01-31T23:59:59.997, Some(3)), Timestamp(2026-01-31T23:59:59.997, Some(3))]",
            ],
        },
        RefusedBoundsTestCase {
            description: "a cursor the driver describes in whole seconds (MySQL's DATETIME(3)) binds with every digit, as before",
            source_settings: "",
            cursor_type: DataType::Timestamp(TimeUnit::Second, None),
            column_type: ColumnType::TimestampNtz,
            from: Some("2026-01-31 23:59:59.997"),
            responses: &[],
            expected_problem: None,
            expected_runs: &[
                "\"updated\" > ? AND \"updated\" < ? AND 1 = 0 [Timestamp(2026-01-31T23:59:59.997, None), Timestamp(2026-01-31T23:59:59.997, None)]",
            ],
        },
        RefusedBoundsTestCase {
            description: "a numeric cursor is a bare number either way, so nothing runs",
            source_settings: "",
            cursor_type: DataType::Int64,
            column_type: ColumnType::Int,
            from: Some("41"),
            responses: &[],
            expected_problem: None,
            expected_runs: &[],
        },
    ];
    for test_case in test_cases {
        let (problem, runs) = refused_bounds_runs(&test_case)?;
        assert_eq!(
            (problem.as_deref(), runs),
            (
                test_case.expected_problem,
                test_case
                    .expected_runs
                    .iter()
                    .map(|run| (*run).to_owned())
                    .collect::<Vec<String>>()
            ),
            "{}",
            test_case.description
        );
    }
    Ok(())
}
