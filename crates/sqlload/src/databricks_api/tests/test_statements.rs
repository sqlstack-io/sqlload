use serde_json::json;

use crate::databricks_api::_helpers::statements::{StatementState, chunk_rows, statement_state};
use crate::databricks_api::tests::test_types::StatementResponseTestCase;

#[test]
fn given_a_statement_response_when_reading_it_then_running_failed_and_finished_are_told_apart() {
    let test_cases = [
        StatementResponseTestCase {
            description: "a pending statement is polled by its id",
            response: json!({"statement_id": "01ab", "status": {"state": "PENDING"}}),
            expected_state: Ok(Some("01ab")),
            expected_rows: Vec::new(),
        },
        StatementResponseTestCase {
            description: "a failed statement carries Databricks' error class and message",
            response: json!({"status": {"state": "FAILED", "error": {"error_code": "TABLE_OR_VIEW_NOT_FOUND", "message": "no such table"}}}),
            expected_state: Err("HTTP 400 TABLE_OR_VIEW_NOT_FOUND: no such table"),
            expected_rows: Vec::new(),
        },
        StatementResponseTestCase {
            description: "a finished statement's rows keep their NULLs",
            response: json!({
                "status": {"state": "SUCCEEDED"},
                "result": {"data_array": [["1", null], ["2", "b"]]}
            }),
            expected_state: Ok(None),
            expected_rows: vec![vec![Some("1"), None], vec![Some("2"), Some("b")]],
        },
    ];
    for test_case in test_cases {
        let state = statement_state(&test_case.response)
            .map(|state| Some(state).filter(|state| *state != StatementState::Done))
            .map_err(|error| error.to_string());
        let rows: Vec<Vec<Option<String>>> = chunk_rows(&test_case.response["result"]);
        let expected_rows: Vec<Vec<Option<String>>> = test_case
            .expected_rows
            .iter()
            .map(|row| row.iter().map(|value| value.map(str::to_owned)).collect())
            .collect();
        assert_eq!(
            (state, rows),
            (
                test_case
                    .expected_state
                    .map(|id| id.map(|id| StatementState::Running(id.to_owned())))
                    .map_err(str::to_owned),
                expected_rows
            ),
            "{}",
            test_case.description
        );
    }
}
