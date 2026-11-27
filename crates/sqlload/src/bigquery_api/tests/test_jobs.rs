use serde_json::json;

use crate::bigquery_api::_helpers::jobs::{JobState, column_names, job_state, page_rows};
use crate::bigquery_api::_helpers::requests::google_error;
use crate::bigquery_api::tests::test_types::{GoogleErrorTestCase, JobAnswerTestCase};

#[test]
fn given_a_query_answer_when_reading_it_then_running_paged_and_finished_jobs_are_told_apart() {
    let test_cases = [
        JobAnswerTestCase {
            description: "a job still running is polled by its id",
            response: json!({"jobComplete": false, "jobReference": {"jobId": "job_1"}}),
            expected_state: JobState::Running("job_1".to_owned()),
            expected_columns: &[],
            expected_rows: Vec::new(),
        },
        JobAnswerTestCase {
            description: "a finished page keeps NULLs, gives repeated values as JSON text, and names the next page",
            response: json!({
                "jobComplete": true,
                "jobReference": {"jobId": "job_2"},
                "pageToken": "next",
                "schema": {"fields": [{"name": "id"}, {"name": "tags"}]},
                "rows": [
                    {"f": [{"v": "1"}, {"v": null}]},
                    {"f": [{"v": "2"}, {"v": [{"v": "a"}]}]}
                ]
            }),
            expected_state: JobState::Done(Some("next".to_owned())),
            expected_columns: &["id", "tags"],
            expected_rows: vec![
                vec![Some("1"), None],
                vec![Some("2"), Some("[{\"v\":\"a\"}]")],
            ],
        },
        JobAnswerTestCase {
            description: "a statement with no rows finishes with none",
            response: json!({"jobComplete": true, "jobReference": {"jobId": "job_3"}}),
            expected_state: JobState::Done(None),
            expected_columns: &[],
            expected_rows: Vec::new(),
        },
    ];
    for test_case in test_cases {
        let expected_rows: Vec<Vec<Option<String>>> = test_case
            .expected_rows
            .iter()
            .map(|row| row.iter().map(|value| value.map(str::to_owned)).collect())
            .collect();
        assert_eq!(
            (
                job_state(&test_case.response),
                column_names(&test_case.response),
                page_rows(&test_case.response)
            ),
            (
                test_case.expected_state,
                test_case
                    .expected_columns
                    .iter()
                    .map(|name| (*name).to_owned())
                    .collect::<Vec<String>>(),
                expected_rows
            ),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_google_error_body_when_reading_it_then_its_reason_and_message_are_kept() {
    let test_cases = [
        GoogleErrorTestCase {
            description: "a REST error gives its first reason",
            body: r#"{"error": {"code": 404, "message": "Not found: Table p:d.t", "errors": [{"reason": "notFound"}], "status": "NOT_FOUND"}}"#,
            expected_reason: "notFound",
            expected_message: "Not found: Table p:d.t",
        },
        GoogleErrorTestCase {
            description: "an OAuth error gives its code and description",
            body: r#"{"error": "invalid_grant", "error_description": "Invalid JWT Signature."}"#,
            expected_reason: "invalid_grant",
            expected_message: "Invalid JWT Signature.",
        },
        GoogleErrorTestCase {
            description: "a body that is not JSON is kept as text",
            body: "upstream connect error",
            expected_reason: "",
            expected_message: "upstream connect error",
        },
    ];
    for test_case in test_cases {
        let (code, message) = google_error(test_case.body);
        assert_eq!(
            (code.as_str(), message.as_str()),
            (test_case.expected_reason, test_case.expected_message),
            "{}",
            test_case.description
        );
    }
}
