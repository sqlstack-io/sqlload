use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rsa::pkcs8::{EncodePrivateKey, LineEnding};
use serde_json::json;

use crate::bigquery_api::_helpers::jobs::{JobState, column_names, job_state, page_rows};
use crate::bigquery_api::_helpers::requests::google_error;
use crate::bigquery_api::_helpers::token::signed_assertion;
use crate::bigquery_api::constants::SCOPE;
use crate::bigquery_api::main::parse_key::parse_key;
use crate::bigquery_api::tests::test_types::{
    GoogleErrorTestCase, JobAnswerTestCase, KeyFileTestCase,
};

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

#[test]
fn given_a_service_account_key_file_when_signing_then_the_assertion_asks_for_bigquery_for_an_hour()
-> Result<(), String> {
    let key = rsa::RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048)
        .map_err(|error| error.to_string())?;
    let pem = key
        .to_pkcs8_pem(LineEnding::LF)
        .map_err(|error| error.to_string())?;
    let test_cases = [
        KeyFileTestCase {
            description: "a complete key file signs for its account, audience and BigQuery's scope",
            fields: json!({"client_email": "loader@example.iam.gserviceaccount.com", "token_uri": "https://oauth2.example.com/token", "project_id": "demo"}),
            expected_claims: Ok(
                json!({"iss": "loader@example.iam.gserviceaccount.com", "scope": "https://www.googleapis.com/auth/bigquery", "aud": "https://oauth2.example.com/token", "iat": 1000, "exp": 4600}),
            ),
        },
        KeyFileTestCase {
            description: "a key file without its account is refused by name",
            fields: json!({"token_uri": "https://oauth2.example.com/token", "project_id": "demo"}),
            expected_claims: Err("the service-account key has no client_email"),
        },
    ];
    for test_case in test_cases {
        let mut file = test_case.fields.clone();
        file["private_key"] = json!(pem.as_str());
        let claims = parse_key(&file.to_string())
            .and_then(|key| signed_assertion(&key, SCOPE, 1000))
            .map_err(|error| error.to_string())
            .and_then(|assertion| {
                let payload = assertion.split('.').nth(1).unwrap_or_default();
                let bytes = URL_SAFE_NO_PAD
                    .decode(payload)
                    .map_err(|error| error.to_string())?;
                serde_json::from_slice::<serde_json::Value>(&bytes)
                    .map_err(|error| error.to_string())
            });
        assert_eq!(
            claims,
            test_case.expected_claims.clone().map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
    Ok(())
}
