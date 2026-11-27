use serde_json::{Value, json};

use crate::auth::models::Secret;
use crate::google_identity::_helpers::federation::aws_subject_token;
use crate::google_identity::models::AwsKeys;
use crate::google_identity::tests::test_types::AwsSubjectTestCase;

const AUDIENCE: &str =
    "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/pool/providers/aws";

#[test]
fn given_aws_keys_when_making_googles_subject_token_then_it_is_get_caller_identity_signed_as_sigv4_defines()
-> Result<(), String> {
    // Signatures computed apart from SQLLoad, from AWS's SigV4 definition, for 2023-11-14T22:13:20Z.
    let test_cases = [
        AwsSubjectTestCase {
            description: "temporary keys sign their session token too",
            session_token: Some("session-token"),
            expected_headers: &[
                (
                    "Authorization",
                    "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20231114/us-east-1/sts/aws4_request, SignedHeaders=host;x-amz-date;x-amz-security-token;x-goog-cloud-target-resource, Signature=b9b1178bb0161a0efc04eb9d404327bf66f4aa48ac709f1755e0eaa32c587c7c",
                ),
                ("host", "sts.us-east-1.amazonaws.com"),
                ("x-amz-date", "20231114T221320Z"),
                ("x-amz-security-token", "session-token"),
                ("x-goog-cloud-target-resource", AUDIENCE),
            ],
        },
        AwsSubjectTestCase {
            description: "long-lived keys sign without one",
            session_token: None,
            expected_headers: &[
                (
                    "Authorization",
                    "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20231114/us-east-1/sts/aws4_request, SignedHeaders=host;x-amz-date;x-goog-cloud-target-resource, Signature=f5a2e5dd64be1292579d152cec183d6a0072e744043718649d3daf067b2a9fb5",
                ),
                ("host", "sts.us-east-1.amazonaws.com"),
                ("x-amz-date", "20231114T221320Z"),
                ("x-goog-cloud-target-resource", AUDIENCE),
            ],
        },
    ];
    for test_case in &test_cases {
        let keys = AwsKeys {
            access_key_id: "AKIDEXAMPLE".to_owned(),
            secret_access_key: Secret("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_owned()),
            session_token: test_case
                .session_token
                .map(|token| Secret(token.to_owned())),
        };
        let token = aws_subject_token(
            &keys,
            (
                "us-east-1",
                "https://sts.{region}.amazonaws.com?Action=GetCallerIdentity&Version=2011-06-15",
            ),
            AUDIENCE,
            1_700_000_000_000,
        )
        .map_err(|error| error.to_string())?;
        let wrapped = reqwest::Url::parse(&format!("http://decode.invalid/?t={}", token.expose()))
            .map_err(|error| error.to_string())?;
        let decoded = wrapped
            .query_pairs()
            .next()
            .map(|(_, value)| value.into_owned())
            .unwrap_or_default();
        let request: Value = serde_json::from_str(&decoded).map_err(|error| error.to_string())?;
        let expected: Vec<Value> = test_case
            .expected_headers
            .iter()
            .map(|(key, value)| json!({ "key": key, "value": value }))
            .collect();
        assert_eq!(
            (&request["method"], &request["url"], &request["headers"]),
            (
                &json!("POST"),
                &json!(
                    "https://sts.us-east-1.amazonaws.com?Action=GetCallerIdentity&Version=2011-06-15"
                ),
                &json!(expected)
            ),
            "{}",
            test_case.description
        );
    }
    Ok(())
}
