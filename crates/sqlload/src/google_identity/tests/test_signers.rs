use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rsa::pkcs8::{EncodePrivateKey, LineEnding};
use serde_json::json;

use crate::bigquery_api::constants::SCOPE;
use crate::google_identity::_helpers::files::key_fields;
use crate::google_identity::_helpers::signers::signed_assertion;
use crate::google_identity::tests::test_types::KeyFileTestCase;

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
        let claims = key_fields(&file, "the service-account key")
            .and_then(|key| signed_assertion(&key, SCOPE, 1000).map_err(|error| error.to_string()))
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
