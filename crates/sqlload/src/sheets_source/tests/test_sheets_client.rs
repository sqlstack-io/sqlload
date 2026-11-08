use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use rsa::pkcs8::{EncodePrivateKey, LineEnding};
use serde_json::json;

use crate::auth::models::Secret;
use crate::http::main::http_client::build_http_client;
use crate::http::models::ConnectionCounter;
use crate::sheets_source::main::sheets_client::sheets_client;
use crate::sheets_source::models::KeySource;
use crate::sheets_source::tests::test_types::KeyTestCase;

#[test]
fn given_a_connection_key_when_making_a_client_then_it_reads_as_the_keys_service_account()
-> Result<(), String> {
    let private = rsa::RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048)
        .map_err(|error| error.to_string())?;
    let pem = private
        .to_pkcs8_pem(LineEnding::LF)
        .map_err(|error| error.to_string())?;
    let key = json!({"type": "service_account", "client_email": "reader@example.iam.gserviceaccount.com", "private_key": pem.as_str(), "token_uri": "https://oauth2.example.com/token", "project_id": "demo"}).to_string();
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let file = directory.path().join("key.json");
    std::fs::write(&file, &key).map_err(|error| error.to_string())?;
    let test_cases = [
        KeyTestCase {
            description: "the key's JSON text",
            source: KeySource::Json(Secret(key.clone())),
            expected_result: Ok("reader@example.iam.gserviceaccount.com"),
        },
        KeyTestCase {
            description: "the key's JSON text in base64, as secret stores without newlines keep it",
            source: KeySource::Base64(Secret(STANDARD.encode(&key))),
            expected_result: Ok("reader@example.iam.gserviceaccount.com"),
        },
        KeyTestCase {
            description: "a key file",
            source: KeySource::File(file.clone()),
            expected_result: Ok("reader@example.iam.gserviceaccount.com"),
        },
        KeyTestCase {
            description: "base64 that is not is refused by setting",
            source: KeySource::Base64(Secret("not base64!".to_owned())),
            expected_result: Err(
                "service_account_json_base64 is not base64: Invalid symbol 32, offset 3.",
            ),
        },
        KeyTestCase {
            description: "a user's OAuth credentials are refused, naming their kind",
            source: KeySource::Json(Secret(
                json!({"type": "authorized_user", "client_id": "x"}).to_string(),
            )),
            expected_result: Err(
                "the key is a \"authorized_user\" credential; Google Sheets sources read with a service-account key",
            ),
        },
        KeyTestCase {
            description: "a missing key file is named",
            source: KeySource::File(directory.path().join("missing.json")),
            expected_result: Err("cannot read the service-account key file"),
        },
    ];
    let http =
        build_http_client(&ConnectionCounter::default()).map_err(|error| error.to_string())?;
    for test_case in test_cases {
        let width = test_case.expected_result.err().map_or(0, str::len);
        let seen = sheets_client(http.clone(), &test_case.source, None)
            .map(|client| client.account().to_owned())
            .map_err(|error| error.to_string().chars().take(width).collect::<String>());
        assert_eq!(
            seen,
            test_case
                .expected_result
                .map(str::to_owned)
                .map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
    Ok(())
}
