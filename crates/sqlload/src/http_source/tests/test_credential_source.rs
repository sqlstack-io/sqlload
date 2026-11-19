use crate::http_source::main::http_credential_source::http_credential_source;
use crate::http_source::models::Auth;
use crate::http_source::tests::test_types::CredentialSourceTestCase;

#[test]
fn given_an_http_connections_auth_when_debugging_then_it_names_the_settings_the_credentials_come_from()
 {
    let test_cases = [
        CredentialSourceTestCase {
            description: "bearer with its token",
            auth: Some(r#"kind = "bearer""#),
            set: &["token", "username"],
            expected_source: Ok("auth bearer: token from the connection"),
        },
        CredentialSourceTestCase {
            description: "OAuth2 also names the token URL",
            auth: Some(
                r#"kind = "oauth2_client_credentials"
token_url = "https://auth.example.com/token""#,
            ),
            set: &["client_id", "client_secret"],
            expected_source: Ok(
                "auth oauth2_client_credentials: client_id and client_secret from the connection; access tokens from https://auth.example.com/token",
            ),
        },
        CredentialSourceTestCase {
            description: "basic without a password names only what is missing",
            auth: Some(r#"kind = "basic""#),
            set: &["username", "token"],
            expected_source: Err("auth basic needs password on the connection"),
        },
        CredentialSourceTestCase {
            description: "no auth needs nothing",
            auth: None,
            set: &[],
            expected_source: Ok("no auth"),
        },
    ];
    for test_case in test_cases {
        let auth: Option<Auth> = test_case
            .auth
            .map(toml::from_str::<Auth>)
            .transpose()
            .unwrap_or_else(|error| panic!("{}: {error}", test_case.description));
        let set: Vec<(&str, bool)> = [
            "token",
            "username",
            "password",
            "client_id",
            "client_secret",
        ]
        .into_iter()
        .map(|key| (key, test_case.set.contains(&key)))
        .collect();
        assert_eq!(
            http_credential_source(auth.as_ref(), &set),
            test_case
                .expected_source
                .map(str::to_owned)
                .map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
}
