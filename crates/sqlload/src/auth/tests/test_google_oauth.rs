use crate::auth::tests::helpers::{block_on, user_token_outcome};
use crate::auth::tests::test_types::UserTokenTestCase;

const NO_TERMINAL: &str = "no usable Google sign-in is cached for the Google account for connection \"sheets\", and signing in in the browser needs a terminal; run `sqlload plan` from a terminal once to sign in, or set the connection's refresh_token for unattended runs";

#[test]
fn given_a_google_user_connection_when_a_token_is_needed_then_it_is_renewed_consented_or_refused_clearly()
-> Result<(), String> {
    let test_cases = [
        UserTokenTestCase {
            description: "the connection's refresh token renews an access token, which the next read reuses; it is not written to the cache",
            configured: Some("refresh-own"),
            cached: None,
            refused: None,
            revoked: &[],
            client_secret: "secret",
            terminal: false,
            expected_outcome: "access-1",
            expected_grants: &["refresh_token"],
            expected_cached_refresh: None,
            expected_retryable: false,
            expected_repeated: ("access-1", 1),
        },
        UserTokenTestCase {
            description: "a revoked refresh token in the connection fails, saying why and how to get a new one, without asking anyone to consent",
            configured: Some("refresh-own"),
            cached: None,
            refused: None,
            revoked: &["refresh-own"],
            client_secret: "secret",
            terminal: true,
            expected_outcome: "Google refused the refresh_token of connection \"sheets\" (invalid_grant: Token has been expired or revoked.): it was revoked, has expired",
            expected_grants: &["refresh_token"],
            expected_cached_refresh: None,
            expected_retryable: false,
            expected_repeated: ("Google refused the refresh_token", 2),
        },
        UserTokenTestCase {
            description: "with nothing cached and no terminal, the run fails at once, saying how to sign in",
            configured: None,
            cached: None,
            refused: None,
            revoked: &[],
            client_secret: "secret",
            terminal: false,
            expected_outcome: NO_TERMINAL,
            expected_grants: &[],
            expected_cached_refresh: None,
            expected_retryable: false,
            expected_repeated: (NO_TERMINAL, 0),
        },
        UserTokenTestCase {
            description: "with nothing cached and a terminal, the person consents in the browser for offline read-only access; the code is exchanged with its PKCE verifier and the refresh token cached",
            configured: None,
            cached: None,
            refused: None,
            revoked: &[],
            client_secret: "secret",
            terminal: true,
            expected_outcome: "access-1",
            expected_grants: &["authorization_code"],
            expected_cached_refresh: Some("refresh-1"),
            expected_retryable: false,
            expected_repeated: ("access-1", 1),
        },
        UserTokenTestCase {
            description: "a cached access token with time left is used as it is",
            configured: None,
            cached: Some(("access-cached", 600, "refresh-cached")),
            refused: None,
            revoked: &[],
            client_secret: "secret",
            terminal: false,
            expected_outcome: "access-cached",
            expected_grants: &[],
            expected_cached_refresh: Some("refresh-cached"),
            expected_retryable: false,
            expected_repeated: ("access-cached", 0),
        },
        UserTokenTestCase {
            description: "an expired cached access token is renewed with the cached refresh token, which is kept, as Google sends no new one",
            configured: None,
            cached: Some(("access-old", -10, "refresh-cached")),
            refused: None,
            revoked: &[],
            client_secret: "secret",
            terminal: false,
            expected_outcome: "access-1",
            expected_grants: &["refresh_token"],
            expected_cached_refresh: Some("refresh-cached"),
            expected_retryable: false,
            expected_repeated: ("access-1", 1),
        },
        UserTokenTestCase {
            description: "an access token the Sheets API refused is replaced even though it has time left",
            configured: None,
            cached: Some(("access-cached", 600, "refresh-cached")),
            refused: Some("access-cached"),
            revoked: &[],
            client_secret: "secret",
            terminal: false,
            expected_outcome: "access-1",
            expected_grants: &["refresh_token"],
            expected_cached_refresh: Some("refresh-cached"),
            expected_retryable: false,
            expected_repeated: ("access-1", 1),
        },
        UserTokenTestCase {
            description: "a revoked cached refresh token, with a terminal, leads to a new consent whose refresh token replaces it",
            configured: None,
            cached: Some(("access-old", -10, "refresh-cached")),
            refused: None,
            revoked: &["refresh-cached"],
            client_secret: "secret",
            terminal: true,
            expected_outcome: "access-2",
            expected_grants: &["refresh_token", "authorization_code"],
            expected_cached_refresh: Some("refresh-2"),
            expected_retryable: false,
            expected_repeated: ("access-2", 2),
        },
        UserTokenTestCase {
            description: "a revoked cached refresh token, without a terminal, fails saying it was refused and how to sign in again",
            configured: None,
            cached: Some(("access-old", -10, "refresh-cached")),
            refused: None,
            revoked: &["refresh-cached"],
            client_secret: "secret",
            terminal: false,
            expected_outcome: "Google refused the cached refresh token of connection \"sheets\" (invalid_grant: Token has been expired or revoked.), so it needs a new sign-in: no usable Google sign-in",
            expected_grants: &["refresh_token"],
            expected_cached_refresh: Some("refresh-cached"),
            expected_retryable: false,
            expected_repeated: ("Google refused the cached refresh token", 2),
        },
        UserTokenTestCase {
            description: "a wrong client secret fails, naming the client, and is not retried",
            configured: Some("refresh-own"),
            cached: None,
            refused: None,
            revoked: &[],
            client_secret: "wrong",
            terminal: true,
            expected_outcome: "Google's token endpoint refused OAuth client client.apps.googleusercontent.com (invalid_client: The OAuth client was not found.); check the connection's client_id and client_secret",
            expected_grants: &["refresh_token"],
            expected_cached_refresh: None,
            expected_retryable: false,
            expected_repeated: ("refused OAuth client", 2),
        },
        UserTokenTestCase {
            description: "a token endpoint that is down fails as a rerun may fix",
            configured: Some("unavailable"),
            cached: None,
            refused: None,
            revoked: &[],
            client_secret: "secret",
            terminal: false,
            expected_outcome: "HTTP 503 backend_error: Google's token endpoint: try later",
            expected_grants: &["refresh_token"],
            expected_cached_refresh: None,
            expected_retryable: true,
            expected_repeated: ("HTTP 503", 2),
        },
    ];
    for test_case in test_cases {
        let seen = block_on(user_token_outcome(&test_case))?;
        assert!(
            seen.outcome.contains(test_case.expected_outcome),
            "{}: {}",
            test_case.description,
            seen.outcome
        );
        assert_eq!(
            (seen.grants, seen.cached_refresh.as_deref(), seen.retryable),
            (
                test_case
                    .expected_grants
                    .iter()
                    .map(|grant| (*grant).to_owned())
                    .collect::<Vec<String>>(),
                test_case.expected_cached_refresh,
                test_case.expected_retryable
            ),
            "{}",
            test_case.description
        );
        assert!(
            seen.repeated.0.contains(test_case.expected_repeated.0)
                && seen.repeated.1 == test_case.expected_repeated.1,
            "{}: asked again: {:?}",
            test_case.description,
            seen.repeated
        );
    }
    Ok(())
}
