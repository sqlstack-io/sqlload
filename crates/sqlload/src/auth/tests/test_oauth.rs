use crate::auth::_helpers::oauth::{challenge, redirect_code, scope};
use crate::auth::tests::test_types::{ChallengeTestCase, RedirectCodeTestCase, ScopeTestCase};

#[test]
fn given_a_pkce_verifier_when_deriving_its_challenge_then_it_is_the_s256_challenge() {
    let test_cases = [ChallengeTestCase {
        description: "RFC 7636 appendix B",
        verifier: "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk",
        expected_challenge: "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
    }];
    for test_case in &test_cases {
        assert_eq!(
            challenge(test_case.verifier),
            test_case.expected_challenge,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_where_a_sign_in_ended_when_reading_its_code_then_only_a_code_for_this_request_is_taken() {
    let test_cases = [
        RedirectCodeTestCase {
            description: "the browser's redirect",
            redirected: "/?code=abc&state=s1",
            expected_outcome: "abc",
        },
        RedirectCodeTestCase {
            description: "a pasted address with spaces and a newline around it",
            redirected: "  http://127.0.0.1:49152/?state=s1&code=a%2Bb \n",
            expected_outcome: "a+b",
        },
        RedirectCodeTestCase {
            description: "a code for another request is refused",
            redirected: "http://127.0.0.1:49152/?code=abc&state=other",
            expected_outcome: "state does not match",
        },
        RedirectCodeTestCase {
            description: "Snowflake's error is reported before the state is checked",
            redirected: "/?error=invalid_scope&error_description=no+such+role",
            expected_outcome: "Snowflake refused the sign-in: invalid_scope no such role",
        },
        RedirectCodeTestCase {
            description: "an address without a code",
            redirected: "http://127.0.0.1:49152/?state=s1",
            expected_outcome: "has no code= in it",
        },
    ];
    for test_case in &test_cases {
        let found =
            redirect_code(test_case.redirected, "s1").unwrap_or_else(|error| error.to_string());
        assert!(
            found.contains(test_case.expected_outcome),
            "{}: {found}",
            test_case.description
        );
    }
}

#[test]
fn given_a_configured_role_when_asking_for_a_scope_then_the_role_is_named_as_snowflake_stores_it() {
    let test_cases = [
        ScopeTestCase {
            description: "no role: the user's default role",
            role: None,
            expected_scope: "refresh_token",
        },
        ScopeTestCase {
            description: "a plain name is upper-cased",
            role: Some("loader_1"),
            expected_scope: "refresh_token session:role:LOADER_1",
        },
        ScopeTestCase {
            description: "any other name is URL-encoded as written",
            role: Some("Data Loader"),
            expected_scope: "refresh_token session:role-encoded:Data%20Loader",
        },
    ];
    for test_case in &test_cases {
        assert_eq!(
            scope(test_case.role),
            test_case.expected_scope,
            "{}",
            test_case.description
        );
    }
}
