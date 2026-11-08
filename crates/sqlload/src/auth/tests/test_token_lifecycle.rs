use crate::auth::tests::helpers::{block_on, lifecycle};
use crate::auth::tests::test_types::TokenLifecycleTestCase;

#[test]
fn given_a_cache_a_refresh_token_or_a_terminal_when_a_token_is_needed_then_the_browser_is_used_only_when_nothing_else_works()
-> Result<(), String> {
    let test_cases = [
        TokenLifecycleTestCase {
            description: "a cached token before its renewal time is used without any request",
            held: None,
            cached: Some(("cached-access", 600, Some("refresh-0"))),
            revoked: &[],
            terminal: false,
            browser: false,
            username: "LOADER",
            refusal: None,
            expected_outcome: "cached-access",
            expected_grants: &[],
            expected_cached_refresh: Some("refresh-0"),
        },
        TokenLifecycleTestCase {
            description: "a token past its renewal time is refreshed, and the rotated refresh token is cached",
            held: None,
            cached: Some(("cached-access", -1, Some("refresh-0"))),
            revoked: &[],
            terminal: false,
            browser: false,
            username: "LOADER",
            refusal: None,
            expected_outcome: "access-1",
            expected_grants: &["refresh_token"],
            expected_cached_refresh: Some("refresh-1"),
        },
        TokenLifecycleTestCase {
            description: "a refresh token Snowflake does not rotate is kept",
            held: None,
            cached: Some(("cached-access", -1, Some("fixed-0"))),
            revoked: &[],
            terminal: false,
            browser: false,
            username: "LOADER",
            refusal: None,
            expected_outcome: "access-1",
            expected_grants: &["refresh_token"],
            expected_cached_refresh: Some("fixed-0"),
        },
        TokenLifecycleTestCase {
            description: "newer tokens another connection cached replace stale ones held, so a used refresh token is never sent",
            held: Some(("held-access", -1, Some("refresh-0"))),
            cached: Some(("cached-access", 600, Some("refresh-9"))),
            revoked: &["refresh-0"],
            terminal: false,
            browser: false,
            username: "LOADER",
            refusal: None,
            expected_outcome: "cached-access",
            expected_grants: &[],
            expected_cached_refresh: Some("refresh-9"),
        },
        TokenLifecycleTestCase {
            description: "a refused refresh token without a terminal fails at once, naming the unattended methods",
            held: None,
            cached: Some(("cached-access", -5, Some("refresh-0"))),
            revoked: &["refresh-0"],
            terminal: false,
            browser: true,
            username: "LOADER",
            refusal: None,
            expected_outcome: "needs a terminal; run `sqlload debug` from a terminal once, or use auth = { method = \"key_pair\" }",
            expected_grants: &["refresh_token"],
            expected_cached_refresh: Some("refresh-0"),
        },
        TokenLifecycleTestCase {
            description: "nothing cached and no terminal fails without a request",
            held: None,
            cached: None,
            revoked: &[],
            terminal: false,
            browser: true,
            username: "LOADER",
            refusal: None,
            expected_outcome: "no usable Snowflake sign-in is cached for loader",
            expected_grants: &[],
            expected_cached_refresh: None,
        },
        TokenLifecycleTestCase {
            description: "a refused refresh token at a terminal signs in through the browser's redirect, past forged ones",
            held: None,
            cached: Some(("cached-access", -5, Some("refresh-0"))),
            revoked: &["refresh-0"],
            terminal: true,
            browser: true,
            username: "LOADER",
            refusal: None,
            expected_outcome: "access-1",
            expected_grants: &["refresh_token", "authorization_code"],
            expected_cached_refresh: Some("refresh-1"),
        },
        TokenLifecycleTestCase {
            description: "with no browser, the pasted address signs in",
            held: None,
            cached: None,
            revoked: &[],
            terminal: true,
            browser: false,
            username: "loader",
            refusal: None,
            expected_outcome: "access-1",
            expected_grants: &["authorization_code"],
            expected_cached_refresh: Some("refresh-1"),
        },
        TokenLifecycleTestCase {
            description: "signing in as another user is refused, and nothing is cached",
            held: None,
            cached: None,
            revoked: &[],
            terminal: true,
            browser: true,
            username: "OTHER",
            refusal: None,
            expected_outcome: "signed in to Snowflake as OTHER, but this connection's user is loader",
            expected_grants: &["authorization_code"],
            expected_cached_refresh: None,
        },
        TokenLifecycleTestCase {
            description: "Snowflake's own refusal in the redirect is reported, with no code exchanged",
            held: None,
            cached: None,
            revoked: &[],
            terminal: true,
            browser: true,
            username: "LOADER",
            refusal: Some("access_denied"),
            expected_outcome: "Snowflake refused the sign-in: access_denied refused",
            expected_grants: &[],
            expected_cached_refresh: None,
        },
    ];
    for test_case in &test_cases {
        let found = block_on(lifecycle(test_case))?;
        assert!(
            found.outcome.contains(test_case.expected_outcome),
            "{}: {}",
            test_case.description,
            found.outcome
        );
        assert_eq!(
            found.grants, test_case.expected_grants,
            "{}",
            test_case.description
        );
        assert_eq!(
            found.cached_refresh.as_deref(),
            test_case.expected_cached_refresh,
            "{}",
            test_case.description
        );
        assert_eq!(
            found.repeated,
            (found.outcome.clone(), test_case.expected_grants.len()),
            "{}: asking again reuses the token, or the run's sign-in failure, without another request",
            test_case.description
        );
    }
    Ok(())
}
