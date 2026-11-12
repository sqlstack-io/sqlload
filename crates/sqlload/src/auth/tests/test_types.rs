use std::collections::BTreeSet;

/// A token needed with what the cache, the fake Snowflake and the terminal hold.
pub(crate) struct TokenLifecycleTestCase {
    pub(crate) description: &'static str,
    /// The access token this connection already holds, its seconds until renewal, and its refresh token.
    pub(crate) held: Option<(&'static str, i64, Option<&'static str>)>,
    /// The cached access token, its seconds until renewal, and its refresh token.
    pub(crate) cached: Option<(&'static str, i64, Option<&'static str>)>,
    /// Refresh tokens Snowflake refuses.
    pub(crate) revoked: &'static [&'static str],
    /// Whether a terminal is there to sign in from.
    pub(crate) terminal: bool,
    /// Whether the browser opens (else the address is pasted).
    pub(crate) browser: bool,
    /// The user the browser signs in as.
    pub(crate) username: &'static str,
    /// The error Snowflake redirects with instead of a code.
    pub(crate) refusal: Option<&'static str>,
    /// The token, or text the error contains.
    pub(crate) expected_outcome: &'static str,
    /// The grant type of every token request, in order.
    pub(crate) expected_grants: &'static [&'static str],
    /// The refresh token cached afterwards.
    pub(crate) expected_cached_refresh: Option<&'static str>,
}

/// Where a sign-in ended, as the browser's redirect or a pasted address.
pub(crate) struct RedirectCodeTestCase {
    pub(crate) description: &'static str,
    pub(crate) redirected: &'static str,
    /// The code, or text the error contains.
    pub(crate) expected_outcome: &'static str,
}

/// The role a sign-in asks for.
pub(crate) struct ScopeTestCase {
    pub(crate) description: &'static str,
    pub(crate) role: Option<&'static str>,
    pub(crate) expected_scope: &'static str,
}

/// Sign-ins of several users cached one after another.
pub(crate) struct TokenCacheTestCase {
    pub(crate) description: &'static str,
    pub(crate) users: &'static [&'static str],
    /// The access token read back for each user.
    pub(crate) expected_tokens: &'static [&'static str],
    /// The cache file's permission bits on Unix.
    pub(crate) expected_mode: u32,
}

/// What the fake OAuth server was asked, and how it answers.
#[derive(Debug, Default)]
pub(crate) struct FakeOauth {
    /// The grant type of every token request, in order.
    pub(crate) grants: Vec<String>,
    /// The last authorization request's PKCE challenge, which a code exchange's verifier must match.
    pub(crate) challenge: String,
    pub(crate) revoked: BTreeSet<String>,
    pub(crate) username: String,
    pub(crate) refusal: Option<String>,
    pub(crate) issued: u32,
}

/// What one token request came to.
pub(crate) struct LifecycleOutcome {
    /// The token, or the error.
    pub(crate) outcome: String,
    pub(crate) grants: Vec<String>,
    pub(crate) cached_refresh: Option<String>,
    /// What asking a second time gave, and how many token requests had been made by then.
    pub(crate) repeated: (String, usize),
}

/// When to renew an access token issued at second 1000.
pub(crate) struct RenewAfterTestCase {
    pub(crate) description: &'static str,
    pub(crate) lifetime: Option<u64>,
    pub(crate) expected_renew_after: u64,
}

/// A PKCE verifier and the S256 challenge it gives.
pub(crate) struct ChallengeTestCase {
    pub(crate) description: &'static str,
    pub(crate) verifier: &'static str,
    pub(crate) expected_challenge: &'static str,
}

/// An access token a Google user's client needs, with what the connection, the cache, the fake Google and the terminal hold.
pub(crate) struct UserTokenTestCase {
    pub(crate) description: &'static str,
    /// The connection's own refresh token.
    pub(crate) configured: Option<&'static str>,
    /// The cached access token, its seconds until renewal, and its refresh token.
    pub(crate) cached: Option<(&'static str, i64, &'static str)>,
    /// The access token the server just refused, which the client holds as unexpired.
    pub(crate) refused: Option<&'static str>,
    /// Refresh tokens Google refuses as revoked; `unavailable` makes its token endpoint fail with 503, and `broken` with a 500 and an HTML page.
    pub(crate) revoked: &'static [&'static str],
    /// The client secret sent; the fake takes only `secret`.
    pub(crate) client_secret: &'static str,
    /// Whether a terminal is there to consent from, with a browser that follows the consent.
    pub(crate) terminal: bool,
    /// The token, or text the error contains.
    pub(crate) expected_outcome: &'static str,
    /// The grant type of every token request, in order.
    pub(crate) expected_grants: &'static [&'static str],
    /// The refresh token cached afterwards.
    pub(crate) expected_cached_refresh: Option<&'static str>,
    /// Whether the error is one a rerun may fix.
    pub(crate) expected_retryable: bool,
    /// What asking again gives (the token, or text the error contains), and the token requests made by then.
    pub(crate) expected_repeated: (&'static str, usize),
}

/// What the fake Google OAuth server was asked, and how it answers.
#[derive(Debug, Default)]
pub(crate) struct FakeGoogle {
    pub(crate) grants: Vec<String>,
    /// The last consent's PKCE challenge, which a code exchange's verifier must match.
    pub(crate) challenge: String,
    pub(crate) revoked: Vec<String>,
    pub(crate) issued: u32,
}

/// What one token request came to.
pub(crate) struct UserTokenOutcome {
    /// The token, or the error.
    pub(crate) outcome: String,
    pub(crate) retryable: bool,
    pub(crate) grants: Vec<String>,
    pub(crate) cached_refresh: Option<String>,
    /// What asking again gave, and how many token requests had been made by then.
    pub(crate) repeated: (String, usize),
}
