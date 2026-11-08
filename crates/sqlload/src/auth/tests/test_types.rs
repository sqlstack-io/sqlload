use std::collections::BTreeSet;

/// A token needed with what the cache, the fake Snowflake and the terminal hold.
pub(crate) struct TokenLifecycleTestCase {
    pub(crate) description: &'static str,
    /// The cached access token, its seconds left, and its refresh token.
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
    /// The last code exchange's verifier.
    pub(crate) verifier: String,
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
    /// Whether the verifier sent with the code hashes to the challenge in the address the browser was asked to open.
    pub(crate) pkce_matches: bool,
}

/// A PKCE verifier and the S256 challenge it gives.
pub(crate) struct ChallengeTestCase {
    pub(crate) description: &'static str,
    pub(crate) verifier: &'static str,
    pub(crate) expected_challenge: &'static str,
}
