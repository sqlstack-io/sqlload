/// The variable naming a credentials file, which Google's libraries read first.
pub const CREDENTIALS_VARIABLE: &str = "GOOGLE_APPLICATION_CREDENTIALS";

/// The variable that moves the metadata server, as Google's libraries read it.
pub const METADATA_HOST_VARIABLE: &str = "GCE_METADATA_HOST";

/// The metadata server's host on Google Cloud.
pub const METADATA_HOST: &str = "metadata.google.internal";

/// The header every metadata server request carries, and its answers too.
pub const METADATA_FLAVOR: (&str, &str) = ("Metadata-Flavor", "Google");

/// How long the metadata server may take to answer; off Google Cloud there is none.
pub const METADATA_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Google's OAuth token endpoint, where a gcloud login is renewed.
pub const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";

/// IAM Credentials' root, where one service account gets tokens for another.
pub const IAM_CREDENTIALS_URL: &str = "https://iamcredentials.googleapis.com/v1";

/// The scope a token asks for when it is used to sign in further: to impersonate, or exchanged at STS.
pub const CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

/// How long an impersonated token lasts unless the credentials file says otherwise; Google's longest without an organization policy.
pub const IMPERSONATED_LIFETIME_SECS: u64 = 3600;

/// The STS grant exchanging a subject token for a Google access token.
pub const TOKEN_EXCHANGE_GRANT: &str = "urn:ietf:params:oauth:grant-type:token-exchange";

/// The token type STS is asked for.
pub const ACCESS_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:access_token";

/// The OAuth grant exchanging a signed service-account assertion for a token.
pub const JWT_BEARER_GRANT: &str = "urn:ietf:params:oauth:grant-type:jwt-bearer";

/// How long a signed assertion is valid; Google takes at most an hour.
pub const ASSERTION_LIFETIME_SECS: u64 = 3600;

/// A token is fetched again this long before it expires.
pub const TOKEN_REFRESH_MARGIN_MS: i64 = 120_000;

/// The variable that must be `1` before a credentials file's executable runs, as Google's libraries require.
pub const ALLOW_EXECUTABLES_VARIABLE: &str = "GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES";

/// How long a credentials file's executable may run unless the file says otherwise.
pub const EXECUTABLE_TIMEOUT_MS: u64 = 30_000;

/// The subject token type of AWS's signed `GetCallerIdentity` request.
pub const AWS_SUBJECT_TOKEN_TYPE: &str = "urn:ietf:params:aws:token-type:aws4_request";

/// How many characters of an answer an error quotes.
pub const ERROR_CHARS: usize = 300;

/// The only AWS credential source version Google defines, in a file's `credential_source.environment_id`.
pub const AWS_ENVIRONMENT: &str = "aws1";
