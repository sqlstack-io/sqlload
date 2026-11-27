use crate::auth::models::Secret;

/// The fields of a service-account key file that SQLLoad uses.
#[derive(Debug, Clone)]
pub struct ServiceAccountKey {
    pub client_email: String,
    /// The PEM private key.
    pub private_key: Secret,
    /// Where tokens are exchanged for signed assertions.
    pub token_uri: String,
    /// The project the key belongs to, used when the connection names none.
    pub project_id: String,
}

/// How a Google connection signs in, and where SQLLoad found it.
#[derive(Debug, Clone)]
pub struct GoogleCredentials {
    pub origin: CredentialOrigin,
    pub signer: Signer,
    /// The service account the signer's token gets tokens for, when it impersonates one.
    pub impersonate: Option<Impersonation>,
}

/// Where a connection's credentials were found, in Google's order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialOrigin {
    /// The connection's `service_account_key`.
    Connection,
    /// The file `GOOGLE_APPLICATION_CREDENTIALS` names.
    Named { path: String },
    /// gcloud's application default credentials file.
    Gcloud { path: String },
    /// None of them: the machine's metadata server.
    Machine,
}

/// What signs in first.
#[derive(Debug, Clone)]
pub enum Signer {
    /// A service-account key, signing its own assertions.
    ServiceAccount(ServiceAccountKey),
    /// A gcloud user login (`authorized_user`), renewed with its refresh token.
    User(AuthorizedUser),
    /// The service account attached to the machine, from its metadata server at `host`.
    Metadata { host: String },
    /// Workload identity federation (`external_account`): another cloud's token, exchanged at Google STS.
    External(Box<ExternalAccount>),
}

/// A gcloud user login.
#[derive(Debug, Clone)]
pub struct AuthorizedUser {
    pub client_id: String,
    pub client_secret: Secret,
    pub refresh_token: Secret,
    pub token_uri: String,
    /// The project a login bills quota to, which stands in for the connection's project.
    pub quota_project: Option<String>,
}

/// A service account another identity gets tokens for, through IAM Credentials `generateAccessToken`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Impersonation {
    /// The account's email.
    pub account: String,
    /// The `generateAccessToken` URL.
    pub url: String,
    /// The accounts the request passes through, each impersonating the next.
    pub delegates: Vec<String>,
    pub lifetime_secs: u64,
}

/// An `external_account` credentials file's sign-in: where its subject token comes from and the STS exchange.
#[derive(Debug, Clone)]
pub struct ExternalAccount {
    /// The workload identity pool provider, as STS's `audience`.
    pub audience: String,
    pub subject_token_type: String,
    /// Google STS's token endpoint.
    pub token_url: String,
    pub source: SubjectSource,
    /// The file's `quota_project_id`.
    pub quota_project: Option<String>,
}

/// Where a federated identity's subject token comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubjectSource {
    /// Read from a file, its text or one field of its JSON.
    File { path: String, field: Option<String> },
    /// Fetched from a URL with these headers, its text or one field of its JSON.
    Url {
        url: String,
        headers: Vec<(String, Secret)>,
        field: Option<String>,
    },
    /// Printed by a command, as Google's executable-sourced credentials define.
    Executable {
        command: String,
        timeout_ms: u64,
        output_file: Option<String>,
    },
    /// AWS: a signed `GetCallerIdentity` request, with the keys from the environment or the instance metadata service.
    Aws(AwsSource),
}

/// An AWS credential source's endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsSource {
    /// The instance metadata URL giving the region (its zone, which ends in a letter).
    pub region_url: Option<String>,
    /// The instance metadata URL listing the instance role, whose keys are below it.
    pub url: Option<String>,
    /// The `GetCallerIdentity` URL, with `{region}` to fill in.
    pub verification_url: String,
    /// The IMDSv2 session token URL, when the instance requires one.
    pub session_url: Option<String>,
    /// `AWS_REGION`, else `AWS_DEFAULT_REGION`, when the environment sets one; else the instance's.
    pub region: Option<String>,
    /// The keys in `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`, when the environment sets both of the first two; else the instance role's.
    pub keys: Option<AwsKeys>,
}

/// AWS keys, as the environment or an instance role gives them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsKeys {
    pub access_key_id: String,
    pub secret_access_key: Secret,
    pub session_token: Option<Secret>,
}

impl GoogleCredentials {
    /// How the connection signs in and where SQLLoad found it, for `sqlload debug`; never a secret.
    pub fn describe(&self) -> String {
        let signer = match &self.signer {
            Signer::ServiceAccount(key) => {
                format!("the service-account key of {}", key.client_email)
            }
            Signer::User(_) => "a gcloud user login (authorized_user)".to_owned(),
            Signer::Metadata { host } => {
                format!("the machine's service account, from its metadata server ({host})")
            }
            Signer::External(account) => {
                let source = match &account.source {
                    SubjectSource::File { path, .. } => format!("the token file {path}"),
                    SubjectSource::Url { url, .. } => {
                        let shown = url.split(['?', '#']).next().unwrap_or_default();
                        format!("a token from {shown}")
                    }
                    SubjectSource::Executable { command, .. } => {
                        let program = command.split_whitespace().next().unwrap_or_default();
                        format!("a token {program} prints")
                    }
                    SubjectSource::Aws(source) => match source.keys {
                        Some(_) => {
                            "the AWS keys in AWS_ACCESS_KEY_ID, by a signed GetCallerIdentity"
                        }
                        None => "the EC2 instance role's AWS keys, by a signed GetCallerIdentity",
                    }
                    .to_owned(),
                };
                format!(
                    "workload identity federation with {source}, through provider {}",
                    account.audience
                )
            }
        };
        let origin = match &self.origin {
            CredentialOrigin::Connection => " (the connection's service_account_key)".to_owned(),
            CredentialOrigin::Named { path } => {
                format!(" (GOOGLE_APPLICATION_CREDENTIALS, {path})")
            }
            CredentialOrigin::Gcloud { path } => {
                format!(" (gcloud's application default credentials, {path})")
            }
            CredentialOrigin::Machine => String::new(),
        };
        match &self.impersonate {
            Some(target) => format!("{signer}{origin}, impersonating {}", target.account),
            None => format!("{signer}{origin}"),
        }
    }

    /// The project jobs run in when the connection names none: a connection key's `project_id`; else `environment` (`GOOGLE_CLOUD_PROJECT`), which Google's libraries read before the credentials for application default credentials; else the credentials' own [`project`](Self::project).
    pub fn default_project(&self, environment: Option<String>) -> Option<String> {
        match (&self.origin, &self.signer) {
            (CredentialOrigin::Connection, Signer::ServiceAccount(key)) => {
                Some(key.project_id.clone())
            }
            _ => environment.or_else(|| self.project()),
        }
    }

    /// The project the credentials belong to, where they name one: a key's `project_id`, an impersonated or key service account's project (from its `…@PROJECT.iam.gserviceaccount.com` address), or a login's or federated file's `quota_project_id`.
    pub fn project(&self) -> Option<String> {
        let account_project = |account: &str| {
            account
                .split_once('@')
                .and_then(|(_, domain)| domain.strip_suffix(".iam.gserviceaccount.com"))
                .map(str::to_owned)
        };
        if let Some(target) = &self.impersonate
            && let Some(project) = account_project(&target.account)
        {
            return Some(project);
        }
        match &self.signer {
            Signer::ServiceAccount(key) => Some(key.project_id.clone()),
            Signer::User(user) => user.quota_project.clone(),
            Signer::External(account) => account.quota_project.clone(),
            Signer::Metadata { .. } => None,
        }
    }
}
