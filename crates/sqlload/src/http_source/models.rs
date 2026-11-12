use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::{Map, Value};
use tokio::sync::mpsc::Sender;

/// A job a table function built with `sl.http(...)`: run entirely in Rust, its pages parsed like `sl.json(...)` bytes.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HttpJob {
    /// Every page of one endpoint.
    Paginate(Box<Request>),
    /// Every page of one endpoint per key of `over`, up to `concurrency` endpoints at once.
    FanOut(Box<FanOut>),
}

/// One endpoint and how to page through it.
#[derive(Debug, Clone, Deserialize)]
pub struct Request {
    pub client: ClientSpec,
    /// A path under the connection's `base_url`, with `{name}` placeholders in a fan-out.
    pub path: String,
    /// Query parameters; a null value is left out.
    #[serde(default)]
    pub params: Map<String, Value>,
    #[serde(default)]
    pub method: Method,
    /// The JSON body every `POST` starts from; each page's copy gets the paginator's values when they go in the body.
    #[serde(default)]
    pub json_body: Option<Value>,
    pub paginator: Paginator,
    /// Where the rows are in each response, such as `$.items`; `$` (or none) is the whole response.
    #[serde(default)]
    pub select: Option<String>,
    /// What an HTTP status other than success means, such as `{"404": "skip"}`.
    #[serde(default)]
    pub on_status: BTreeMap<String, StatusAction>,
    /// The only values of each row that load, renamed: `None` loads every key under its own name.
    #[serde(default)]
    pub fields: Option<Vec<FieldSpec>>,
    /// A column holding each row's whole object as JSON.
    #[serde(default)]
    pub keep_record: Option<String>,
    /// For a job used as a fan-out's `over`: the name its picked values fill as keys, when not the selected field's own (such as `order_id` for `$.items[*].id`).
    #[serde(default)]
    pub key: Option<String>,
}

/// The HTTP method of a job's requests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Method {
    #[default]
    Get,
    Post,
}

impl Method {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
        }
    }
}

/// Where a paginator's values go: the query string, or a path in the JSON body.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Location {
    #[default]
    Query,
    Body,
}

/// One request of a page: its method, URL and JSON body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageCall {
    pub method: Method,
    pub url: reqwest::Url,
    pub body: Option<Value>,
}

/// One projected column of a job's rows.
#[derive(Debug, Clone, Deserialize)]
pub struct FieldSpec {
    pub name: String,
    /// Such as `$.address.city`.
    pub path: String,
    #[serde(default)]
    pub required: bool,
}

/// A request per key of `over`.
#[derive(Debug, Clone, Deserialize)]
pub struct FanOut {
    #[serde(flatten)]
    pub request: Request,
    pub over: Over,
    pub concurrency: usize,
}

/// The keys a fan-out requests.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Over {
    /// Scalars (filling the path's one placeholder) or objects (filling placeholders by name).
    Values { values: Vec<Value> },
    /// The values its `select` (`$.items[*].id`) picks from every page, named after the last field.
    Paginate(Box<Request>),
    /// The values another fan-out's `select` picks from each of its pages, each key keeping the fields of the key above it, so a chain of fan-outs goes as many levels deep as it nests.
    FanOut(Box<FanOut>),
}

/// The connection a job uses and how it calls it.
#[derive(Debug, Clone, Deserialize)]
pub struct ClientSpec {
    pub connection: String,
    #[serde(default)]
    pub auth: Auth,
    /// Requests allowed per second, minute or hour, such as `10/s`, shared by every job of the connection with the same limit.
    #[serde(default)]
    pub rate_limit: Option<String>,
    /// Retries of a request that failed with a transport error, 429 or a 5xx.
    pub retries: u32,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}

/// Credentials from the connection's settings, sent only to the connection's own origin.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Auth {
    #[default]
    None,
    /// `Authorization: Bearer <token>`.
    Bearer,
    /// The token in a header or a query parameter.
    ApiKey {
        #[serde(default)]
        header: Option<String>,
        #[serde(default)]
        query: Option<String>,
    },
    /// `username` and `password`.
    Basic,
    /// An access token from `token_url` for the connection's `client_id` and `client_secret`.
    Oauth2ClientCredentials(OAuth2Spec),
    /// An access token from `token_url` for the connection's `refresh_token`.
    Oauth2RefreshToken(OAuth2Spec),
    /// An access token from `token_url` for the connection's `username` and `password` (the resource-owner password grant).
    Oauth2Password(OAuth2Spec),
}

/// Where an OAuth2 access token comes from and how the client authenticates there.
#[derive(Debug, Clone, Deserialize)]
pub struct OAuth2Spec {
    pub token_url: String,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub client_auth: ClientAuth,
}

/// How the client's id and secret reach the token endpoint.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientAuth {
    /// HTTP basic auth, which every OAuth2 server must accept.
    #[default]
    Basic,
    /// Form fields `client_id` and `client_secret`.
    Post,
}

/// The form an OAuth2 token request posts, and the access token it last returned.
#[derive(Debug)]
pub struct TokenSource {
    pub token_url: reqwest::Url,
    /// `grant_type` and its fields, plus the client's fields under `client_auth = "post"`.
    pub form: Vec<(String, String)>,
    /// For a grant that starts from something other than a refresh token (the password grant): the `refresh_token` grant's form, used instead of `form` once a token response hands one out.
    pub refresh_form: Option<Vec<(String, String)>>,
    /// `client_id` and `client_secret` for basic client auth, form-encoded as RFC 6749 asks.
    pub basic: Option<(String, String)>,
    pub state: tokio::sync::Mutex<TokenState>,
    /// Every secret of the token flow, including each access token issued, for redaction.
    pub secrets: Mutex<Vec<String>>,
}

/// The current access token, when it was issued and should be replaced, and a rotated refresh token.
#[derive(Debug, Default)]
pub struct TokenState {
    pub access: Option<String>,
    pub issued_at: Option<Instant>,
    pub renew_at: Option<Instant>,
    pub refresh_token: Option<String>,
}

/// The grant an OAuth2 auth uses, with the connection's settings it needs.
#[derive(Debug, Clone)]
pub enum Grant {
    ClientCredentials {
        client_id: String,
        client_secret: String,
    },
    RefreshToken {
        refresh_token: String,
        /// `client_id` and `client_secret`, when the connection has them.
        client: (Option<String>, Option<String>),
    },
    Password {
        username: String,
        password: String,
        /// `client_id` and `client_secret`, when the connection has them.
        client: (Option<String>, Option<String>),
    },
}

/// What a token response issued.
#[derive(Debug, Clone)]
pub struct Issued {
    pub access: String,
    /// When after issue to renew it; `None` keeps it until a 401.
    pub renew_after: Option<Duration>,
    pub refresh_token: Option<String>,
}

/// How the next page is found; always named, never guessed.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Paginator {
    /// One request.
    Single,
    /// The next page's URL at `pointer` in the body; ends when it is missing, null or empty.
    NextLink { pointer: String },
    /// The `Link` header's `rel="next"`.
    LinkHeader,
    /// `param` counts rows from 0 by `limit`; ends at a page with fewer rows. In the body, `param` and `limit_param` are paths such as `$.page.offset`.
    Offset {
        param: String,
        limit_param: String,
        limit: u64,
        #[serde(default)]
        location: Location,
    },
    /// `param` counts pages from `start`; ends at an empty page.
    PageNumber {
        param: String,
        start: u64,
        #[serde(default)]
        location: Location,
    },
    /// The token at `pointer` in the response goes back as `param`; ends when it is missing, null or empty.
    Cursor {
        pointer: String,
        param: String,
        #[serde(default)]
        location: Location,
    },
}

/// What a listed status does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatusAction {
    /// No rows for that request.
    Skip,
}

/// One response's body, and where its rows are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpPage {
    pub bytes: Vec<u8>,
    /// The dotted path to the rows, as `sl.json(..., path=...)` takes it.
    pub path: Option<String>,
    /// The request, secrets hidden, for errors about the page.
    pub source: String,
    /// The columns each row loads as, when the job names its fields.
    pub projection: Option<crate::row_batches::models::Projection>,
}

/// A parsed `select`: the fields down to the rows and, for keys, the field of each row.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Selector {
    pub rows: Vec<String>,
    pub field: Option<String>,
}

/// A connection resolved for requests: its base URL, headers and credentials.
#[derive(Debug, Clone)]
pub struct Endpoint {
    pub base_url: reqwest::Url,
    pub headers: BTreeMap<String, String>,
    pub credential: Credential,
    /// Values never shown in errors.
    pub secrets: Vec<String>,
}

/// Credentials ready to attach to a request.
#[derive(Debug, Clone)]
pub enum Credential {
    None,
    Header {
        name: String,
        value: String,
    },
    Query {
        name: String,
        value: String,
    },
    /// `Authorization: Bearer <access token>`, the token fetched and renewed for the run.
    OAuth2(std::sync::Arc<TokenSource>),
}

/// Requests spaced `interval` apart, shared across jobs.
#[derive(Debug)]
pub struct Limiter {
    pub interval: Duration,
    pub next: Mutex<Instant>,
}

/// What one request needs: the pooled client, the endpoint, the shared rate limit and the retry budget.
#[derive(Debug, Clone)]
pub struct Calls {
    pub http: reqwest::Client,
    pub endpoint: Endpoint,
    pub limiter: Option<std::sync::Arc<Limiter>>,
    pub retries: u32,
    pub on_status: BTreeMap<u16, StatusAction>,
}

/// What one try at a request came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Attempt {
    /// The body, or `None` when `on_status` skips the status.
    Done(Option<Fetched>),
    /// A 401 to an OAuth2 access token: renew the token and try once more.
    Refused,
    /// A failure worth retrying, after `after` when the server said.
    Retry {
        reason: String,
        after: Option<Duration>,
    },
}

/// A body fetched, with the `Link` header's next URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fetched {
    pub bytes: Vec<u8>,
    pub next_link: Option<String>,
    pub url: reqwest::Url,
}

/// Where a fan-out's keys come from: listed values, another endpoint's pages, or a fan-out one level up, run with the job's connections and client.
#[derive(Debug)]
pub enum KeyFeed {
    Values(Vec<Value>),
    Pages(Box<(Calls, Request)>),
    FanOut(Box<(FanOut, Map<String, Value>, reqwest::Client)>),
}

/// A fan-out key: the placeholder values it fills, its ancestors' included, and the trail of requests that produced it (such as `/orders order_id=7 → `), which prefixes a multi-level job's errors.
#[derive(Debug, Clone, Default)]
pub struct FanKey {
    pub fields: Map<String, Value>,
    pub trail: String,
}

/// Where a fan-out level's pages go: out as the job's rows, or into keys for the level below.
#[derive(Debug, Clone)]
pub enum Sink {
    Pages(Sender<HttpPage>),
    Keys(Sender<FanKey>),
}
