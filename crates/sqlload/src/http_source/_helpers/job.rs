//! Checking a job before any request, and running it: one endpoint, or a fan-out over keys.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::{Map, Value};
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::task::JoinSet;

use crate::http_source::_helpers::endpoint::{endpoint, interval, shared_limiter};
use crate::http_source::_helpers::oauth::token_url;
use crate::http_source::_helpers::pages::{paginate, set_at};
use crate::http_source::_helpers::selector::{selected_rows, selector};
use crate::http_source::_helpers::urls::{filled_path, placeholders};
use crate::python_sources::errors::ReadError;
use crate::row_batches::main::projection::projection;
use crate::row_batches::main::projection_problem::projection_problem;
use crate::row_batches::models::{Projection, ProjectionRules};
use std::time::Duration;

use crate::http::main::install_tls_provider::install_tls_provider;
use crate::http_source::constants::{
    BODY_DROPPING_REDIRECTS, JOB_THREADS, MAX_REDIRECTS, PAGES_AHEAD,
};
use crate::http_source::models::{
    Auth, Calls, FanOut, FieldSpec, HttpJob, HttpPage, KeyFeed, Location, Method, Over, Paginator,
    Request, Selector, StatusAction,
};

/// Every problem the job's definition has, found before any request, including projected rows that break the table's `rules`.
pub(crate) fn job_problems(job: &HttpJob, rules: ProjectionRules<'_>) -> Vec<String> {
    let rows = match job {
        HttpJob::Paginate(request) => request,
        HttpJob::FanOut(fan_out) => &fan_out.request,
    };
    let mut problems = match request_projection(rows) {
        Ok(Some(projection)) => projection_problem(&projection, rules).into_iter().collect(),
        Ok(None) => Vec::new(),
        Err(problem) => vec![problem],
    };
    problems.extend(match job {
        HttpJob::Paginate(request) => {
            let mut problems = request_problems(request);
            problems.extend(field_problem(request));
            problems.extend(placeholders(&request.path).first().map(|name| {
                format!(
                    "paginate path {} has the placeholder {{{name}}}; use fan_out to fill it",
                    request.path
                )
            }));
            problems
        }
        HttpJob::FanOut(fan_out) => fan_out_problems(fan_out),
    });
    problems
}

/// The projection `request`'s `fields` and `keep_record` describe, if any.
pub(crate) fn request_projection(request: &Request) -> Result<Option<Projection>, String> {
    let fields = request.fields.as_deref().map(field_triples);
    projection(fields.as_deref(), request.keep_record.as_deref())
}

/// Each field as (column, path, required).
fn field_triples(fields: &[FieldSpec]) -> Vec<(String, String, bool)> {
    fields
        .iter()
        .map(|field| (field.name.clone(), field.path.clone(), field.required))
        .collect()
}

/// Why the rows' `select` names a key field, which only fan_out's over reads, if it does.
fn field_problem(request: &Request) -> Option<String> {
    let field = match selector(request.select.as_deref()) {
        Ok(selected) => selected.field?,
        Err(_) => return None,
    };
    Some(format!(
        "select {:?} names a key field ({field}); that form is for fan_out's over, so select the rows instead",
        request.select.clone().unwrap_or_default()
    ))
}

fn request_problems(request: &Request) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    problems.extend(selector(request.select.as_deref()).err());
    problems.extend(paginator_problem(&request.paginator));
    problems.extend(body_problems(request));
    problems.extend(match &request.client.auth {
        Auth::Oauth2ClientCredentials(spec)
        | Auth::Oauth2RefreshToken(spec)
        | Auth::Oauth2Password(spec) => token_url(spec).err(),
        Auth::None | Auth::Bearer | Auth::ApiKey { .. } | Auth::Basic => None,
    });
    problems.extend(
        request
            .client
            .headers
            .iter()
            .filter_map(|(name, value)| header_problem(name, value)),
    );
    problems.extend(
        request
            .client
            .rate_limit
            .as_deref()
            .and_then(|rate| interval(rate).err()),
    );
    problems.extend(
        request
            .on_status
            .keys()
            .filter_map(|status| match status.parse::<u16>() {
                Ok(100..=599) => None,
                _ => Some(format!("on_status {status} is not an HTTP status")),
            }),
    );
    problems
}

/// Why the paginator cannot work, if it cannot: a pointer that is not a plain path, or an offset limit of 0.
fn paginator_problem(paginator: &Paginator) -> Option<String> {
    match paginator {
        Paginator::NextLink { pointer } | Paginator::Cursor { pointer, .. } => {
            match selector(Some(pointer)) {
                Ok(parsed) if parsed.field.is_none() && !parsed.rows.is_empty() => None,
                Ok(_) | Err(_) => Some(format!(
                    "paginator pointer {pointer:?} is not a path such as \"$.next\" or \"$.meta.next_cursor\""
                )),
            }
        }
        Paginator::Offset { limit: 0, .. } => Some("sl.offset limit must be at least 1".to_owned()),
        Paginator::Offset { .. }
        | Paginator::Single
        | Paginator::LinkHeader
        | Paginator::PageNumber { .. } => None,
    }
}

/// Why the job's body settings cannot work: a body or body paging on a `GET`, a body path that is not a plain path, or body paging into a body that is not an object.
fn body_problems(request: &Request) -> Vec<String> {
    let (paths, location) = match &request.paginator {
        Paginator::Offset {
            param,
            limit_param,
            location,
            ..
        } => (vec![param, limit_param], *location),
        Paginator::PageNumber {
            param, location, ..
        }
        | Paginator::Cursor {
            param, location, ..
        } => (vec![param], *location),
        Paginator::Single | Paginator::NextLink { .. } | Paginator::LinkHeader => {
            (Vec::new(), Location::Query)
        }
    };
    let mut problems: Vec<String> = Vec::new();
    if request.method == Method::Get && request.json_body.is_some() {
        problems.push("json_body is sent only with method=\"POST\"".to_owned());
    }
    if location == Location::Query {
        return problems;
    }
    if request.method == Method::Get {
        problems.push("a paginator with location=\"body\" needs method=\"POST\"".to_owned());
    }
    if request
        .json_body
        .as_ref()
        .is_some_and(|body| !body.is_object())
    {
        problems.push(
            "a paginator with location=\"body\" sets fields in json_body, so json_body must be an object".to_owned(),
        );
    }
    problems.extend(paths.iter().filter_map(|path| match selector(Some(path)) {
        Ok(parsed) if parsed.field.is_none() && !parsed.rows.is_empty() => None,
        Ok(_) | Err(_) => Some(format!(
            "body path {path:?} is not a path such as \"$.page.cursor\""
        )),
    }));
    if problems.is_empty() {
        let mut body = request
            .json_body
            .clone()
            .unwrap_or_else(|| Value::Object(Map::new()));
        problems.extend(
            paths
                .iter()
                .filter_map(|path| set_at(&mut body, path, Value::Null).err()),
        );
    }
    problems
}

/// Why a header cannot be sent, if it cannot.
fn header_problem(name: &str, value: &str) -> Option<String> {
    match (
        reqwest::header::HeaderName::from_bytes(name.as_bytes()),
        reqwest::header::HeaderValue::from_str(value),
    ) {
        (Ok(_), Ok(_)) => None,
        (Err(_), _) | (_, Err(_)) => Some(format!(
            "header {name:?} is not a valid HTTP header name and value"
        )),
    }
}

fn fan_out_problems(fan_out: &FanOut) -> Vec<String> {
    let path = &fan_out.request.path;
    let wanted = placeholders(path);
    let mut problems = request_problems(&fan_out.request);
    problems.extend(field_problem(&fan_out.request));
    problems.extend(
        (fan_out.concurrency == 0).then(|| "fan_out concurrency must be at least 1".to_owned()),
    );
    match &fan_out.over {
        Over::Paginate(over) => {
            problems.extend(request_problems(over));
            problems.extend(
                (over.fields.is_some() || over.keep_record.is_some())
                    .then(|| "fan_out's over picks keys with its select; fields and keep_record are for the fan-out's own rows".to_owned()),
            );
            problems.extend(placeholders(&over.path).first().map(|name| {
                format!("fan_out over path {} has the placeholder {{{name}}}; over must be a plain paginate", over.path)
            }));
            let field = match selector(over.select.as_deref()) {
                Ok(selected) => selected.field,
                Err(_) => None,
            };
            match field {
                Some(field) => problems.extend(
                    wanted
                        .iter()
                        .filter(|name| **name != field)
                        .map(|name| format!("fan-out path {path} needs {{{name}}}, but over gives only {field}")),
                ),
                None => problems.push(format!(
                    "fan_out over a paginate needs a select naming each key's field, such as \"$.items[*].id\", not {:?}",
                    over.select.clone().unwrap_or_else(|| "$".to_owned())
                )),
            }
        }
        Over::Values { .. } if wanted.is_empty() => problems.push(format!(
            "fan-out path {path} has no placeholder, so every key would request the same URL; add one such as {{id}}"
        )),
        Over::Values { values } => problems.extend(
            values
                .iter()
                .find_map(|value| key_problem(path, &wanted, value)),
        ),
    }
    problems
}

/// Why the listed key `value` cannot fill the placeholders `wanted` of `path`, if it cannot.
fn key_problem(path: &str, wanted: &[String], value: &Value) -> Option<String> {
    let missing = |key: &Map<String, Value>| {
        wanted
            .iter()
            .find(|name| !key.contains_key(name.as_str()))
            .cloned()
    };
    match value {
        Value::Object(key) => missing(key).map(|name| {
            format!("fan-out path {path} needs {{{name}}}, which the key {value} lacks")
        }),
        _ if wanted.len() == 1 => None,
        _ => Some(format!(
            "fan-out path {path} has {} placeholders, so over needs objects naming each, not {value}",
            wanted.len()
        )),
    }
}

/// Run `job` on a runtime of its own, handing each page to `deliver` in arrival order; `deliver` returning `false` cancels the requests in flight.
pub(crate) fn drive_job(
    job: HttpJob,
    connections: &Map<String, Value>,
    mut deliver: impl FnMut(HttpPage) -> bool,
) -> Result<(), ReadError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(JOB_THREADS)
        .enable_all()
        .build()
        .map_err(|error| format!("cannot start the sl.http runtime: {error}"))?;
    let (sender, mut receiver) = channel(PAGES_AHEAD);
    let task = runtime.spawn(run_job(job, connections.clone(), sender));
    while let Some(page) = receiver.blocking_recv() {
        if !deliver(page) {
            task.abort();
            break;
        }
    }
    drop(receiver);
    match runtime.block_on(task) {
        Ok(result) => result,
        Err(error) if error.is_cancelled() => Ok(()),
        Err(error) => Err(format!("the sl.http job stopped: {error}").into()),
    }
}

/// Run `job`, sending every page to `pages`.
async fn run_job(
    job: HttpJob,
    connections: Map<String, Value>,
    pages: Sender<HttpPage>,
) -> Result<(), ReadError> {
    let posts = match &job {
        HttpJob::Paginate(request) => request.method == Method::Post,
        HttpJob::FanOut(fan_out) => {
            fan_out.request.method == Method::Post
                || matches!(&fan_out.over, Over::Paginate(over) if over.method == Method::Post)
        }
    };
    let http = job_client(posts)?;
    match job {
        HttpJob::Paginate(request) => {
            let calls = request_calls(&request, &connections, &http)?;
            paginate(&calls, &request, &request.path, &pages).await
        }
        HttpJob::FanOut(fan_out) => run_fan_out(*fan_out, &connections, &http, pages).await,
    }
}

/// A pooled client that follows redirects only within the first request's origin, so connection headers and credentials never reach another host; when the job `posts`, it also stops at 301, 302 and 303, which would resend the request as a `GET` without its body.
fn job_client(posts: bool) -> Result<reqwest::Client, String> {
    install_tls_provider();
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::custom(move |attempt| {
            match posts && BODY_DROPPING_REDIRECTS.contains(&attempt.status().as_u16()) {
                true => attempt.stop(),
                false => same_origin_redirect(attempt),
            }
        }))
        .pool_idle_timeout(Duration::from_secs(90))
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(300))
        .user_agent(concat!("sqlload/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|error| format!("cannot start the HTTP client: {error}"))
}

fn same_origin_redirect(attempt: reqwest::redirect::Attempt<'_>) -> reqwest::redirect::Action {
    let crossed = attempt
        .previous()
        .first()
        .is_some_and(|first| first.origin() != attempt.url().origin());
    match crossed || attempt.previous().len() > MAX_REDIRECTS {
        true => attempt.stop(),
        false => attempt.follow(),
    }
}

/// What requests of `request` need: its connection's endpoint, the shared limiter and its statuses.
fn request_calls(
    request: &Request,
    connections: &Map<String, Value>,
    http: &reqwest::Client,
) -> Result<Calls, String> {
    let client = &request.client;
    let limiter = client
        .rate_limit
        .as_deref()
        .map(|rate| shared_limiter(&client.connection, rate))
        .transpose()?;
    let on_status = request
        .on_status
        .iter()
        .map(|(status, action)| {
            let code = status
                .parse::<u16>()
                .map_err(|_| format!("on_status {status} is not an HTTP status"))?;
            Ok((code, *action))
        })
        .collect::<Result<BTreeMap<u16, StatusAction>, String>>()?;
    Ok(Calls {
        http: http.clone(),
        endpoint: endpoint(client, connections)?,
        limiter,
        retries: client.retries,
        on_status,
    })
}

/// Page through the fan-out's path for each key, up to `concurrency` at once, while the keys still arrive.
async fn run_fan_out(
    fan_out: FanOut,
    connections: &Map<String, Value>,
    http: &reqwest::Client,
    pages: Sender<HttpPage>,
) -> Result<(), ReadError> {
    let calls = request_calls(&fan_out.request, connections, http)?;
    let (key_sender, key_receiver) = channel::<Map<String, Value>>(fan_out.concurrency * 2);
    let keys = Arc::new(Mutex::new(key_receiver));
    let mut tasks: JoinSet<Result<(), ReadError>> = JoinSet::new();
    let feed = match fan_out.over.clone() {
        Over::Values { values } => KeyFeed::Values(values),
        Over::Paginate(over) => {
            KeyFeed::Pages(Box::new((request_calls(&over, connections, http)?, *over)))
        }
    };
    let wanted = placeholders(&fan_out.request.path);
    tasks.spawn(feed_keys(feed, wanted, key_sender));
    let request = Arc::new(fan_out.request);
    for _ in 0..fan_out.concurrency {
        let (calls, request, keys, pages) =
            (calls.clone(), request.clone(), keys.clone(), pages.clone());
        tasks.spawn(async move {
            loop {
                let key = keys.lock().await.recv().await;
                let Some(key) = key else { return Ok(()) };
                let path = filled_path(&request.path, &key)?;
                paginate(&calls, &request, &path, &pages).await?;
            }
        });
    }
    while let Some(finished) = tasks.join_next().await {
        finished.map_err(|error| format!("a fan-out request stopped: {error}"))??;
    }
    Ok(())
}

/// Send each key: a value fills the path's one placeholder, an object fills them by name, and a paginate's selected field is named after itself.
async fn feed_keys(
    feed: KeyFeed,
    wanted: Vec<String>,
    keys: Sender<Map<String, Value>>,
) -> Result<(), ReadError> {
    let single = |value: Value| -> Map<String, Value> {
        match value {
            Value::Object(key) => key,
            other => wanted
                .first()
                .map(|name| (name.clone(), other))
                .into_iter()
                .collect(),
        }
    };
    match feed {
        KeyFeed::Values(values) => {
            for value in values {
                if keys.send(single(value)).await.is_err() {
                    return Ok(());
                }
            }
            Ok(())
        }
        KeyFeed::Pages(pages) => {
            let (calls, over) = *pages;
            let selected = selector(over.select.as_deref())?;
            let field = selected.field.clone().unwrap_or_default();
            let (page_sender, page_receiver) = channel::<HttpPage>(PAGES_AHEAD);
            let pager = async move { paginate(&calls, &over, &over.path, &page_sender).await };
            let forward = forward_keys(page_receiver, (&selected, &field), &keys);
            let (paged, forwarded) = tokio::join!(pager, forward);
            paged.and(forwarded)
        }
    }
}

/// Each page's selected field values as keys named after the field.
async fn forward_keys(
    mut pages: Receiver<HttpPage>,
    (selected, field): (&Selector, &str),
    keys: &Sender<Map<String, Value>>,
) -> Result<(), ReadError> {
    while let Some(page) = pages.recv().await {
        let document: Value = serde_json::from_slice(&page.bytes)
            .map_err(|error| format!("the fan-out's over response is not JSON: {error}"))?;
        for row in selected_rows(&document, selected) {
            let Some(value) = row.get(field).filter(|value| !value.is_null()) else {
                continue;
            };
            let key: Map<String, Value> = [(field.to_owned(), value.clone())].into_iter().collect();
            if keys.send(key).await.is_err() {
                return Ok(());
            }
        }
    }
    Ok(())
}
