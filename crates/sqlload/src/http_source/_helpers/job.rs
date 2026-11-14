//! Checking a job before any request, and running it: one endpoint, or a fan-out over keys.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::{Map, Value};
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::task::JoinSet;

use crate::http_source::_helpers::cells::cells_problem;
use crate::http_source::_helpers::endpoint::{endpoint, interval, shared_limiter};
use crate::http_source::_helpers::oauth::token_url;
use crate::http_source::_helpers::pages::{paginate, set_at};
use crate::http_source::_helpers::reshaped::reshapes;
use crate::http_source::_helpers::selector::{enclosing_levels, selected_rows, selector};
use crate::http_source::_helpers::urls::{
    body_placeholders, filled_body, filled_path, placeholders, redacted,
};
use crate::python_sources::errors::ReadError;
use crate::row_batches::main::field_path::field_path;
use crate::row_batches::main::projection::projection;
use crate::row_batches::main::projection_problem::projection_problem;
use crate::row_batches::models::{FieldFrom, Projection, ProjectionRules};
use std::time::Duration;

use crate::http::main::install_tls_provider::install_tls_provider;
use crate::http_source::constants::{
    BODY_DROPPING_REDIRECTS, JOB_THREADS, MAX_READ_AHEAD, MAX_REDIRECTS, PAGES_AHEAD, SHOWN_KEYS,
};
use crate::http_source::models::{
    Auth, Calls, FanKey, FanOut, HttpJob, HttpPage, KeyFeed, Location, Method, Over, Paginator,
    Request, RowContext, Selector, Sink, StatusAction,
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
    problems.extend(rows.key.as_ref().map(|key| {
        format!("key={key:?} names the keys a job gives as a fan_out's over; this job loads rows")
    }));
    problems.extend(context_problems(job));
    problems
}

/// Why the job's fields that read from around each row (`sl.parent`, `sl.key`) or its `key_path` can't work, if they can't.
fn context_problems(job: &HttpJob) -> Vec<String> {
    let (request, fan_out) = match job {
        HttpJob::Paginate(request) => (request.as_ref(), None),
        HttpJob::FanOut(fan_out) => (&fan_out.request, Some(fan_out.as_ref())),
    };
    let select = request.select.as_deref().unwrap_or("$");
    let rows = match selector(request.select.as_deref()) {
        Ok(rows) => rows,
        Err(_) => return Vec::new(),
    };
    let mut problems: Vec<String> = Vec::new();
    let reads = request
        .fields
        .iter()
        .flatten()
        .map(|field| (format!("fields[{:?}]", field.name), &field.from))
        .chain(
            request
                .key_path
                .iter()
                .map(|key_path| ("key_path".to_owned(), &key_path.from)),
        );
    for (what, from) in reads {
        match from {
            FieldFrom::Row => {}
            FieldFrom::Parent { levels: 0 } => {
                problems.push(format!("{what}: sl.parent levels must be at least 1"));
            }
            FieldFrom::Parent { levels } => {
                let available = enclosing_levels(&rows);
                if *levels > available {
                    problems.push(format!(
                        "{what}: sl.parent(levels={levels}) needs {levels} objects around each row, but select {select:?} passes through {available}"
                    ));
                }
            }
            FieldFrom::Key { name } => problems.extend(row_key_problem(&what, name, fan_out)),
        }
    }
    if fan_out.is_none() && request.key_path.is_some() {
        problems.push(
            "key_path checks each record against the keys its fan_out request sent; a paginate sends none"
                .to_owned(),
        );
    }
    if let Some(key_path) = &request.key_path {
        problems.extend(key_path_problem(&key_path.path));
    }
    if request.cells.is_some() && reshapes(request, &rows) {
        problems.push(format!(
            "cells pivots each row's cells, so it can't combine with sl.parent, sl.key, key_path or select {select:?} through more than one list"
        ));
    }
    problems
}

/// Why `key_path`'s `path` can't be read, if it can't: it is a field path below `$`, such as `$.issueId` or `$.keys[0]`.
fn key_path_problem(text: &str) -> Option<String> {
    match field_path(text) {
        Ok(segments) if !segments.is_empty() => None,
        Ok(_) => Some(format!(
            "key_path {text:?} is not a path such as \"$.issueId\"; use dotted field names after \"$\""
        )),
        Err(reason) => Some(format!("key_path {reason}")),
    }
}

/// Why field `what`'s `sl.key(name)` can't work in `fan_out` (none for a paginate), if it can't: no fan-out, a batch, or a name no level's keys have.
fn row_key_problem(what: &str, name: &str, fan_out: Option<&FanOut>) -> Option<String> {
    let Some(fan_out) = fan_out else {
        return Some(format!(
            "{what}: sl.key reads the key a fan_out request was made for; a paginate has none"
        ));
    };
    if fan_out.batch.is_some() {
        return Some(format!(
            "{what}: sl.key({name:?}) can't tell which of a batch's keys a row is for, as one request carries many; read the key from a response field or sl.parent instead"
        ));
    }
    let provided = provided_keys(&fan_out.over, &fan_out_placeholders(&fan_out.request));
    (!provided.iter().any(|key| key == name)).then(|| {
        format!(
            "{what}: sl.key({name:?}) names no key of this fan-out; its keys are {}",
            provided.join(", ")
        )
    })
}

/// The projection `request`'s `fields` and `keep_record` describe, if any.
pub(crate) fn request_projection(request: &Request) -> Result<Option<Projection>, String> {
    projection(request.fields.as_deref(), request.keep_record.as_deref())
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
    problems.extend(request.cells.as_ref().and_then(cells_problem));
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
                Ok(parsed)
                    if parsed.field.is_none()
                        && parsed.within.is_empty()
                        && !parsed.rows.is_empty() =>
                {
                    None
                }
                Ok(_) | Err(_) => Some(format!(
                    "paginator pointer {pointer:?} is not a path such as \"$.next\" or \"$.meta.next_cursor\""
                )),
            }
        }
        Paginator::Offset { limit: 0, .. } => Some("sl.offset limit must be at least 1".to_owned()),
        Paginator::Offset {
            read_ahead: Some(0),
            ..
        }
        | Paginator::PageNumber {
            read_ahead: Some(0),
            ..
        } => Some("read_ahead must be at least 1 page".to_owned()),
        Paginator::Offset {
            read_ahead: Some(pages),
            ..
        }
        | Paginator::PageNumber {
            read_ahead: Some(pages),
            ..
        } if *pages > MAX_READ_AHEAD => Some(format!(
            "read_ahead {pages} is more than {MAX_READ_AHEAD} pages at once; most APIs refuse that many open requests"
        )),
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
        Ok(parsed)
            if parsed.field.is_none() && parsed.within.is_empty() && named_fields(&parsed.rows) =>
        {
            None
        }
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

/// Whether `segments` are one or more field names, with no list index such as `[0]`.
fn named_fields(segments: &[String]) -> bool {
    !segments.is_empty() && !segments.iter().any(|segment| segment.starts_with('['))
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
    let mut problems = request_problems(&fan_out.request);
    problems.extend(field_problem(&fan_out.request));
    problems.extend(level_problems(fan_out));
    problems.extend(batch_problems(fan_out));
    problems
}

/// The placeholders a fan-out level's keys fill: its path's, then its `json_body`'s.
pub(crate) fn fan_out_placeholders(request: &Request) -> Vec<String> {
    let mut names = placeholders(&request.path);
    for name in request
        .json_body
        .as_ref()
        .map(body_placeholders)
        .unwrap_or_default()
    {
        if !names.contains(&name) {
            names.push(name);
        }
    }
    names
}

/// Where placeholder `name` of `request` is, for its problems: the path, or `json_body`.
fn placeholder_place(request: &Request, name: &str) -> String {
    match placeholders(&request.path)
        .iter()
        .any(|found| found == name)
    {
        true => format!("path {}", request.path),
        false => "json_body".to_owned(),
    }
}

/// Why `fan_out`'s `batch` cannot work, if it is set and cannot: zero, a path placeholder (one request carries many keys), or no `json_body` value that is just a placeholder for the keys' list.
fn batch_problems(fan_out: &FanOut) -> Vec<String> {
    let Some(batch) = fan_out.batch else {
        return Vec::new();
    };
    let request = &fan_out.request;
    let mut problems: Vec<String> = Vec::new();
    problems.extend((batch == 0).then(|| "fan_out batch must be at least 1".to_owned()));
    problems.extend(placeholders(&request.path).first().map(|name| {
        format!(
            "a batched fan-out sends many keys in one request, so its path {} cannot have the placeholder {{{name}}}; put the keys in json_body",
            request.path
        )
    }));
    let Some(body) = &request.json_body else {
        problems.push(
            "fan_out batch= sends each batch's keys as a list in json_body, so it needs json_body with a placeholder, such as {\"ids\": \"{id}\"}".to_owned(),
        );
        return problems;
    };
    let names = body_placeholders(body);
    if names.is_empty() {
        problems.push(
            "fan_out batch= sends each batch's keys as a list in json_body, but json_body has no placeholder, such as {\"ids\": \"{id}\"}".to_owned(),
        );
    }
    let sample: Map<String, Value> = names
        .into_iter()
        .map(|name| (name, Value::Bool(true)))
        .collect();
    problems.extend(filled_body(body, &[&sample], true).err());
    problems
}

/// The problems of a fan-out level apart from its own request's, and of every level above it, whether it loads rows or feeds keys to the level below.
fn level_problems(fan_out: &FanOut) -> Vec<String> {
    let path = &fan_out.request.path;
    let wanted = fan_out_placeholders(&fan_out.request);
    let mut problems: Vec<String> = Vec::new();
    problems.extend(
        (fan_out.concurrency == 0).then(|| "fan_out concurrency must be at least 1".to_owned()),
    );
    let (picker, kind) = match &fan_out.over {
        Over::Values { .. } if wanted.is_empty() => {
            problems.push(format!(
                "fan-out path {path} has no placeholder, so every key would send the same request; add one such as {{id}} to the path or json_body"
            ));
            return problems;
        }
        Over::Values { values } => {
            problems.extend(
                values
                    .iter()
                    .find_map(|value| key_problem(&fan_out.request, &wanted, value)),
            );
            return problems;
        }
        Over::Paginate(over) => {
            problems.extend(request_problems(over));
            problems.extend(picker_problem(over));
            problems.extend(placeholders(&over.path).first().map(|name| {
                format!("fan_out over path {} has the placeholder {{{name}}}; over must be a plain paginate", over.path)
            }));
            (&**over, "a paginate")
        }
        Over::FanOut(inner) => {
            problems.extend(request_problems(&inner.request));
            problems.extend(level_problems(inner));
            problems.extend(inner.batch.map(|_| {
                "batch= is for the fan-out that loads rows; a fan_out used as over sends one request per key".to_owned()
            }));
            problems.extend(picker_problem(&inner.request));
            (&inner.request, "a fan_out")
        }
    };
    let Some(name) = key_name(picker) else {
        problems.push(format!(
            "fan_out over {kind} needs a select naming each key's field, such as \"$.items[*].id\", not {:?}",
            picker.select.clone().unwrap_or_else(|| "$".to_owned())
        ));
        return problems;
    };
    let above = match &fan_out.over {
        Over::FanOut(inner) => provided_keys(&inner.over, &fan_out_placeholders(&inner.request)),
        Over::Values { .. } | Over::Paginate(_) => Vec::new(),
    };
    problems.extend(above.contains(&name).then(|| {
        format!(
            "fan_out over picks {name}, which a level above already gives; name it apart with key="
        )
    }));
    let given: Vec<String> = above.into_iter().chain([name]).collect();
    problems.extend(
        wanted
            .iter()
            .filter(|name| !given.contains(name))
            .map(|name| {
                format!(
                    "fan-out {} needs {{{name}}}, but over gives only {}",
                    placeholder_place(&fan_out.request, name),
                    given.join(", ")
                )
            }),
    );
    problems
}

/// Why `picker`, which feeds keys to the level below, cannot have `fields`, `keep_record`, `cells` or `key_path`, if it has them.
fn picker_problem(picker: &Request) -> Option<String> {
    if picker.key_path.is_some() {
        return Some("fan_out's over picks keys with its select; key_path checks the records of the fan-out that loads rows".to_owned());
    }
    match (picker.fields.is_some() || picker.keep_record.is_some(), picker.cells.is_some()) {
        (true, _) => Some("fan_out's over picks keys with its select; fields and keep_record are for the fan-out's own rows".to_owned()),
        (false, true) => Some("fan_out's over picks keys with its select; cells are for the fan-out's own rows".to_owned()),
        (false, false) => None,
    }
}

/// The name the keys `picker` selects fill: its `key`, else the selected field; `None` when its select names no field.
fn key_name(picker: &Request) -> Option<String> {
    let field = match selector(picker.select.as_deref()) {
        Ok(selected) => selected.field,
        Err(_) => None,
    }?;
    Some(picker.key.clone().unwrap_or(field))
}

/// Whether every object of `objects` has the field `name`.
fn in_every(objects: &[&Map<String, Value>], name: &str) -> bool {
    objects.iter().all(|object| object.contains_key(name))
}

/// The placeholder names every key `over` sends fills, for a level whose path has the placeholders `wanted`.
fn provided_keys(over: &Over, wanted: &[String]) -> Vec<String> {
    match over {
        Over::Values { values } => {
            let objects: Vec<&Map<String, Value>> =
                values.iter().filter_map(Value::as_object).collect();
            match objects.split_first() {
                Some((first, rest)) if objects.len() == values.len() => first
                    .keys()
                    .filter(|name| in_every(rest, name))
                    .cloned()
                    .collect(),
                Some(_) | None => wanted.to_vec(),
            }
        }
        Over::Paginate(over) => key_name(over).into_iter().collect(),
        Over::FanOut(inner) => provided_keys(&inner.over, &fan_out_placeholders(&inner.request))
            .into_iter()
            .chain(key_name(&inner.request))
            .collect(),
    }
}

/// Why the listed key `value` cannot fill the placeholders `wanted` of `request`, if it cannot.
fn key_problem(request: &Request, wanted: &[String], value: &Value) -> Option<String> {
    let path = &request.path;
    let missing = |key: &Map<String, Value>| {
        wanted
            .iter()
            .find(|name| !key.contains_key(name.as_str()))
            .cloned()
    };
    match value {
        Value::Object(key) => missing(key).map(|name| {
            format!(
                "fan-out {} needs {{{name}}}, which the key {value} lacks",
                placeholder_place(request, &name)
            )
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
        HttpJob::FanOut(fan_out) => fan_out_posts(fan_out),
    };
    let http = job_client(posts)?;
    match job {
        HttpJob::Paginate(request) => {
            let calls = request_calls(&request, &connections, &http)?;
            let context = RowContext::default();
            paginate(&calls, (&request, &context), &request.path, &pages).await
        }
        HttpJob::FanOut(fan_out) => {
            run_fan_out(*fan_out, &connections, &http, Sink::Pages(pages)).await
        }
    }
}

/// Whether any level of `fan_out` sends a `POST`.
fn fan_out_posts(fan_out: &FanOut) -> bool {
    fan_out.request.method == Method::Post
        || match &fan_out.over {
            Over::Values { .. } => false,
            Over::Paginate(over) => over.method == Method::Post,
            Over::FanOut(inner) => fan_out_posts(inner),
        }
}

/// A pooled client that follows redirects only within the first request's origin, so connection headers and credentials never reach another host; when the job `posts`, it also stops at 301, 302 and 303, which would resend the request as a `GET` without its body.
pub(crate) fn job_client(posts: bool) -> Result<reqwest::Client, String> {
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
        full_page_seen: std::sync::Arc::default(),
    })
}

/// Page through the fan-out's path for each key, up to `concurrency` at once, while the keys still arrive, sending the pages to `sink`. In a job of more than one fan-out level, errors start with the trail of requests that led to the failing one.
fn run_fan_out<'a>(
    fan_out: FanOut,
    connections: &'a Map<String, Value>,
    http: &'a reqwest::Client,
    sink: Sink,
) -> LevelRun<'a> {
    Box::pin(async move {
        let calls = request_calls(&fan_out.request, connections, http)?;
        let (key_sender, key_receiver) = channel::<FanKey>(fan_out.concurrency * 2);
        let keys = Arc::new(Mutex::new(key_receiver));
        let mut tasks: JoinSet<Result<(), ReadError>> = JoinSet::new();
        let nested = matches!(sink, Sink::Keys(_)) || matches!(fan_out.over, Over::FanOut(_));
        let feed = match fan_out.over.clone() {
            Over::Values { values } => KeyFeed::Values(values),
            Over::Paginate(over) => {
                KeyFeed::Pages(Box::new((request_calls(&over, connections, http)?, *over)))
            }
            Over::FanOut(inner) => {
                KeyFeed::FanOut(Box::new((*inner, connections.clone(), http.clone())))
            }
        };
        let wanted = fan_out_placeholders(&fan_out.request);
        tasks.spawn(feed_keys(feed, wanted, key_sender, nested));
        let request = Arc::new(fan_out.request);
        let batch = fan_out.batch;
        for _ in 0..fan_out.concurrency {
            let (calls, request, keys, sink) =
                (calls.clone(), request.clone(), keys.clone(), sink.clone());
            tasks.spawn(async move {
                loop {
                    let taken = next_keys(&keys, batch).await;
                    if taken.is_empty() {
                        return Ok(());
                    }
                    let shown = shown_keys(&request, &taken, batch.is_some());
                    fan_out_key(&calls, &request, (&taken, batch.is_some()), &sink)
                        .await
                        .map_err(|error| ReadError {
                            message: format!(
                                "{}{}",
                                redacted(&shown, &calls.endpoint.current_secrets()),
                                error.message
                            ),
                            retryable: error.retryable,
                        })?;
                }
            });
        }
        while let Some(finished) = tasks.join_next().await {
            finished.map_err(|error| format!("a fan-out request stopped: {error}"))??;
        }
        Ok(())
    })
}

/// What an error of the request for `keys` starts with: the trail of requests that led to its key, then, when the key goes in the body rather than the URL, its values of the body's placeholders (`order_id=5 → `). A batch, whose keys can come from different requests above, shows only its keys' values, the first few of a large one and their count (`order_id=4, 5, 6 → `).
fn shown_keys(request: &Request, keys: &[FanKey], batched: bool) -> String {
    let names = request
        .json_body
        .as_ref()
        .map(body_placeholders)
        .unwrap_or_default();
    let trail = match (batched, keys.first()) {
        (false, Some(key)) => key.trail.clone(),
        (true, _) | (_, None) => String::new(),
    };
    let values = |name: &String| {
        let listed: Vec<String> = keys
            .iter()
            .filter_map(|key| key.fields.get(name))
            .take(SHOWN_KEYS)
            .map(|value| match value {
                Value::String(text) => text.clone(),
                other => other.to_string(),
            })
            .collect();
        let more = match keys.len() > SHOWN_KEYS {
            true => format!(", … ({} keys)", keys.len()),
            false => String::new(),
        };
        format!("{name}={}{more}", listed.join(", "))
    };
    let body: Vec<String> = names.iter().map(values).collect();
    match body.is_empty() {
        true => trail,
        false => format!("{trail}{} → ", body.join(" ")),
    }
}

/// What the rows of `request` for `keys` take from the request: the key's fields for `sl.key` (none for a batch, which `plan` refuses `sl.key` on), and, with `key_path`, the values of the placeholders the keys filled.
fn row_context(request: &Request, (keys, batched): (&[FanKey], bool)) -> RowContext {
    let names = fan_out_placeholders(request);
    let mut values: Vec<String> = Vec::new();
    for key in keys {
        for name in &names {
            match key.fields.get(name) {
                Some(Value::String(text)) => values.push(text.clone()),
                Some(other) => values.push(other.to_string()),
                None => {}
            }
        }
    }
    let sent = request.key_path.as_ref().map(|_| values);
    RowContext {
        key: match (batched, keys.first()) {
            (false, Some(key)) => key.fields.clone(),
            (true, _) | (_, None) => Map::new(),
        },
        sent,
    }
}

/// A fan-out level's run, boxed so a level can run the level above it.
type LevelRun<'a> = Pin<Box<dyn Future<Output = Result<(), ReadError>> + Send + 'a>>;

/// The next key as they arrive, or with `batch` up to that many (fewer once the keys run out); none when no key is left.
async fn next_keys(keys: &Mutex<Receiver<FanKey>>, batch: Option<usize>) -> Vec<FanKey> {
    let wanted = batch.unwrap_or(1).max(1);
    let mut receiver = keys.lock().await;
    let mut taken: Vec<FanKey> = Vec::new();
    while taken.len() < wanted {
        match receiver.recv().await {
            Some(key) => taken.push(key),
            None => break,
        }
    }
    taken
}

/// Every page of `request` for `keys` (one, or a batch filling `json_body` with their lists when `batched`): out as rows, or picked into keys for the level below, each keeping the key's fields.
async fn fan_out_key(
    calls: &Calls,
    request: &Request,
    (keys, batched): (&[FanKey], bool),
    sink: &Sink,
) -> Result<(), ReadError> {
    let Some(key) = keys.first() else {
        return Ok(());
    };
    let path = filled_path(&request.path, &key.fields)?;
    let context = row_context(request, (keys, batched));
    let filled: Request;
    let request = match &request.json_body {
        Some(body) => {
            let fields: Vec<&Map<String, Value>> = keys.iter().map(|key| &key.fields).collect();
            filled = Request {
                json_body: Some(filled_body(body, &fields, batched)?),
                ..request.clone()
            };
            &filled
        }
        None => request,
    };
    match sink {
        Sink::Pages(pages) => paginate(calls, (request, &context), &path, pages).await,
        Sink::Keys(keys) => {
            let picker = key_picker(request)?;
            let (page_sender, page_receiver) = channel::<HttpPage>(PAGES_AHEAD);
            let shown = path.clone();
            let pager =
                async move { paginate(calls, (request, &context), &path, &page_sender).await };
            let secrets = calls.endpoint.current_secrets();
            let forward = forward_keys(page_receiver, &picker, (key, &shown, Some(&secrets)), keys);
            let (paged, forwarded) = tokio::join!(pager, forward);
            paged.and(forwarded)
        }
    }
}

/// What a key-picking job selects: the rows, the field of each to read, and the name it fills.
fn key_picker(request: &Request) -> Result<(Selector, String, String), String> {
    let selected = selector(request.select.as_deref())?;
    let field = selected.field.clone().unwrap_or_default();
    let name = request.key.clone().unwrap_or_else(|| field.clone());
    Ok((selected, field, name))
}

/// Send each key: a value fills the path's one placeholder, an object fills them by name, and a paginate's or fan-out's selected field is named after itself or its `key`.
async fn feed_keys(
    feed: KeyFeed,
    wanted: Vec<String>,
    keys: Sender<FanKey>,
    nested: bool,
) -> Result<(), ReadError> {
    let single = |value: Value| -> FanKey {
        let fields = match value {
            Value::Object(key) => key,
            other => wanted
                .first()
                .map(|name| (name.clone(), other))
                .into_iter()
                .collect(),
        };
        FanKey {
            fields,
            trail: String::new(),
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
            let picker = key_picker(&over)?;
            let path = over.path.clone();
            let secrets = nested.then(|| calls.endpoint.current_secrets());
            let (page_sender, page_receiver) = channel::<HttpPage>(PAGES_AHEAD);
            let pager = async move {
                let context = RowContext::default();
                paginate(&calls, (&over, &context), &over.path, &page_sender).await
            };
            let root = FanKey::default();
            let forward = forward_keys(
                page_receiver,
                &picker,
                (&root, &path, secrets.as_deref()),
                &keys,
            );
            let (paged, forwarded) = tokio::join!(pager, forward);
            paged.and(forwarded)
        }
        KeyFeed::FanOut(level) => {
            let (inner, connections, http) = *level;
            run_fan_out(inner, &connections, &http, Sink::Keys(keys)).await
        }
    }
}

/// Each page's picked field values as keys named by `picker`, each with `parent`'s fields and, when `secrets` is given, its trail extended by the request at `path` that gave it, with `secrets` (that request's) redacted.
async fn forward_keys(
    mut pages: Receiver<HttpPage>,
    (selected, field, name): &(Selector, String, String),
    (parent, path, secrets): (&FanKey, &str, Option<&[String]>),
    keys: &Sender<FanKey>,
) -> Result<(), ReadError> {
    while let Some(page) = pages.recv().await {
        let document: Value = serde_json::from_slice(&page.bytes)
            .map_err(|error| format!("the fan-out's over response is not JSON: {error}"))?;
        for row in selected_rows(&document, selected) {
            let Some(value) = row.get(field).filter(|value| !value.is_null()) else {
                continue;
            };
            let mut fields = parent.fields.clone();
            fields.insert(name.clone(), value.clone());
            let trail = secrets.map_or_else(String::new, |secrets| {
                trail_step(&parent.trail, (path, name, value), secrets)
            });
            if keys.send(FanKey { fields, trail }).await.is_err() {
                return Ok(());
            }
        }
    }
    Ok(())
}

/// `trail` followed by the request at `path` and the key `name` = `value` it gave, with `secrets` redacted from the step.
fn trail_step(
    trail: &str,
    (path, name, value): (&str, &str, &Value),
    secrets: &[String],
) -> String {
    let shown = match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    };
    format!(
        "{trail}{}",
        redacted(&format!("{path} {name}={shown} → "), secrets)
    )
}
