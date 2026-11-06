//! Checking a job before any request, and running it: one endpoint, or a fan-out over keys.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::{Map, Value};
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::task::JoinSet;

use crate::http_source::_helpers::endpoint::{endpoint, interval, shared_limiter};
use crate::http_source::_helpers::pages::paginate;
use crate::http_source::_helpers::selector::{selected_rows, selector};
use crate::http_source::_helpers::urls::{filled_path, placeholders};
use crate::http_source::constants::{JOB_THREADS, PAGES_AHEAD};
use crate::http_source::models::{
    Calls, FanOut, HttpJob, HttpPage, KeyFeed, Over, Request, Selector, StatusAction,
};

/// Every problem the job's definition has, found before any request.
pub(crate) fn job_problems(job: &HttpJob) -> Vec<String> {
    match job {
        HttpJob::Paginate(request) => {
            let mut problems = request_problems(request);
            problems.extend(placeholders(&request.path).first().map(|name| {
                format!(
                    "paginate path {} has the placeholder {{{name}}}; use fan_out to fill it",
                    request.path
                )
            }));
            problems
        }
        HttpJob::FanOut(fan_out) => fan_out_problems(fan_out),
    }
}

fn request_problems(request: &Request) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    problems.extend(selector(request.select.as_deref()).err());
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

fn fan_out_problems(fan_out: &FanOut) -> Vec<String> {
    let path = &fan_out.request.path;
    let wanted = placeholders(path);
    let mut problems = request_problems(&fan_out.request);
    problems.extend(
        (fan_out.concurrency == 0).then(|| "fan_out concurrency must be at least 1".to_owned()),
    );
    match &fan_out.over {
        Over::Paginate(over) => {
            problems.extend(request_problems(over));
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
) -> Result<(), String> {
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
        Err(error) => Err(format!("the sl.http job stopped: {error}")),
    }
}

/// Run `job`, sending every page to `pages`.
async fn run_job(
    job: HttpJob,
    connections: Map<String, Value>,
    pages: Sender<HttpPage>,
) -> Result<(), String> {
    let http = crate::http::main::http_client::build_http_client(&Default::default())
        .map_err(|error| format!("cannot start the HTTP client: {error}"))?;
    match job {
        HttpJob::Paginate(request) => {
            let calls = request_calls(&request, &connections, &http)?;
            paginate(&calls, &request, &request.path, &pages).await
        }
        HttpJob::FanOut(fan_out) => run_fan_out(*fan_out, &connections, &http, pages).await,
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
) -> Result<(), String> {
    let calls = request_calls(&fan_out.request, connections, http)?;
    let (key_sender, key_receiver) = channel::<Map<String, Value>>(fan_out.concurrency * 2);
    let keys = Arc::new(Mutex::new(key_receiver));
    let mut tasks: JoinSet<Result<(), String>> = JoinSet::new();
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
) -> Result<(), String> {
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
) -> Result<(), String> {
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
