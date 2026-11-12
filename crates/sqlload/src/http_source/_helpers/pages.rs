//! Paging through one endpoint with its named paginator.

use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use reqwest::Url;
use serde_json::{Map, Value};
use tokio::sync::mpsc::Sender;

use crate::http_source::_helpers::fetch::fetch;
use crate::http_source::_helpers::job::request_projection;
use crate::http_source::_helpers::selector::{
    pointer_text, pointer_value, selected_node, selector,
};
use crate::http_source::_helpers::urls::{redacted, request_url};
use crate::http_source::models::{
    Calls, Fetched, HttpPage, Location, Method, PageCall, Paginator, Request, Selector,
};
use crate::python_sources::errors::ReadError;

/// Send every page of `request` at `path` (its placeholders filled) to `pages`, with where its rows are; a page whose selected node is missing or null has no rows and is not sent. Stops quietly once `pages` closes; a page read twice, or a response repeating the previous one, is an error rather than a loop.
pub(crate) async fn paginate(
    calls: &Calls,
    request: &Request,
    path: &str,
    pages: &Sender<HttpPage>,
) -> Result<(), ReadError> {
    let rows = selector(request.select.as_deref())?;
    let projection = request_projection(request)?;
    let rows_path = Some(rows.rows.join(".")).filter(|path| !path.is_empty());
    let first = request_url(&calls.endpoint.base_url, path)?;
    let mut call = page_call(request, &first, start_values(&request.paginator))?;
    let mut seen: HashSet<(Url, Option<String>)> = HashSet::new();
    let mut previous: Option<u64> = None;
    for step in 1_u64.. {
        let shown = redacted(
            &format!("{} {}", call.method.as_str(), call.url),
            &calls.endpoint.current_secrets(),
        );
        let body_text = call.body.as_ref().map(Value::to_string);
        if !seen.insert((call.url.clone(), body_text)) {
            return Err(format!(
                "{shown}: the paginator came back to a page it already read; check its pointer or parameter"
            )
            .into());
        }
        let Some(fetched) = fetch(calls, &call).await? else {
            return Ok(());
        };
        let digest = counted(&request.paginator).then(|| digest(&fetched.bytes));
        if digest.is_some() && previous == digest {
            return Err(format!(
                "{shown}: the response repeats the previous page; the API may ignore the paginator's parameter"
            )
            .into());
        }
        previous = digest;
        let document = parsed(request, &rows, &fetched.bytes)
            .map_err(|error| format!("{shown}: the response is not JSON: {error}"))?;
        let next = next_call(
            request,
            (&first, &call),
            (&fetched, document.as_ref(), step),
        )
        .map_err(|error| format!("{shown}: {error}"))?;
        let has_rows = document.as_ref().is_none_or(|document| {
            selected_node(document, &rows).is_some_and(|node| !node.is_null())
        });
        let page = HttpPage {
            bytes: fetched.bytes,
            path: rows_path.clone(),
            source: shown,
            projection: projection.clone(),
        };
        if has_rows && pages.send(page).await.is_err() {
            return Ok(());
        }
        match next {
            Some(next) => call = next,
            None => return Ok(()),
        }
    }
    Ok(())
}

/// The response as JSON when the selector or the paginator reads it, else `None`.
fn parsed(request: &Request, rows: &Selector, bytes: &[u8]) -> Result<Option<Value>, String> {
    let reads_body = match &request.paginator {
        Paginator::Single | Paginator::LinkHeader => !rows.rows.is_empty(),
        Paginator::NextLink { .. }
        | Paginator::Cursor { .. }
        | Paginator::Offset { .. }
        | Paginator::PageNumber { .. } => true,
    };
    match reads_body {
        true => serde_json::from_slice(bytes)
            .map(Some)
            .map_err(|error| error.to_string()),
        false => Ok(None),
    }
}

/// Whether the paginator moves by a counter whatever the response says, so only a repeated body reveals an API ignoring it; the others follow the response, and the seen URLs catch their loops.
fn counted(paginator: &Paginator) -> bool {
    match paginator {
        Paginator::Offset { .. } | Paginator::PageNumber { .. } => true,
        Paginator::Single
        | Paginator::NextLink { .. }
        | Paginator::LinkHeader
        | Paginator::Cursor { .. } => false,
    }
}

fn digest(bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

/// The paginator's values on the first request.
fn start_values(paginator: &Paginator) -> Vec<(String, Value)> {
    match paginator {
        Paginator::Offset {
            param,
            limit_param,
            limit,
            ..
        } => vec![
            (param.clone(), Value::from(0_u64)),
            (limit_param.clone(), Value::from(*limit)),
        ],
        Paginator::PageNumber { param, start, .. } => vec![(param.clone(), Value::from(*start))],
        Paginator::Single
        | Paginator::NextLink { .. }
        | Paginator::LinkHeader
        | Paginator::Cursor { .. } => Vec::new(),
    }
}

/// Where the paginator's values go.
fn location(paginator: &Paginator) -> Location {
    match paginator {
        Paginator::Offset { location, .. }
        | Paginator::PageNumber { location, .. }
        | Paginator::Cursor { location, .. } => *location,
        Paginator::Single | Paginator::NextLink { .. } | Paginator::LinkHeader => Location::Query,
    }
}

/// The request at `url` with the paginator's `values`: in the query after `params`, or at their paths in a fresh copy of `json_body`, which is never changed itself. A `POST` without `json_body` starts from `{}`.
fn page_call(
    request: &Request,
    url: &Url,
    values: Vec<(String, Value)>,
) -> Result<PageCall, String> {
    let (query, in_body) = match location(&request.paginator) {
        Location::Query => (values, Vec::new()),
        Location::Body => (Vec::new(), values),
    };
    let query: Vec<(String, String)> = query
        .into_iter()
        .map(|(name, value)| (name, param_text(&value)))
        .collect();
    let body = match request.method {
        Method::Get => None,
        Method::Post => {
            let mut body = request
                .json_body
                .clone()
                .unwrap_or_else(|| Value::Object(Map::new()));
            for (path, value) in in_body {
                set_at(&mut body, &path, value)?;
            }
            Some(body)
        }
    };
    Ok(PageCall {
        method: request.method,
        url: with_params(url, &request.params, &query),
        body,
    })
}

/// `body` with `value` at `path` (such as `$.page.cursor`), adding objects on the way.
pub(crate) fn set_at(body: &mut Value, path: &str, value: Value) -> Result<(), String> {
    let names = selector(Some(path))?.rows;
    let Some((last, parents)) = names.split_last() else {
        return Err(format!("body path {path:?} names no field"));
    };
    let mut node = body;
    for name in parents {
        node = node
            .as_object_mut()
            .ok_or_else(|| format!("json_body has no object to set {path} in"))?
            .entry(name.clone())
            .or_insert_with(|| Value::Object(Map::new()));
    }
    node.as_object_mut()
        .ok_or_else(|| format!("json_body has no object to set {path} in"))?
        .insert(last.clone(), value);
    Ok(())
}

/// A value as a query parameter: text as itself, anything else as JSON.
fn param_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

/// The request after `fetched` (the `step`th page, sent as `current`), or `None` at the end; links resolve against the URL the response came from, after any redirect, and keep the current body; `document` is the parsed response whenever the paginator reads the body.
fn next_call(
    request: &Request,
    (first, current): (&Url, &PageCall),
    (fetched, document, step): (&Fetched, Option<&Value>, u64),
) -> Result<Option<PageCall>, String> {
    let body = || document.ok_or_else(|| "the paginator needs a JSON response".to_owned());
    let joined = |link: String| {
        fetched
            .url
            .join(&link)
            .map(|url| {
                Some(PageCall {
                    url,
                    ..current.clone()
                })
            })
            .map_err(|error| format!("next page {link} is not a URL: {error}"))
    };
    let next = |values: Vec<(String, Value)>| page_call(request, first, values).map(Some);
    match &request.paginator {
        Paginator::Single => Ok(None),
        Paginator::LinkHeader => fetched.next_link.clone().map_or(Ok(None), joined),
        Paginator::NextLink { pointer } => {
            pointer_text(body()?, &selector(Some(pointer))?).map_or(Ok(None), joined)
        }
        Paginator::Cursor { pointer, param, .. } => {
            match pointer_value(body()?, &selector(Some(pointer))?) {
                Some(token) => next(vec![(param.clone(), token)]),
                None => Ok(None),
            }
        }
        Paginator::Offset {
            param,
            limit_param,
            limit,
            ..
        } => match page_rows(body()?, request)? >= *limit {
            true => next(vec![
                (param.clone(), Value::from(step * limit)),
                (limit_param.clone(), Value::from(*limit)),
            ]),
            false => Ok(None),
        },
        Paginator::PageNumber { param, start, .. } => match page_rows(body()?, request)? > 0 {
            true => next(vec![(param.clone(), Value::from(start + step))]),
            false => Ok(None),
        },
    }
}

/// How many rows a page holds for offset and page-number paging: its selected list's length, 0 when missing or null; anything else is an error, since a counted object would never end the paging.
fn page_rows(document: &Value, request: &Request) -> Result<u64, String> {
    match selected_node(document, &selector(request.select.as_deref())?) {
        Some(Value::Array(items)) => Ok(items.len() as u64),
        Some(Value::Null) | None => Ok(0),
        Some(_) => Err(format!(
            "offset and page-number paging count the rows in a list, but select {:?} is not one; point select at the page's list of rows",
            request.select.clone().unwrap_or_else(|| "$".to_owned())
        )),
    }
}

/// `url` with `params` (nulls left out) and then `extra` appended to its query.
fn with_params(url: &Url, params: &Map<String, Value>, extra: &[(String, String)]) -> Url {
    let mut url = url.clone();
    let pairs = params.iter().filter_map(|(name, value)| match value {
        Value::Null => None,
        Value::String(text) => Some((name.clone(), text.clone())),
        other => Some((name.clone(), other.to_string())),
    });
    let pairs: Vec<(String, String)> = pairs.chain(extra.iter().cloned()).collect();
    if !pairs.is_empty() {
        url.query_pairs_mut().extend_pairs(pairs);
    }
    url
}
