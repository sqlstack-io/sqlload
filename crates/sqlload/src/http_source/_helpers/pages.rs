//! Paging through one endpoint with its named paginator.

use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use reqwest::Url;
use serde_json::{Map, Value};
use tokio::sync::mpsc::Sender;

use crate::http_source::_helpers::fetch::fetch;
use crate::http_source::_helpers::selector::{pointer_text, selected_node, selector};
use crate::http_source::_helpers::urls::{redacted, request_url};
use crate::http_source::models::{Calls, Fetched, HttpPage, Paginator, Request, Selector};

/// Send every page of `request` at `path` (its placeholders filled) to `pages`, with where its rows are; a page whose selected node is missing or null has no rows and is not sent. Stops quietly once `pages` closes; a page read twice, or a response repeating the previous one, is an error rather than a loop.
pub(crate) async fn paginate(
    calls: &Calls,
    request: &Request,
    path: &str,
    pages: &Sender<HttpPage>,
) -> Result<(), String> {
    let rows = selector(request.select.as_deref())?;
    let rows_path = Some(rows.rows.join(".")).filter(|path| !path.is_empty());
    let first = request_url(&calls.endpoint.base_url, path)?;
    let mut url = with_params(&first, &request.params, &start_params(&request.paginator));
    let mut seen: HashSet<Url> = HashSet::new();
    let mut previous: Option<u64> = None;
    for step in 1_u64.. {
        let shown = redacted(&format!("GET {url}"), &calls.endpoint.current_secrets());
        if !seen.insert(url.clone()) {
            return Err(format!(
                "{shown}: the paginator came back to a page it already read; check its pointer or parameter"
            ));
        }
        let Some(fetched) = fetch(calls, &url).await? else {
            return Ok(());
        };
        let digest = counted(&request.paginator).then(|| digest(&fetched.bytes));
        if digest.is_some() && previous == digest {
            return Err(format!(
                "{shown}: the response repeats the previous page; the API may ignore the paginator's parameter"
            ));
        }
        previous = digest;
        let document = parsed(request, &rows, &fetched.bytes)
            .map_err(|error| format!("{shown}: the response is not JSON: {error}"))?;
        let next = next_url(request, &first, (&fetched, document.as_ref(), step))
            .map_err(|error| format!("{shown}: {error}"))?;
        let has_rows = document.as_ref().is_none_or(|document| {
            selected_node(document, &rows).is_some_and(|node| !node.is_null())
        });
        let page = HttpPage {
            bytes: fetched.bytes,
            path: rows_path.clone(),
            source: shown,
        };
        if has_rows && pages.send(page).await.is_err() {
            return Ok(());
        }
        match next {
            Some(next) => url = next,
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

/// The paginator's parameters on the first request.
fn start_params(paginator: &Paginator) -> Vec<(String, String)> {
    match paginator {
        Paginator::Offset {
            param,
            limit_param,
            limit,
        } => vec![
            (param.clone(), "0".to_owned()),
            (limit_param.clone(), limit.to_string()),
        ],
        Paginator::PageNumber { param, start } => vec![(param.clone(), start.to_string())],
        Paginator::Single
        | Paginator::NextLink { .. }
        | Paginator::LinkHeader
        | Paginator::Cursor { .. } => Vec::new(),
    }
}

/// The page after `fetched` (the `step`th), or `None` at the end; links resolve against the URL the response came from, after any redirect; `document` is the parsed response whenever the paginator reads the body.
fn next_url(
    request: &Request,
    first: &Url,
    (fetched, document, step): (&Fetched, Option<&Value>, u64),
) -> Result<Option<Url>, String> {
    let body = || document.ok_or_else(|| "the paginator needs a JSON response".to_owned());
    let joined = |link: String| {
        fetched
            .url
            .join(&link)
            .map(Some)
            .map_err(|error| format!("next page {link} is not a URL: {error}"))
    };
    let params = &request.params;
    match &request.paginator {
        Paginator::Single => Ok(None),
        Paginator::LinkHeader => fetched.next_link.clone().map_or(Ok(None), joined),
        Paginator::NextLink { pointer } => {
            pointer_text(body()?, &selector(Some(pointer))?).map_or(Ok(None), joined)
        }
        Paginator::Cursor { pointer, param } => {
            Ok(pointer_text(body()?, &selector(Some(pointer))?)
                .map(|token| with_params(first, params, &[(param.clone(), token)])))
        }
        Paginator::Offset {
            param,
            limit_param,
            limit,
        } => {
            let full = page_rows(body()?, request)? >= *limit;
            Ok(full.then(|| {
                with_params(
                    first,
                    params,
                    &[
                        (param.clone(), (step * limit).to_string()),
                        (limit_param.clone(), limit.to_string()),
                    ],
                )
            }))
        }
        Paginator::PageNumber { param, start } => {
            let more = page_rows(body()?, request)? > 0;
            Ok(more.then(|| {
                with_params(
                    first,
                    params,
                    &[(param.clone(), (start + step).to_string())],
                )
            }))
        }
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
