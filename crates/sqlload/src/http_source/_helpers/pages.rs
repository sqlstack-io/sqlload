//! Paging through one endpoint with its named paginator.

use reqwest::Url;
use serde_json::{Map, Value};
use tokio::sync::mpsc::Sender;

use crate::http_source::_helpers::fetch::fetch;
use crate::http_source::_helpers::selector::{pointer_text, selected_rows, selector};
use crate::http_source::_helpers::urls::{redacted, request_url};
use crate::http_source::models::{Calls, Fetched, HttpPage, Paginator, Request, Selector};

/// Send every page of `request` at `path` (its placeholders filled) to `pages`, with where its rows are; stops quietly once `pages` closes.
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
    for step in 1_u64.. {
        let Some(fetched) = fetch(calls, &url).await? else {
            return Ok(());
        };
        let next = next_url(request, (&first, &url), (&fetched, &rows, step))?;
        let page = HttpPage {
            bytes: fetched.bytes,
            path: rows_path.clone(),
        };
        if pages.send(page).await.is_err() {
            return Ok(());
        }
        match next {
            Some(next) if next == url => {
                return Err(redacted(
                    &format!(
                        "GET {url}: the paginator points at the same page again; check its pointer or parameter"
                    ),
                    &calls.endpoint.secrets,
                ));
            }
            Some(next) => url = next,
            None => return Ok(()),
        }
    }
    Ok(())
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

/// The page after `fetched` (the `step`th), or `None` at the end.
fn next_url(
    request: &Request,
    (first, current): (&Url, &Url),
    (fetched, rows, step): (&Fetched, &Selector, u64),
) -> Result<Option<Url>, String> {
    let document = || -> Result<Value, String> {
        serde_json::from_slice(&fetched.bytes)
            .map_err(|error| format!("GET {current}: the response is not JSON: {error}"))
    };
    let joined = |link: String| {
        current
            .join(&link)
            .map(Some)
            .map_err(|error| format!("GET {current}: next page {link} is not a URL: {error}"))
    };
    let params = &request.params;
    match &request.paginator {
        Paginator::Single => Ok(None),
        Paginator::LinkHeader => fetched.next_link.clone().map_or(Ok(None), joined),
        Paginator::NextLink { pointer } => {
            pointer_text(&document()?, &selector(Some(pointer))?).map_or(Ok(None), joined)
        }
        Paginator::Cursor { pointer, param } => {
            Ok(pointer_text(&document()?, &selector(Some(pointer))?)
                .map(|token| with_params(first, params, &[(param.clone(), token)])))
        }
        Paginator::Offset {
            param,
            limit_param,
            limit,
        } => {
            let full = selected_rows(&document()?, rows).len() as u64 >= *limit;
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
            let empty = selected_rows(&document()?, rows).is_empty();
            Ok((!empty).then(|| {
                with_params(
                    first,
                    params,
                    &[(param.clone(), (start + step).to_string())],
                )
            }))
        }
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
