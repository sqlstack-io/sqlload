//! Paging through one endpoint with its named paginator.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::Ordering;

use std::collections::VecDeque;

use reqwest::Url;
use serde_json::{Map, Value};
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

use crate::http_source::_helpers::cells::pivoted_rows;
use crate::http_source::_helpers::fetch::fetch;
use crate::http_source::_helpers::job::request_projection;
use crate::http_source::_helpers::reshaped::{reshapes, wrapped_projection, wrapped_rows};
use crate::http_source::_helpers::selector::{
    missing_path, pointer_text, pointer_value, selected_node, selector,
};
use crate::http_source::_helpers::urls::{redacted, request_url};
use crate::http_source::constants::REPORTED_ERROR_CHARS;
use crate::http_source::models::{
    AheadPage, Calls, CellNames, Fetched, HttpPage, Location, Method, PageCall, Paginator, Request,
    RowContext, SelectMissing, Selector,
};
use crate::python_sources::errors::ReadError;

/// Send every page of `request` at `path` (its placeholders filled) to `pages`, with where its rows are; a page whose selected node is null has no rows and is not sent, and one missing it fails, naming what the response holds, unless the request reads a missing node as no rows (`select_missing`). An offset run that would end after a first page shorter than its limit asks for the next offset once, unless the job has shown the API sends pages of more rows, and fails when rows come back, as the API then caps its pages lower than the limit. A page-number run with `page_size` ends at a short page in the same way, and where it can't yet tell asks for the next page, paging on to an empty page when rows come back (an API capping its pages lower). With `read_ahead`, offset and page-number pages after a full first page are requested two at a time, then four, eight and so on up to that many (fewer under a memory budget: only as many as pages the size of the largest yet fit in a quarter of each table's share), each through the shared rate limit and retries in a task of its own (so it keeps reading while the pages' receiver holds this loop up), and handled strictly in page order, so the run still ends at the first short page, dropping the requests past it. Stops quietly once `pages` closes. A next position (cursor, link or count) already requested for this request fails before it is requested again, naming the value and the page that returned it, and a counted page repeating the previous response fails, rather than loop. With `error_path`, an error a page reports there fails the table before any of that page's rows load.
pub(crate) async fn paginate(
    calls: &Calls,
    (request, context): (&Request, &RowContext),
    path: &str,
    pages: &Sender<HttpPage>,
) -> Result<(), ReadError> {
    let rows = selector(request.select.as_deref())?;
    let reshaped = reshapes(request, &rows);
    let own = request_projection(request)?;
    let projection = match reshaped {
        true => wrapped_projection(request)?,
        false => own.clone(),
    };
    let rows_path = Some(rows.rows.join(".")).filter(|path| !path.is_empty());
    let first = request_url(&calls.endpoint.base_url, path)?;
    let mut call = page_call(request, &first, start_values(&request.paginator))?;
    let mut seen: HashMap<(Url, Option<String>), u64> = HashMap::from([(seen_key(&call), 1)]);
    let mut previous: Option<u64> = None;
    let mut names: Option<CellNames> = None;
    let ahead = read_ahead(&request.paginator);
    let mut in_flight: VecDeque<AbortOnDropHandle<AheadPage>> = VecDeque::new();
    let stop = CancellationToken::new();
    let _stopping = stop.clone().drop_guard();
    let mut queued: u64 = 1;
    let mut largest: u64 = 0;
    let (mut ramp, mut ramped): (u64, u64) = (1, 1);
    let mut checking: Option<u64> = None;
    for step in 1_u64.. {
        let shown = redacted(
            &format!("{} {}", call.method.as_str(), call.url),
            &calls.endpoint.current_secrets(),
        );
        let fetched = match (step, ahead) {
            (1, _) | (_, 1) => fetch(calls, &call).await?,
            _ => {
                let fitting = match (calls.source_part, largest) {
                    (Some(part), largest) if largest > 0 => (part / largest).max(1),
                    _ => ahead,
                };
                let grows = step > ramped;
                if grows {
                    ramp = ramp.saturating_mul(2).min(ahead);
                }
                while queued < step + ramp.min(fitting) - 1 {
                    queued += 1;
                    let ahead_call = counted_call(request, &first, queued)?;
                    let ahead_calls = Calls {
                        stop: stop.clone(),
                        ..calls.clone()
                    };
                    in_flight.push_back(AbortOnDropHandle::new(tokio::spawn(async move {
                        let fetched = fetch(&ahead_calls, &ahead_call).await;
                        AheadPage {
                            call: ahead_call,
                            fetched,
                        }
                    })));
                }
                if grows {
                    ramped = queued;
                }
                let next =
                    match in_flight.pop_front() {
                        Some(handle) => Some(handle.await.map_err(|error| {
                            format!("{shown}: a page read ahead stopped: {error}")
                        })?),
                        None => None,
                    };
                match next {
                    Some(ahead) if ahead.call == call => ahead.fetched?,
                    Some(_) | None => {
                        return Err(format!(
                            "{shown}: the pages read ahead fell out of order with the paginator"
                        )
                        .into());
                    }
                }
            }
        };
        let Some(fetched) = fetched else {
            release(in_flight);
            return Ok(());
        };
        largest = largest.max(fetched.bytes.len() as u64);
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
        if let Some(document) = &document {
            reported_error(request, document, (&shown, calls))?;
            selected_or_allowed(request, (&rows, document), &shown)?;
        }
        let next = next_call(
            request,
            (&first, &call),
            (&fetched, document.as_ref(), step),
            calls.last_page_below.load(Ordering::Relaxed),
        )
        .map_err(|error| format!("{shown}: {error}"))?;
        learn_page_size(
            calls,
            request,
            (document.as_ref(), next.is_some()),
            (&mut checking, &shown),
        )
        .map_err(|error| format!("{shown}: {error}"))?;
        if let Some((next, position)) = &next
            && let Some(earlier) = seen.insert(seen_key(next), step + 1)
        {
            let position = redacted(position, &calls.endpoint.current_secrets());
            return Err(format!(
                "{shown}: {position} repeated after page {step}, so paging would read page {earlier} again; check the paginator's pointer"
            )
            .into());
        }
        if let (1, None, Some(document)) = (step, &next, &document) {
            capped_pages(calls, request, (&first, document), &shown).await?;
        }
        let has_rows = document.as_ref().is_none_or(|document| {
            selected_node(document, &rows).is_some_and(|node| !node.is_null())
        });
        let (bytes, path) = match (&request.cells, document.as_ref()) {
            (Some(cells), Some(document)) => {
                let (text, used) = pivoted_rows(document, (&rows, cells), names.take())
                    .map_err(|error| format!("{shown}: cells: {error}"))?;
                names = used;
                (text.into_bytes(), None)
            }
            (None, Some(document)) if reshaped => (
                wrapped_rows(
                    (document, &fetched.bytes),
                    &rows,
                    (request, own.as_ref()),
                    context,
                )
                .map_err(|error| format!("{shown}: {error}"))?
                .into_bytes(),
                None,
            ),
            (Some(_), None) | (None, _) => (fetched.bytes, rows_path.clone()),
        };
        let page = HttpPage {
            bytes,
            path,
            source: shown,
            projection: projection.clone(),
            number: step,
            key: context.key.clone(),
        };
        if has_rows && pages.send(page).await.is_err() {
            return Ok(());
        }
        match next {
            Some((next, _)) => call = next,
            None => {
                release(in_flight);
                return Ok(());
            }
        }
    }
    Ok(())
}

/// What this page (`document`, `continues` when paging goes on past it) shows about the API's page size, kept in [`Calls::last_page_below`] for every key of the job: a full page shows the paginator's whole page; the page after a short one (`checking`, that page's rows) shows those rows were all the API sends at once, whether it is empty (the short page was the last) or holds more, which shows a cap and is warned about the first time. A short page-number page that paging goes on past is checked by the next.
fn learn_page_size(
    calls: &Calls,
    request: &Request,
    (document, continues): (Option<&Value>, bool),
    (checking, shown): (&mut Option<u64>, &str),
) -> Result<(), String> {
    let full = match &request.paginator {
        Paginator::Offset { limit, .. } => Some(*limit),
        Paginator::PageNumber { page_size, .. } => *page_size,
        Paginator::Single
        | Paginator::NextLink { .. }
        | Paginator::LinkHeader
        | Paginator::Cursor { .. } => None,
    };
    let (Some(full), Some(document)) = (full, document) else {
        return Ok(());
    };
    let read = page_rows(document, request)?;
    if let Some(short) = checking.take() {
        let before = calls.last_page_below.fetch_max(short, Ordering::Relaxed);
        if read > 0 && before < short {
            tracing::warn!(
                "{shown}: the page before held {short} rows, fewer than page_size {full}, but more followed, so the API caps its pages at {short} rows; set page_size={short} to end each run at its last page"
            );
        }
    }
    match (read >= full, read > 0 && continues) {
        (true, _) => {
            calls.last_page_below.fetch_max(full, Ordering::Relaxed);
        }
        (false, true) => *checking = Some(read),
        (false, false) => {}
    }
    Ok(())
}

/// Let the pages still read ahead when paging ends finish their requests, so their connections go back to the pool, instead of aborting them with the paging, as a failure or a closed `pages` does; the paging's stop token keeps them from waiting for a turn or retrying.
fn release(in_flight: VecDeque<AbortOnDropHandle<AheadPage>>) {
    for handle in in_flight {
        drop(handle.detach());
    }
}

/// What identifies a request among those a paginator sent: its URL and body.
fn seen_key(call: &PageCall) -> (Url, Option<String>) {
    (call.url.clone(), call.body.as_ref().map(Value::to_string))
}

/// Fails when the response `document` to `shown` holds an error at the request's `error_path`: any value there but null, false, `""`, `[]` or `{}`, shown as text (a string as itself, else as JSON), its secrets hidden, cut short.
fn reported_error(
    request: &Request,
    document: &Value,
    (shown, calls): (&str, &Calls),
) -> Result<(), ReadError> {
    let Some(path) = &request.error_path else {
        return Ok(());
    };
    let text = match selected_node(document, &selector(Some(path))?) {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Null | Value::Bool(false)) | None => String::new(),
        Some(Value::Array(items)) if items.is_empty() => String::new(),
        Some(Value::Object(fields)) if fields.is_empty() => String::new(),
        Some(other) => other.to_string(),
    };
    if text.is_empty() {
        return Ok(());
    }
    let text: String = redacted(&text, &calls.endpoint.current_secrets())
        .chars()
        .take(REPORTED_ERROR_CHARS)
        .collect();
    Err(format!("{shown}: the API reported an error at {path}: {text}").into())
}

/// For offset paging whose first page (`document`, from the request `shown`) held some rows but fewer than its limit: the page at the next offset, which an API capping its pages below the limit fills, so rows there fail with the page size to set. An empty or missing page is the real end, and so are the first page's rows again, from an API that rounds the offset down to a page boundary. Skipped when the job has already shown the API sends pages of more rows than this one held (see [`Calls::last_page_below`]); an empty next page shows it for later keys.
async fn capped_pages(
    calls: &Calls,
    request: &Request,
    (first, document): (&Url, &Value),
    shown: &str,
) -> Result<(), ReadError> {
    let Paginator::Offset {
        param,
        limit_param,
        limit,
        ..
    } = &request.paginator
    else {
        return Ok(());
    };
    let read = page_rows(document, request).map_err(|error| format!("{shown}: {error}"))?;
    if read == 0 || read >= *limit || read < calls.last_page_below.load(Ordering::Relaxed) {
        return Ok(());
    }
    let probe = page_call(
        request,
        first,
        vec![
            (param.clone(), Value::from(read)),
            (limit_param.clone(), Value::from(*limit)),
        ],
    )?;
    let Some(fetched) = fetch(calls, &probe).await? else {
        return Ok(());
    };
    let probed = redacted(
        &format!("{} {}", probe.method.as_str(), probe.url),
        &calls.endpoint.current_secrets(),
    );
    let rows = selector(request.select.as_deref())?;
    let more = match parsed(request, &rows, &fetched.bytes)
        .map_err(|error| format!("{probed}: the response is not JSON: {error}"))?
    {
        Some(next) if selected_node(&next, &rows) == selected_node(document, &rows) => 0,
        Some(next) => {
            reported_error(request, &next, (&probed, calls))?;
            selected_or_allowed(request, (&rows, &next), &probed)?;
            page_rows(&next, request).map_err(|error| format!("{probed}: {error}"))?
        }
        None => 0,
    };
    match more {
        0 => {
            calls.last_page_below.fetch_max(read, Ordering::Relaxed);
            Ok(())
        }
        _ => Err(format!(
            "{shown}: the first page held {read} rows, fewer than limit {limit}, but the API returned {more} more at offset {read}, so it caps its pages at {read} rows; set limit={read}"
        )
        .into()),
    }
}

/// Fails when the response `document` to `shown` lacks the `rows` path, naming what it holds, unless the request reads a missing path as no rows (`select_missing`).
fn selected_or_allowed(
    request: &Request,
    (rows, document): (&Selector, &Value),
    shown: &str,
) -> Result<(), ReadError> {
    match request.select_missing == SelectMissing::Fail && missing_path(document, rows) {
        true => Err(format!(
            "{shown}: select {:?} is not in the response, which holds {}; an API can answer an error with a 200 status, so check the response. For an API that leaves an empty list out, set select_missing=\"empty\" to read such a page as no rows",
            request.select.clone().unwrap_or_default(),
            top_level(document)
        )
        .into()),
        false => Ok(()),
    }
}

/// What a response holds at its top level, for an error: its keys, or the kind of value it is.
fn top_level(document: &Value) -> String {
    match document {
        Value::Object(fields) if fields.is_empty() => "an empty object".to_owned(),
        Value::Object(fields) => {
            let keys: Vec<&str> = fields.keys().map(String::as_str).collect();
            format!("the keys {}", keys.join(", "))
        }
        Value::Array(_) => "a list".to_owned(),
        Value::String(_) => "text".to_owned(),
        Value::Number(_) => "a number".to_owned(),
        Value::Bool(_) => "true or false".to_owned(),
        Value::Null => "null".to_owned(),
    }
}

/// The response as JSON when the selector, the paginator, the cells or `error_path` read it, else `None`.
fn parsed(request: &Request, rows: &Selector, bytes: &[u8]) -> Result<Option<Value>, String> {
    let reads_body = match &request.paginator {
        Paginator::Single | Paginator::LinkHeader => !rows.rows.is_empty(),
        Paginator::NextLink { .. }
        | Paginator::Cursor { .. }
        | Paginator::Offset { .. }
        | Paginator::PageNumber { .. } => true,
    } || request.cells.is_some()
        || request.error_path.is_some()
        || reshapes(request, rows);
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

/// How many pages an offset or page-number paginator keeps in flight: its `read_ahead`, else 1, as every other paginator reads one page at a time.
fn read_ahead(paginator: &Paginator) -> u64 {
    match paginator {
        Paginator::Offset { read_ahead, .. } | Paginator::PageNumber { read_ahead, .. } => {
            read_ahead.map_or(1, |pages| pages.max(1))
        }
        Paginator::Single
        | Paginator::NextLink { .. }
        | Paginator::LinkHeader
        | Paginator::Cursor { .. } => 1,
    }
}

/// The request for the `step`th page of an offset or page-number paginator, which `next_call` reaches from the page before it: offset `(step - 1) * limit`, or page `start + step - 1`.
fn counted_call(request: &Request, first: &Url, step: u64) -> Result<PageCall, String> {
    let values = match &request.paginator {
        Paginator::Offset {
            param,
            limit_param,
            limit,
            ..
        } => vec![
            (param.clone(), Value::from((step - 1) * limit)),
            (limit_param.clone(), Value::from(*limit)),
        ],
        Paginator::PageNumber { start, .. } => {
            page_number_values(&request.paginator, start + step - 1)
        }
        Paginator::Single
        | Paginator::NextLink { .. }
        | Paginator::LinkHeader
        | Paginator::Cursor { .. } => start_values(&request.paginator),
    };
    page_call(request, first, values)
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
        Paginator::PageNumber { start, .. } => page_number_values(paginator, *start),
        Paginator::Single
        | Paginator::NextLink { .. }
        | Paginator::LinkHeader
        | Paginator::Cursor { .. } => Vec::new(),
    }
}

/// A page-number paginator's values for page `page`: its number, then its `page_size` where it is sent.
fn page_number_values(paginator: &Paginator, page: u64) -> Vec<(String, Value)> {
    let Paginator::PageNumber {
        param,
        page_size,
        page_size_param,
        ..
    } = paginator
    else {
        return Vec::new();
    };
    let mut values = vec![(param.clone(), Value::from(page))];
    if let (Some(size), Some(name)) = (page_size, page_size_param) {
        values.push((name.clone(), Value::from(*size)));
    }
    values
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

/// The request after `fetched` (the `step`th page, sent as `current`) with the position it goes to, such as `cursor abc`, or `None` at the end; links resolve against the URL the response came from, after any redirect, and keep the current body; `document` is the parsed response whenever the paginator reads the body. A page-number page with `page_size` ends the run when it is empty, or short and below `last_page_below` (see [`Calls::last_page_below`]).
fn next_call(
    request: &Request,
    (first, current): (&Url, &PageCall),
    (fetched, document, step): (&Fetched, Option<&Value>, u64),
    last_page_below: u64,
) -> Result<Option<(PageCall, String)>, String> {
    let body = || document.ok_or_else(|| "the paginator needs a JSON response".to_owned());
    let joined = |link: String, what: &str| {
        fetched
            .url
            .join(&link)
            .map(|url| {
                Some((
                    PageCall {
                        url,
                        ..current.clone()
                    },
                    format!("{what} {link}"),
                ))
            })
            .map_err(|error| format!("next page {link} is not a URL: {error}"))
    };
    let next = |values: Vec<(String, Value)>, position: String| {
        page_call(request, first, values).map(|call| Some((call, position)))
    };
    match &request.paginator {
        Paginator::Single => Ok(None),
        Paginator::LinkHeader => fetched
            .next_link
            .clone()
            .map_or(Ok(None), |link| joined(link, "Link header next")),
        Paginator::NextLink { pointer } => pointer_text(body()?, &selector(Some(pointer))?)
            .map_or(Ok(None), |link| joined(link, "next link")),
        Paginator::Cursor { pointer, param, .. } => {
            match pointer_value(body()?, &selector(Some(pointer))?) {
                Some(token) => {
                    let position = format!("cursor {}", param_text(&token));
                    next(vec![(param.clone(), token)], position)
                }
                None => Ok(None),
            }
        }
        Paginator::Offset {
            param,
            limit_param,
            limit,
            ..
        } => match page_rows(body()?, request)? >= *limit {
            true => next(
                vec![
                    (param.clone(), Value::from(step * limit)),
                    (limit_param.clone(), Value::from(*limit)),
                ],
                format!("offset {}", step * limit),
            ),
            false => Ok(None),
        },
        Paginator::PageNumber {
            start, page_size, ..
        } => {
            let read = page_rows(body()?, request)?;
            let more = match page_size {
                None => read > 0,
                Some(size) => read >= *size || (read > 0 && read >= last_page_below),
            };
            match more {
                true => next(
                    page_number_values(&request.paginator, start + step),
                    format!("page number {}", start + step),
                ),
                false => Ok(None),
            }
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
