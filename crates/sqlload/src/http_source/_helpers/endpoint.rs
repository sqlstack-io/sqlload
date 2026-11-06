//! A connection's settings resolved into what requests need, and the rate limit its jobs share.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use base64::Engine;
use serde_json::{Map, Value};

use crate::http_source::models::{Auth, ClientSpec, Credential, Endpoint, Limiter};

/// The endpoint of `client`'s connection: its `base_url`, headers and the credential its `auth` names.
pub(crate) fn endpoint(
    client: &ClientSpec,
    connections: &Map<String, Value>,
) -> Result<Endpoint, String> {
    let name = &client.connection;
    let settings = connections.get(name).ok_or_else(|| {
        format!("sl.http uses connection {name}, which is not in sqlload.toml or is a target's connection")
    })?;
    let text = |key: &str| settings.get(key).and_then(Value::as_str).map(str::to_owned);
    let base =
        text("base_url").ok_or_else(|| format!("connection {name} needs base_url for sl.http"))?;
    let base_url = reqwest::Url::parse(&base)
        .map_err(|error| format!("connection {name}: base_url {base} is not a URL: {error}"))?;
    let needed = |key: &str| {
        text(key).filter(|value| !value.is_empty()).ok_or_else(|| {
            format!("connection {name} needs {key} for the sl.http auth this job uses")
        })
    };
    let credential = match &client.auth {
        Auth::None => Credential::None,
        Auth::Bearer => Credential::Header {
            name: "Authorization".to_owned(),
            value: format!("Bearer {}", needed("token")?),
        },
        Auth::ApiKey {
            header: Some(header),
            query: None,
        } => Credential::Header {
            name: header.clone(),
            value: needed("token")?,
        },
        Auth::ApiKey {
            header: None,
            query: Some(query),
        } => Credential::Query {
            name: query.clone(),
            value: needed("token")?,
        },
        Auth::ApiKey { .. } => {
            return Err("sl.api_key takes exactly one of header= or query=".to_owned());
        }
        Auth::Basic => {
            let pair = format!("{}:{}", needed("username")?, needed("password")?);
            Credential::Header {
                name: "Authorization".to_owned(),
                value: format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD.encode(pair)
                ),
            }
        }
    };
    let mut headers = settings
        .get("headers")
        .and_then(Value::as_object)
        .map(text_headers)
        .unwrap_or_default();
    headers.extend(client.headers.clone());
    let secrets: Vec<String> = ["token", "password"]
        .iter()
        .filter_map(|key| text(key))
        .collect();
    Ok(Endpoint {
        base_url,
        headers,
        credential,
        secrets,
    })
}

/// The connection's `headers` that are text.
fn text_headers(headers: &Map<String, Value>) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(key, value)| Some((key.clone(), value.as_str()?.to_owned())))
        .collect()
}

/// The limiter every job of `connection` with the limit `rate` (`10/s`, `600/m`, `1000/h`) shares in this process.
pub(crate) fn shared_limiter(connection: &str, rate: &str) -> Result<Arc<Limiter>, String> {
    static LIMITERS: OnceLock<Mutex<HashMap<String, Arc<Limiter>>>> = OnceLock::new();
    let interval = interval(rate)?;
    let limiters = LIMITERS.get_or_init(Mutex::default);
    let mut limiters = limiters
        .lock()
        .map_err(|_| "the rate limiters' lock is poisoned".to_owned())?;
    let limiter = limiters
        .entry(format!("{connection}\u{0}{rate}"))
        .or_insert_with(|| {
            Arc::new(Limiter {
                interval,
                next: Mutex::new(Instant::now()),
            })
        });
    Ok(limiter.clone())
}

/// The time between requests `rate` allows.
pub(crate) fn interval(rate: &str) -> Result<Duration, String> {
    let invalid =
        || format!("rate_limit {rate:?} is not a rate such as \"10/s\", \"600/m\" or \"1000/h\"");
    let (count, unit) = rate.split_once('/').ok_or_else(invalid)?;
    let count: f64 = count.trim().parse().map_err(|_| invalid())?;
    let seconds = match unit.trim() {
        "s" => 1.0,
        "m" => 60.0,
        "h" => 3_600.0,
        _ => return Err(invalid()),
    };
    match count.is_normal() && count.is_sign_positive() {
        true => Ok(Duration::from_secs_f64(seconds / count)),
        false => Err(invalid()),
    }
}

impl Limiter {
    /// Wait for this request's turn.
    pub(crate) async fn wait(&self) {
        let wait = match self.next.lock() {
            Ok(mut next) => {
                let now = Instant::now();
                let turn = (*next).max(now);
                *next = turn + self.interval;
                turn - now
            }
            Err(_) => Duration::ZERO,
        };
        tokio::time::sleep(wait).await;
    }
}
