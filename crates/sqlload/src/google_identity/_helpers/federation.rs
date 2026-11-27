//! Workload identity federation: another cloud's token, from a file, a URL, a command or AWS, exchanged at Google STS for a Google access token.

use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::auth::models::Secret;
use crate::google_identity::_helpers::exchange::{access_token, answer, excerpt};
use crate::google_identity::constants::{
    ACCESS_TOKEN_TYPE, ALLOW_EXECUTABLES_VARIABLE, CLOUD_PLATFORM_SCOPE, TOKEN_EXCHANGE_GRANT,
};
use crate::google_identity::models::{AwsKeys, AwsSource, ExternalAccount, SubjectSource};
use crate::http::errors::ApiError;

/// A Google access token for `account`: its subject token, exchanged at STS.
pub(crate) async fn federated_token(
    http: &reqwest::Client,
    account: &ExternalAccount,
    now_ms: i64,
) -> Result<(Secret, i64), ApiError> {
    let subject = subject_token(http, account, now_ms).await?;
    let step = format!(
        "Google STS's token exchange, for workload identity provider {}",
        account.audience
    );
    let request = http.post(&account.token_url).form(&[
        ("grant_type", TOKEN_EXCHANGE_GRANT),
        ("audience", account.audience.as_str()),
        ("scope", CLOUD_PLATFORM_SCOPE),
        ("requested_token_type", ACCESS_TOKEN_TYPE),
        ("subject_token", subject.expose()),
        ("subject_token_type", account.subject_token_type.as_str()),
    ]);
    access_token(
        &answer(request, &step).await?,
        ("access_token", &step),
        now_ms,
    )
}

/// The subject token `account`'s credential source gives now.
async fn subject_token(
    http: &reqwest::Client,
    account: &ExternalAccount,
    now_ms: i64,
) -> Result<Secret, ApiError> {
    match &account.source {
        SubjectSource::File { path, field } => {
            let text = std::fs::read_to_string(path).map_err(|error| {
                ApiError::Auth(format!(
                    "reading the subject token file {path} of the workload identity credentials: {error}"
                ))
            })?;
            token_in(
                &text,
                field.as_deref(),
                &format!("the subject token file {path}"),
            )
        }
        SubjectSource::Url {
            url,
            headers,
            field,
        } => {
            let step = format!("fetching the subject token from {url}");
            let mut request = http.get(url);
            for (name, value) in headers {
                request = request.header(name, value);
            }
            let response = request
                .send()
                .await
                .map_err(|error| ApiError::Transport(format!("{step}: {error}")))?;
            let status = response.status();
            let text = response
                .text()
                .await
                .map_err(|error| ApiError::Transport(format!("{step}: {error}")))?;
            if !status.is_success() {
                return Err(ApiError::Auth(format!(
                    "{step} refused (HTTP {}): {}",
                    status.as_u16(),
                    excerpt(&text)
                )));
            }
            token_in(&text, field.as_deref(), &step)
        }
        SubjectSource::Executable {
            command,
            timeout_ms,
            output_file,
        } => {
            executable_token(
                account,
                (command, *timeout_ms, output_file.as_deref()),
                now_ms,
            )
            .await
        }
        SubjectSource::Aws(source) => {
            let (region, keys) = aws_identity(http, source).await?;
            aws_subject_token(
                &keys,
                (&region, &source.verification_url),
                &account.audience,
                now_ms,
            )
        }
    }
}

/// The token in `text`: all of it, or the `field` of its JSON.
fn token_in(text: &str, field: Option<&str>, what: &str) -> Result<Secret, ApiError> {
    let token = match field {
        None => text.trim().to_owned(),
        Some(field) => {
            let parsed: Value = serde_json::from_str(text).map_err(|error| {
                ApiError::Auth(format!(
                    "{what} is not JSON, so holds no field {field}: {error}"
                ))
            })?;
            parsed[field]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| ApiError::Auth(format!("{what} holds no JSON field {field}")))?
        }
    };
    match token.is_empty() {
        true => Err(ApiError::Auth(format!("{what} holds no subject token"))),
        false => Ok(Secret(token)),
    }
}

/// The subject token a credentials file's command prints, as Google's executable-sourced credentials define: allowed only with `GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES=1`, read from its output file while that holds an unexpired answer, and refused when the command reports a failure.
async fn executable_token(
    account: &ExternalAccount,
    (command, timeout_ms, output_file): (&str, u64, Option<&str>),
    now_ms: i64,
) -> Result<Secret, ApiError> {
    if std::env::var(ALLOW_EXECUTABLES_VARIABLE).as_deref() != Ok("1") {
        return Err(ApiError::Auth(format!(
            "the workload identity credentials run a command for their subject token, which needs {ALLOW_EXECUTABLES_VARIABLE}=1 in the environment"
        )));
    }
    if let Some(path) = output_file
        && let Ok(text) = std::fs::read_to_string(path)
        && let Ok(token) = executable_answer(&text, now_ms, &format!("the output file {path}"))
    {
        return Ok(token);
    }
    let mut words = command.split_whitespace();
    let program = words.next().ok_or_else(|| {
        ApiError::Auth("the workload identity credentials' executable command is empty".to_owned())
    })?;
    let mut process = tokio::process::Command::new(program);
    process
        .args(words)
        .env("GOOGLE_EXTERNAL_ACCOUNT_AUDIENCE", &account.audience)
        .env(
            "GOOGLE_EXTERNAL_ACCOUNT_TOKEN_TYPE",
            &account.subject_token_type,
        )
        .env("GOOGLE_EXTERNAL_ACCOUNT_INTERACTIVE", "0")
        .kill_on_drop(true);
    if let Some(path) = output_file {
        process.env("GOOGLE_EXTERNAL_ACCOUNT_OUTPUT_FILE", path);
    }
    let step = format!("running {program} for the subject token");
    let output = tokio::time::timeout(
        std::time::Duration::from_millis(timeout_ms),
        process.output(),
    )
    .await
    .map_err(|_| ApiError::Auth(format!("{step}: it ran longer than {timeout_ms} ms")))?
    .map_err(|error| ApiError::Auth(format!("{step}: {error}")))?;
    let text = String::from_utf8_lossy(&output.stdout);
    if !output.status.success() {
        return Err(ApiError::Auth(format!(
            "{step}: it exited with {}: {}",
            output.status,
            excerpt(&String::from_utf8_lossy(&output.stderr))
        )));
    }
    executable_answer(&text, now_ms, &step)
}

/// The token in an executable's JSON answer, unless it reports a failure or has expired.
fn executable_answer(text: &str, now_ms: i64, what: &str) -> Result<Secret, ApiError> {
    let parsed: Value = serde_json::from_str(text)
        .map_err(|error| ApiError::Auth(format!("{what} printed no JSON answer: {error}")))?;
    if parsed["success"].as_bool() != Some(true) {
        return Err(ApiError::Auth(format!(
            "{what} reported a failure: {} {}",
            parsed["code"].as_str().unwrap_or_default(),
            parsed["message"].as_str().unwrap_or_default()
        )));
    }
    if let Some(expires) = parsed["expiration_time"].as_i64()
        && expires.saturating_mul(1000) <= now_ms
    {
        return Err(ApiError::Auth(format!(
            "{what} gave a token that has expired"
        )));
    }
    let token = parsed["id_token"]
        .as_str()
        .or_else(|| parsed["saml_response"].as_str())
        .filter(|token| !token.is_empty())
        .ok_or_else(|| ApiError::Auth(format!("{what} gave no id_token or saml_response")))?;
    Ok(Secret(token.to_owned()))
}

/// The AWS region and keys to sign with: those the environment set when the credentials were found, else the instance metadata service the credentials file names, with an IMDSv2 session when it names one.
async fn aws_identity(
    http: &reqwest::Client,
    source: &AwsSource,
) -> Result<(String, AwsKeys), ApiError> {
    let needs_metadata = source.region.is_none() || source.keys.is_none();
    let session = match (&source.session_url, needs_metadata) {
        (Some(url), true) => {
            let step = "AWS's instance metadata service, for an IMDSv2 session";
            let response = http
                .put(url)
                .header("X-aws-ec2-metadata-token-ttl-seconds", "300")
                .send()
                .await
                .map_err(|error| ApiError::Transport(format!("{step}: {error}")))?;
            Some(metadata_text(response, step).await?)
        }
        _ => None,
    };
    let get = |url: &str| {
        let mut request = http.get(url);
        if let Some(session) = &session {
            request = request.header("X-aws-ec2-metadata-token", session);
        }
        request
    };
    let region = match &source.region {
        Some(region) => region.clone(),
        None => {
            let url = source.region_url.as_deref().ok_or_else(|| {
                ApiError::Auth("no AWS region: set AWS_REGION, or run on an EC2 instance whose credentials file names a region_url".to_owned())
            })?;
            let step = "AWS's instance metadata service, for the region";
            let zone = metadata_text(sent(get(url), step).await?, step).await?;
            let mut region = zone.trim().to_owned();
            region.pop();
            region
        }
    };
    if let Some(keys) = &source.keys {
        return Ok((region, keys.clone()));
    }
    let url = source.url.as_deref().ok_or_else(|| {
        ApiError::Auth("no AWS keys: set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or run on an EC2 instance whose credentials file names its metadata url".to_owned())
    })?;
    let step = "AWS's instance metadata service, for the instance role";
    let role = metadata_text(sent(get(url), step).await?, step).await?;
    let role = role.lines().next().unwrap_or_default().trim();
    let step = "AWS's instance metadata service, for the instance role's keys";
    let text = metadata_text(
        sent(get(&format!("{}/{role}", url.trim_end_matches('/'))), step).await?,
        step,
    )
    .await?;
    let keys: Value = serde_json::from_str(&text)
        .map_err(|error| ApiError::Protocol(format!("{step}: {error}")))?;
    let field = |name: &str| {
        keys[name]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| ApiError::Protocol(format!("{step}: no {name}")))
    };
    Ok((
        region,
        AwsKeys {
            access_key_id: field("AccessKeyId")?,
            secret_access_key: Secret(field("SecretAccessKey")?),
            session_token: keys["Token"].as_str().map(|token| Secret(token.to_owned())),
        },
    ))
}

/// `request` sent, a transport failure naming `step`.
async fn sent(request: reqwest::RequestBuilder, step: &str) -> Result<reqwest::Response, ApiError> {
    request
        .send()
        .await
        .map_err(|error| ApiError::Transport(format!("{step}: {error}")))
}

/// A metadata answer's text, or the refusal naming `step`.
async fn metadata_text(response: reqwest::Response, step: &str) -> Result<String, ApiError> {
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|error| ApiError::Transport(format!("{step}: {error}")))?;
    match status.is_success() {
        true => Ok(text),
        false => Err(ApiError::Auth(format!(
            "{step} refused (HTTP {}): {}",
            status.as_u16(),
            excerpt(&text)
        ))),
    }
}

/// The subject token Google STS takes from AWS: a `GetCallerIdentity` request signed with `keys` (SigV4) in `region` at `verification_url` (whose `{region}` is filled in), carrying the provider `audience` in `x-goog-cloud-target-resource`, serialized as Google's libraries do and URL-encoded. Google sends it to AWS to learn the caller's identity.
pub(crate) fn aws_subject_token(
    keys: &AwsKeys,
    (region, verification_url): (&str, &str),
    audience: &str,
    now_ms: i64,
) -> Result<Secret, ApiError> {
    let url = verification_url.replace("{region}", region);
    let parsed = reqwest::Url::parse(&url).map_err(|error| {
        ApiError::Auth(format!(
            "the AWS verification URL {url:?} is not a URL: {error}"
        ))
    })?;
    let host = parsed
        .host_str()
        .ok_or_else(|| ApiError::Auth(format!("the AWS verification URL {url:?} has no host")))?
        .to_owned();
    let mut query: Vec<(String, String)> = parsed
        .query_pairs()
        .map(|(name, value)| (encode(&name), encode(&value)))
        .collect();
    query.sort();
    let canonical_query: Vec<String> = query
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect();
    let time = chrono::DateTime::from_timestamp_millis(now_ms)
        .ok_or_else(|| ApiError::Protocol("the clock is out of range".to_owned()))?;
    let amz_date = time.format("%Y%m%dT%H%M%SZ").to_string();
    let date = time.format("%Y%m%d").to_string();
    let mut headers: Vec<(String, String)> = vec![
        ("host".to_owned(), host),
        ("x-amz-date".to_owned(), amz_date.clone()),
        (
            "x-goog-cloud-target-resource".to_owned(),
            audience.to_owned(),
        ),
    ];
    if let Some(session) = &keys.session_token {
        headers.push((
            "x-amz-security-token".to_owned(),
            session.expose().to_owned(),
        ));
    }
    headers.sort();
    let canonical_headers: String = headers
        .iter()
        .map(|(name, value)| format!("{name}:{}\n", value.trim()))
        .collect();
    let signed_headers: Vec<&str> = headers.iter().map(|(name, _)| name.as_str()).collect();
    let signed_headers = signed_headers.join(";");
    let path = match parsed.path() {
        "" => "/",
        path => path,
    };
    let canonical_request = format!(
        "POST\n{path}\n{}\n{canonical_headers}\n{signed_headers}\n{}",
        canonical_query.join("&"),
        hex::encode(Sha256::digest(b""))
    );
    let scope = format!("{date}/{region}/sts/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );
    let mut key = format!("AWS4{}", keys.secret_access_key.expose()).into_bytes();
    for part in [date.as_str(), region, "sts", "aws4_request"] {
        key = hmac_sha256(&key, part.as_bytes());
    }
    let signature = hex::encode(hmac_sha256(&key, string_to_sign.as_bytes()));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        keys.access_key_id
    );
    let mut sent_headers: Vec<Value> = headers
        .iter()
        .map(|(name, value)| json!({ "key": name, "value": value }))
        .collect();
    sent_headers.insert(0, json!({ "key": "Authorization", "value": authorization }));
    let request = json!({ "url": url, "method": "POST", "headers": sent_headers });
    Ok(Secret(encode(&request.to_string())))
}

/// HMAC-SHA256 of `data` with `key`.
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    match <Hmac<Sha256> as Mac>::new_from_slice(key) {
        Ok(mut mac) => {
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        }
        Err(_) => Vec::new(),
    }
}

/// `text` percent-encoded, every byte but the unreserved ones (RFC 3986), as SigV4 and Google STS read it.
pub(crate) fn encode(text: &str) -> String {
    text.bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}
