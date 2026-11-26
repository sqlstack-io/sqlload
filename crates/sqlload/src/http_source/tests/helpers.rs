/// The base URL the URL tests join paths under.
pub(crate) fn base() -> reqwest::Url {
    reqwest::Url::parse("https://api.shop.example/v1/").unwrap_or_else(|_| unreachable!())
}

/// The interval of the rate limit the limiter tests wait under, `10/s`.
pub(crate) const LIMIT_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

/// When each of seven requests waiting under a `10/s` limit went, from the first, in order, on tokio's paused clock: four wait at once as read-ahead starts them, those at `aborted` among them are aborted 50 ms in (the run ended past them), and three more then wait.
pub(crate) fn read_ahead_turns(aborted: &[usize]) -> Vec<std::time::Duration> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap_or_else(|error| panic!("{error}"));
    runtime.block_on(async {
        let limiter = std::sync::Arc::new(crate::http_source::models::Limiter {
            connection: "api".to_owned(),
            rate: "10/s".to_owned(),
            interval: LIMIT_INTERVAL,
            next: std::sync::Mutex::new(tokio::time::Instant::now()),
        });
        let start = tokio::time::Instant::now();
        let waiting = |limiter: &std::sync::Arc<crate::http_source::models::Limiter>| {
            let limiter = std::sync::Arc::clone(limiter);
            tokio::spawn(async move {
                limiter.wait().await;
                tokio::time::Instant::now()
            })
        };
        let first: Vec<tokio::task::JoinHandle<tokio::time::Instant>> =
            (0..4).map(|_| waiting(&limiter)).collect();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        aborted.iter().for_each(|index| first[*index].abort());
        let later: Vec<tokio::task::JoinHandle<tokio::time::Instant>> =
            (0..3).map(|_| waiting(&limiter)).collect();
        let mut ended: Vec<Result<tokio::time::Instant, tokio::task::JoinError>> = Vec::new();
        for handle in [first, later].into_iter().flatten() {
            ended.push(handle.await);
        }
        let mut went: Vec<std::time::Duration> =
            ended.into_iter().flatten().map(|at| at - start).collect();
        went.sort();
        went
    })
}

/// The folder `sqlload_api` commits the wire it crosses as in, one `<case>.json` per item.
fn wire_folder() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../packages/sqlload-api/python/sqlload_api/_wire")
}

/// The names of the wire files `sqlload_api` commits, in name order.
pub(crate) fn wire_files() -> Result<Vec<String>, String> {
    let folder = wire_folder();
    let entries =
        std::fs::read_dir(&folder).map_err(|error| format!("{}: {error}", folder.display()))?;
    let mut names: Vec<String> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .filter_map(|path| {
            path.file_stem()
                .map(|stem| stem.to_string_lossy().into_owned())
        })
        .collect();
    names.sort();
    Ok(names)
}

/// What wire file `name` holds as a `kind` (`job`, `json` or `checkpoint`), or why it holds none.
fn wire_item(name: &str, kind: &str) -> Result<serde_json::Value, String> {
    let path = wire_folder().join(format!("{name}.json"));
    let text =
        std::fs::read_to_string(&path).map_err(|error| format!("{}: {error}", path.display()))?;
    let item: serde_json::Value = serde_json::from_str(&text).map_err(|error| error.to_string())?;
    item.get(kind)
        .cloned()
        .ok_or_else(|| format!("{name} holds no {kind}"))
}

/// The problems the engine finds in wire file `name`'s job before any request.
pub(crate) fn wire_job_problems(name: &str) -> Result<Vec<String>, String> {
    let job: crate::http_source::models::HttpJob =
        serde_json::from_value(wire_item(name, "job")?).map_err(|error| error.to_string())?;
    Ok(crate::http_source::_helpers::job::job_problems(
        &job,
        crate::row_batches::models::ProjectionRules::default(),
    ))
}

/// Why the engine can't read wire file `name`'s JSON data as its read spec asks, if it can't.
pub(crate) fn wire_json_problems(name: &str) -> Result<Vec<String>, String> {
    let read = wire_item(name, "json")?["read"].to_string();
    Ok(
        crate::row_batches::main::json_read_spec::json_read_spec(&read, 1_000)
            .err()
            .into_iter()
            .collect(),
    )
}

/// Nothing, once wire file `name` holds a checkpoint's state.
pub(crate) fn wire_checkpoint_problems(name: &str) -> Result<Vec<String>, String> {
    wire_item(name, "checkpoint").map(|_| Vec::new())
}
