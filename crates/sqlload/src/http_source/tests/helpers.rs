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
