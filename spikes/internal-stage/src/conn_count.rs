//! Tower layer for reqwest's connector that counts every new connection it dials.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

#[derive(Clone, Default)]
pub struct ConnCounter(pub Arc<AtomicUsize>);

impl ConnCounter {
    pub fn get(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

impl<S> tower_layer::Layer<S> for ConnCounter {
    type Service = Counting<S>;

    fn layer(&self, inner: S) -> Counting<S> {
        Counting {
            inner,
            count: self.0.clone(),
        }
    }
}

#[derive(Clone)]
pub struct Counting<S> {
    inner: S,
    count: Arc<AtomicUsize>,
}

impl<S, R> tower_service::Service<R> for Counting<S>
where
    S: tower_service::Service<R>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: R) -> Self::Future {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.inner.call(req)
    }
}

/// System-wide `Tcp: ActiveOpens` from `/proc/net/snmp` (noisy on a shared host).
pub fn tcp_active_opens() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/net/snmp").ok()?;
    let mut lines = text.lines().filter(|l| l.starts_with("Tcp:"));
    let header = lines.next()?;
    let values = lines.next()?;
    let idx = header.split_whitespace().position(|h| h == "ActiveOpens")?;
    values.split_whitespace().nth(idx)?.parse().ok()
}
