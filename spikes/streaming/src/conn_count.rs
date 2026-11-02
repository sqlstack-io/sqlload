//! A connector layer that counts how many times reqwest dials a new connection.
//!
//! reqwest only calls its connector when the pool has no reusable connection, so the
//! counter equals the number of new TCP (+TLS) connections the client opened.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use tower_layer::Layer;
use tower_service::Service;

#[derive(Clone)]
pub struct CountingLayer(Arc<AtomicUsize>);

impl CountingLayer {
    pub fn new(counter: Arc<AtomicUsize>) -> Self {
        Self(counter)
    }
}

impl<S> Layer<S> for CountingLayer {
    type Service = Counting<S>;

    fn layer(&self, inner: S) -> Counting<S> {
        Counting {
            inner,
            counter: self.0.clone(),
        }
    }
}

#[derive(Clone)]
pub struct Counting<S> {
    inner: S,
    counter: Arc<AtomicUsize>,
}

impl<S, R> Service<R> for Counting<S>
where
    S: Service<R>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: R) -> Self::Future {
        self.counter.fetch_add(1, Ordering::SeqCst);
        self.inner.call(req)
    }
}
