use std::task::{Context, Poll};

use tower_layer::Layer;
use tower_service::Service;

use crate::http::models::ConnectionCounter;

/// A connector layer that counts dials; reqwest only dials when its pool has no reusable connection.
#[derive(Clone)]
pub(crate) struct CountingLayer(pub(crate) ConnectionCounter);

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
pub(crate) struct Counting<S> {
    inner: S,
    counter: ConnectionCounter,
}

impl<S, R> Service<R> for Counting<S>
where
    S: Service<R>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: R) -> Self::Future {
        self.counter.increment();
        self.inner.call(request)
    }
}
