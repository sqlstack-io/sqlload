use crate::routing::models::{ReaderOffer, RouteOverrides, WriterAccepts};

pub(crate) struct PlanRouteTestCase {
    pub(crate) description: &'static str,
    pub(crate) reader: ReaderOffer,
    pub(crate) writer: WriterAccepts,
    pub(crate) overrides: RouteOverrides,
    /// The route's steps as they print.
    pub(crate) expected_steps: &'static [&'static str],
}

pub(crate) struct PlanRouteErrorTestCase {
    pub(crate) description: &'static str,
    pub(crate) reader: ReaderOffer,
    pub(crate) writer: WriterAccepts,
    pub(crate) overrides: RouteOverrides,
    pub(crate) expected_message: &'static str,
}
