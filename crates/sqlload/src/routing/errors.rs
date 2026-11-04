/// Why no route connects a source to a destination.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RouteError {
    #[error("the source produces no data the destination can take: {0}")]
    NoRoute(String),
    #[error("stage = \"{wanted}\" was requested, but the destination has no {wanted} stage")]
    StageUnavailable { wanted: &'static str },
    #[error("read_via = \"{wanted}\" was requested, but the source cannot {wanted}")]
    ReadViaUnavailable { wanted: &'static str },
}
