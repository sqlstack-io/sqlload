/// Why no route connects a source to a destination.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RouteError {
    #[error("the source produces no data the destination can take: {0}")]
    NoRoute(String),
    #[error(
        "{location} cannot be read by {warehouse}: no external stage covers it. Fix: add a storage integration that allows this bucket (the fast path), or, if that isn't possible, set copy_to_internal_stage = true (files are downloaded and uploaded, much slower)"
    )]
    Unreadable {
        warehouse: &'static str,
        location: String,
    },
    #[error("the destination cannot load these files exactly by reading them itself: {0}")]
    DirectRefused(String),
    #[error("stage = \"{wanted}\" was requested, but the destination has no {wanted} stage")]
    StageUnavailable { wanted: &'static str },
    #[error("read_via = \"{wanted}\" was requested, but the source cannot {wanted}")]
    ReadViaUnavailable { wanted: &'static str },
}
