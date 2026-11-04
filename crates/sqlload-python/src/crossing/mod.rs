//! Table generators into Arrow: yielded dicts, lists of dicts, JSON bytes and Arrow data become a stream of record batches.

pub(crate) mod _helpers;
pub mod errors;
pub mod main;
pub mod models;
