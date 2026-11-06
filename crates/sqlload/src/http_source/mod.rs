//! HTTP jobs that Python table functions build with `sl.http`: pagination, fan-out, auth, retries and a shared rate limit, all run in Rust; pages go to the crossing as JSON bytes.

pub(crate) mod _helpers;
pub mod constants;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
