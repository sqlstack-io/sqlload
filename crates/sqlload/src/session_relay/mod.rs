//! Where a client that speaks only plain text, such as the replication client, reaches a session SQLLoad has opened for it: a private socket on Unix, a loopback port that admits only its own client on Windows.

pub(crate) mod _helpers;
pub mod constants;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
