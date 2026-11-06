//! `sqlload copy`: a one-off copy of one table, from a connection named in the project or a URI, run as a one-table project through the usual `load`.

pub(crate) mod _helpers;
pub mod constants;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
