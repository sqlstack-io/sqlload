//! Snowflake authentication with a programmatic access token, a key pair, or a browser sign-in.

pub(crate) mod _helpers;
pub mod constants;
pub(crate) mod errors;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
