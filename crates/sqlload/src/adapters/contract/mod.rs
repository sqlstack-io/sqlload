//! The warehouse adapter contract: the traits every warehouse implements, their shared defaults, and the models they take.

pub(crate) mod _helpers;
pub mod constants;
pub mod main;
pub mod models;
pub mod types;

#[cfg(test)]
mod tests;
