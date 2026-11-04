//! The loader: resolve tables from a project, plan, apply schema changes and load data.

pub(crate) mod _helpers;
pub mod constants;
pub mod errors;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
