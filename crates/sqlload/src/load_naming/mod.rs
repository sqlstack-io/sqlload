//! How a load names what it loads: the source's tables and columns (`naming`, `rename`, `on_duplicate_names`), and the columns it adds beside the data (`compat`).

pub(crate) mod _helpers;
pub mod constants;
pub mod main;
pub mod models;
pub mod types;

#[cfg(test)]
mod tests;
