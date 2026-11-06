//! Yielded rows into Arrow: rows gathered column by column, JSON bytes, and Arrow batches become typed batches with a merged schema.

pub(crate) mod _helpers;
pub mod constants;
pub mod main;
pub mod models;
pub mod types;

#[cfg(test)]
mod tests;
