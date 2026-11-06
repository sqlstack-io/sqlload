//! `sqlload contract diff`: each selected table's declared columns compared with its live table, in SQLLoad's portable types (sqlbuild's `sqb contract diff`).

pub(crate) mod _helpers;
pub mod constants;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
