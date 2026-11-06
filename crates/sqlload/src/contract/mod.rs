//! `sqlload contract diff` and `generate`: each selected table's declared columns compared with its live table, in SQLLoad's portable types, and the declarations that adopt it (sqlbuild's `sqb contract`).

pub(crate) mod _helpers;
pub mod constants;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
