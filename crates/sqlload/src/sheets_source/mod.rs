//! Google Sheets as a source: a spreadsheet's tabs or ranges read as tables, with typed cells.

pub(crate) mod _helpers;
pub mod constants;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
