//! ODBC as a source: tables and queries of any database with an ODBC driver, read into Arrow through the system driver manager.

pub(crate) mod _helpers;
pub mod constants;
pub mod errors;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
