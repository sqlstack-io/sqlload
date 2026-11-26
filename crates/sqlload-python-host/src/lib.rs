//! The Python host of SQLLoad's engine, for a pyo3 extension of its own: the README gives the runtime module and versions.
#![forbid(unsafe_code)]

pub mod crossing;
pub mod host;

/// The pyo3 this crate is built with, which a host's extension must use.
pub use pyo3;
