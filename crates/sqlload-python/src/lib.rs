//! The `sqlload._native` extension module: the `sqlload` command line in-process, and table generators crossing into Arrow.

#![forbid(unsafe_code)]

pub mod bindings;
pub mod crossing;
pub mod host;
