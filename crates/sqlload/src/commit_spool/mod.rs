//! `send = "at_commit"`: a commit's batches held in local Arrow files while the source is read, then sent to the target in one burst just before the commit.

pub(crate) mod _helpers;
pub mod constants;
pub mod main;
pub mod models;
