//! `parse`: text (or epoch numbers) read into `timestamp_tz` or `date` columns on every source, with a list of formats tried in turn and the zone of times without an offset.

pub(crate) mod _helpers;
pub mod constants;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
