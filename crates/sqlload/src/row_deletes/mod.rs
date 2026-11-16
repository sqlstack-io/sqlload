//! Rows a merge table marks deleted: those whose key the source no longer holds, found by a full key scan (`load --sync-deletes`).

pub mod constants;
pub mod main;

#[cfg(test)]
mod tests;
