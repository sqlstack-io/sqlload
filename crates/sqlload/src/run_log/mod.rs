//! The append-only run log behind `state = "logged"`: one record per run, reset and reconciliation, in `_sqlload_runs` (warehouses) or as one object per record under `_sqlload/runs/` (files targets).

pub mod constants;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
