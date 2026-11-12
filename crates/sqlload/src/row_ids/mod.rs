//! Row ids as dlt gives them: random for a root row, a hash of the merge key for a root row a merge matches, and a hash of the parent's id, the child table and the list position for a child row.

pub(crate) mod _helpers;
pub mod constants;
pub mod main;

#[cfg(test)]
mod tests;
