use crate::adapters::contract::models::TableName;
use crate::adapters::dispatch::models::Destination;
use crate::column_types::models::Warehouse;

#[derive(Debug)]
/// Where a commit's checks run: the relation holding exactly the rows about to be published, and the table they are published into.
pub struct CheckedCommit<'a> {
    pub destination: &'a Destination,
    pub warehouse: Warehouse,
    pub staging: &'a TableName,
    pub table: &'a TableName,
    /// Whether the table exists, so `max_row_drop` has rows to compare with.
    pub exists: bool,
}
