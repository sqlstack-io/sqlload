//! Run schema changes on a destination, one table at a time.

use crate::adapters::dispatch::main::ddl_guard::ddl_guard;
use crate::adapters::dispatch::main::execute::execute;
use crate::adapters::dispatch::models::Destination;
use crate::loader::errors::LoaderError;

/// Run schema changes with the destination's other loading tables' schema changes held off, so parallel tables never conflict in its catalog.
pub async fn execute_ddl(
    destination: &Destination,
    statements: Vec<String>,
) -> Result<(), LoaderError> {
    let _held = ddl_guard(destination).await;
    execute(destination, statements).await
}
