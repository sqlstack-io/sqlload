//! Qualify a configured stage name.

use crate::naming::main::quote_name::quote_name;

/// `DATABASE.SCHEMA.NAME` for `stage`, which is either a bare name in `database.schema` or already qualified.
pub fn qualify_stage(database: &str, schema: &str, stage: &str) -> String {
    let parts: Vec<&str> = stage.split('.').collect();
    match parts.as_slice() {
        [database, schema, name] => [*database, *schema, *name],
        _ => [database, schema, stage],
    }
    .map(quote_name)
    .join(".")
}
