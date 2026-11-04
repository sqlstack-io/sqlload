//! Work out a files table's folder from its settings.

use crate::files_destination::main::table_path::table_path;
use crate::files_destination::models::{FilesDestination, TablePath, TablePathRequest};
use crate::loader::models::TableSpec;
use crate::project::constants::DEFAULT_DESTINATION_PATH;

/// The table's folder and partition folder in `destination`; no I/O.
pub(in crate::loader) fn table_folder(
    spec: &TableSpec,
    destination: &FilesDestination,
) -> Result<TablePath, String> {
    table_path(&TablePathRequest {
        template: spec
            .destination_path
            .as_deref()
            .unwrap_or(DEFAULT_DESTINATION_PATH),
        connection: &destination.connection,
        store: &destination.store,
        source: &spec.source_name,
        schema: &spec.source_table.schema,
        table: &spec.source_table.name,
    })
}
