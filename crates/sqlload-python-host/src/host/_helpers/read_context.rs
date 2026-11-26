use sqlload::python_sources::models::TableRead;

use crate::crossing::models::ReadContext;

/// What the crossing needs of the table `request` reads, besides its data.
pub(crate) fn read_context(request: &TableRead) -> ReadContext {
    ReadContext {
        table: request.table.clone(),
        flatten: request.flatten.clone(),
        connections: request.connections.clone(),
        cursor_column: request.cursor.as_ref().map(|cursor| cursor.column.clone()),
        masked: request.masked.clone(),
        arrays: request.arrays.clone(),
        naming: request.naming,
        zone: request.zone,
        source_part: request.source_part,
    }
}
