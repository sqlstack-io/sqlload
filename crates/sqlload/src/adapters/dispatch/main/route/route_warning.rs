//! The warnings a planned route earns.

use crate::adapters::bigquery::main::plan::_row_number_warning::row_number_warning;
use crate::adapters::snowflake::main::stage::_route_warning::route_warning as snowflake_route_warning;
use crate::adapters::snowflake::models::StageRoute;
use crate::column_types::models::Warehouse;
use crate::files_source::models::FilesRead;
use crate::loader::models::TableSpec;

/// The warnings plan shows for `spec`'s chosen stage `route` and its files' planned `read`: Snowflake's slowest route, and a BigQuery files table that keeping `_sqlload_source_row` alone leaves on SQLLoad's reader, with the change that lets BigQuery read the files itself.
pub fn route_warning(
    route: Option<&StageRoute>,
    spec: &TableSpec,
    read: Option<&FilesRead>,
) -> Vec<String> {
    let row_number = match (spec.warehouse, read) {
        (Warehouse::BigQuery, Some(read)) => row_number_warning(spec, read),
        _ => None,
    };
    snowflake_route_warning(route)
        .map(str::to_owned)
        .into_iter()
        .chain(row_number)
        .collect()
}
