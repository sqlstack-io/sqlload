//! Read a spreadsheet's cells.

use crate::http::errors::ApiError;
use crate::sheets_source::_helpers::cells::parse_grid;
use crate::sheets_source::_helpers::requests::{grid_sheets, read, spreadsheet_title};
use crate::sheets_source::constants::{GRID_FIELDS, RANGES_PARAMETER};
use crate::sheets_source::models::{Grid, SheetsClient};

/// The spreadsheet's title and the cells of `range` (an A1 range or a quoted tab title), or of every tab of cells when `None` (charts are left out).
pub async fn read_grids(
    client: &SheetsClient,
    spreadsheet_id: &str,
    range: Option<&str>,
) -> Result<(String, Vec<Grid>), ApiError> {
    let mut query: Vec<(&str, &str)> = vec![("includeGridData", "true"), ("fields", GRID_FIELDS)];
    query.extend(range.map(|range| (RANGES_PARAMETER, range)));
    let answer = read(client, spreadsheet_id, &query).await?;
    Ok((
        spreadsheet_title(&answer),
        grid_sheets(&answer).map(parse_grid).collect(),
    ))
}
