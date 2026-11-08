//! List a spreadsheet's tabs.

use crate::http::errors::ApiError;
use crate::sheets_source::_helpers::layout::quoted_title;
use crate::sheets_source::_helpers::requests::{grid_sheets, read, spreadsheet_title};
use crate::sheets_source::constants::TAB_FIELDS;
use crate::sheets_source::models::SheetsClient;

/// The spreadsheet's title, and each tab of cells as its title and the A1 range of its whole grid (the title in single quotes), read without their cells.
pub async fn tab_ranges(
    client: &SheetsClient,
    spreadsheet_id: &str,
) -> Result<(String, Vec<(String, String)>), ApiError> {
    let answer = read(client, spreadsheet_id, &[("fields", TAB_FIELDS)]).await?;
    let tabs: Vec<(String, String)> = grid_sheets(&answer)
        .map(|sheet| {
            let title = sheet["properties"]["title"]
                .as_str()
                .unwrap_or_default()
                .to_owned();
            let range = quoted_title(&title);
            (title, range)
        })
        .collect();
    Ok((spreadsheet_title(&answer), tabs))
}
