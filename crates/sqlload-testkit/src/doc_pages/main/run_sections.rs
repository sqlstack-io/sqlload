//! Run sections of a docs page as a reader would, and compare or rewrite their output blocks.

use std::path::Path;

use crate::doc_pages::_helpers::page::{page_lines, rewrite_page, run_page};
use crate::doc_pages::constants::UPDATE_VARIABLE;
use crate::doc_pages::models::PageServer;

/// Run `page`'s sections under each `## heading` in one new folder with `binary`; the blocks that differ (none after `SQLLOAD_UPDATE_DOCS=1` rewrites them).
pub fn run_sections(
    page: &Path,
    binary: &Path,
    headings: &[&str],
    server: Option<PageServer<'_>>,
) -> Result<Vec<String>, String> {
    let lines = page_lines(page)?;
    let run = run_page(&lines, headings, server, binary)?;
    match std::env::var(UPDATE_VARIABLE).as_deref() == Ok("1") {
        true => rewrite_page(page, lines, run.replacements),
        false => Ok(run.differences),
    }
}
