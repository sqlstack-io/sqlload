//! Read, run and rewrite a docs page.

use std::path::Path;

use crate::doc_pages::_helpers::blocks::section_blocks;
use crate::doc_pages::_helpers::steps::run_block;
use crate::doc_pages::models::{PageRun, PageServer};

/// The lines of `page`.
pub(crate) fn page_lines(page: &Path) -> Result<Vec<String>, String> {
    let text =
        std::fs::read_to_string(page).map_err(|error| format!("{}: {error}", page.display()))?;
    Ok(text.lines().map(str::to_owned).collect())
}

/// Run every block of the sections under `headings`, in order, in one new folder.
pub(crate) fn run_page(
    lines: &[String],
    headings: &[&str],
    server: Option<PageServer<'_>>,
    binary: &Path,
) -> Result<PageRun, String> {
    let root = tempfile::tempdir().map_err(|error| error.to_string())?;
    let mut run = PageRun {
        dir: root.path().to_path_buf(),
        env: vec![("NO_COLOR".to_owned(), "1".to_owned())],
        ..PageRun::default()
    };
    for heading in headings {
        for block in section_blocks(lines, heading)? {
            run = run_block(run, &block, server, binary)?;
        }
    }
    Ok(run)
}

/// Write `lines` back to `page` with each block replaced by the run's lines; no differences remain.
pub(crate) fn rewrite_page(
    page: &Path,
    lines: Vec<String>,
    replacements: Vec<(usize, usize, Vec<String>)>,
) -> Result<Vec<String>, String> {
    let mut updated = lines;
    for (first, end, shown) in replacements.into_iter().rev() {
        updated.splice(first..end, shown);
    }
    std::fs::write(page, updated.join("\n") + "\n")
        .map_err(|error| format!("{}: {error}", page.display()))?;
    Ok(Vec::new())
}
