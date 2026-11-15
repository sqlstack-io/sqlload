//! Read a held file back.

use std::path::Path;

use arrow_ipc::reader::FileReader;

/// The batches held in `path`, in the order they were held, read one at a time.
pub fn held_rows(path: &Path) -> Result<FileReader<std::io::BufReader<std::fs::File>>, String> {
    let file = std::fs::File::open(path)
        .map_err(|error| format!("cannot read held rows in {}: {error}", path.display()))?;
    FileReader::try_new(std::io::BufReader::new(file), None)
        .map_err(|error| format!("cannot read held rows in {}: {error}", path.display()))
}
