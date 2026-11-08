//! Work out which existing files a write replaces, before anything is written.

use crate::adapters::contract::models::{ChangeApply, WriteMode};
use crate::adapters::files::_helpers::layout::partitions::partition_in_window;
use crate::adapters::files::main::check_window::check_window;
use crate::adapters::files::models::{ReplaceRequest, ReplacedFiles};

/// None for append, all for replace, partitions in the window (and any other the load writes to) for delete_insert.
pub fn plan_replaced_files(request: &ReplaceRequest<'_>) -> Result<ReplacedFiles, String> {
    let (from, to) = match request.mode {
        WriteMode::Append => return Ok(ReplacedFiles::default()),
        WriteMode::Changes(change) if change.apply == ChangeApply::Changelog => {
            return Ok(ReplacedFiles::default());
        }
        WriteMode::Changes(_) => {
            return Err("captured changes reach a files target only as a changelog (cdc_mode = \"changelog\")".to_owned());
        }
        WriteMode::Replace | WriteMode::Recreate => {
            return Ok(ReplacedFiles {
                always: request.listed.keys(),
                ..ReplacedFiles::default()
            });
        }
        WriteMode::Merge { .. } => {
            return Err("merge is not supported for files targets".to_owned());
        }
        WriteMode::DeleteInsert { from, to, .. } => (from.as_deref(), to.as_deref()),
    };
    let (Some(partition), Some(cursor_type)) = (request.partition, request.cursor_type) else {
        return Err("delete_insert into files needs a partition folder by the cursor".to_owned());
    };
    check_window(partition.by, cursor_type, (from, to))?;
    let mut replaced = ReplacedFiles::default();
    for existing in &request.listed.partitions {
        let value = existing.value.clone().unwrap_or_default();
        let keys: Vec<String> = existing
            .files
            .iter()
            .map(|file| file.relative_key.clone())
            .collect();
        match partition_in_window(&value, partition.by, cursor_type, (from, to))? {
            true => replaced.always.extend(keys),
            false => {
                replaced.by_partition.insert(value, keys);
            }
        }
    }
    Ok(replaced)
}
