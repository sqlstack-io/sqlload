use crate::file_store::models::StoredFile;
use crate::files_source::models::DataFile;
use crate::store_paths::main::hive_values::hive_values;

/// `file` with its values for `partition_columns`, and a problem if a partition folder is missing.
pub(crate) fn data_file(
    file: StoredFile,
    partition_columns: &[String],
) -> (DataFile, Option<String>) {
    let mut partitions: Vec<(String, String)> = hive_values(&file.relative_key);
    partitions.retain(|(key, _)| partition_columns.contains(key));
    let found: Vec<&String> = partitions.iter().map(|(key, _)| key).collect();
    let problem = partition_columns
        .iter()
        .find(|column| !found.contains(column))
        .map(|missing| {
            format!(
                "{} has no {missing}=... folder, but {missing} is a partition column",
                file.relative_key
            )
        });
    (DataFile { file, partitions }, problem)
}
