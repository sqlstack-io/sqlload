//! A table's resource name.

/// `projects/p/datasets/d/tables/t`, as the Storage Write API names a table.
pub fn table_path(project: &str, dataset: &str, table: &str) -> String {
    format!("projects/{project}/datasets/{dataset}/tables/{table}")
}
