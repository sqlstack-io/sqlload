//! Find another table whose folder clashes with a table's.

use crate::adapters::files::models::FolderClaim;
use crate::store_paths::models::{Location, StoreKind};

/// Why `claim`'s folder is unsafe: the folder its own files source reads, or another configured table's folder, is the same, inside it or around it, so replacing one would delete the other's files (or a load would read back what it wrote).
pub fn folder_conflict(claim: &FolderClaim, claims: &[FolderClaim]) -> Option<String> {
    let key = store_key(&claim.folder);
    if let Some(source) = &claim.source {
        let source_key = store_key(source);
        if key.starts_with(&source_key) || source_key.starts_with(&key) {
            return Some(format!(
                "{} writes to {}, which overlaps the folder it reads from, {source}; a load would delete or read back its own source files, so give the target a folder of its own",
                claim.name, claim.folder
            ));
        }
    }
    let other = claims.iter().find(|other| {
        let other_key = store_key(&other.folder);
        other.id != claim.id && (key.starts_with(&other_key) || other_key.starts_with(&key))
    })?;
    let hint = match key == store_key(&other.folder)
        && !claim.template.contains("{schema}")
        && claim.schema != other.schema
    {
        true => format!(
            "; both come from tables named alike in schemas {} and {}, so add {{schema}} to the path",
            claim.schema, other.schema
        ),
        false => String::new(),
    };
    Some(format!(
        "{} writes to {}, which overlaps {}'s folder {}; give each table a folder of its own{hint}",
        claim.name, claim.folder, other.name, other.folder
    ))
}

/// The folder as one string per place in storage: the bucket for S3 (whatever the region or endpoint), the resolved root for a directory.
fn store_key(folder: &Location) -> String {
    match &folder.store {
        StoreKind::S3 { bucket, .. } => format!("s3://{bucket}/{}", folder.prefix),
        StoreKind::Filesystem { root } => {
            let resolved = match std::fs::canonicalize(root) {
                Ok(path) => path.display().to_string(),
                Err(_) => root.clone(),
            };
            format!("{}/{}", resolved.trim_end_matches('/'), folder.prefix)
        }
        StoreKind::Sftp { .. } => folder.to_string(),
    }
}
