use crate::loader::errors::LoaderError;

/// The error for a SQL operation on a files target, which plans and loads without SQL.
pub(crate) fn no_sql() -> LoaderError {
    LoaderError::Invalid(
        "a files target runs no SQL; its tables are planned and loaded as files".to_owned(),
    )
}
