/// The `sqlload-api` module every host's crossing calls for what transforms and crossing need (`crossing_default`, `transform_version`, `call_transform`, `page_items`, `finish`), so a process has one transform registry whichever host runs.
pub const API_HOST_MODULE: &str = "sqlload_api._host";

/// The `sqlload-api` package, whose version must be this crate's.
pub const API_PACKAGE: &str = "sqlload_api";
