//! The host the `sqlload` command of the Python package gives the loader.

use std::sync::Arc;

use sqlload::python_sources::types::PythonHost;

use crate::host::models::ExtensionHost;

/// A host that runs Python sources in this interpreter.
pub fn extension_host() -> Arc<dyn PythonHost> {
    Arc::new(ExtensionHost)
}
