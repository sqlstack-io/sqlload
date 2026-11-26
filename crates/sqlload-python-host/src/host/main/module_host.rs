//! A host whose Python side is a module of the host's own.

use std::sync::Arc;

use sqlload::python_sources::types::PythonHost;

use crate::host::models::ModuleHost;

/// A [`PythonHost`] that calls `runtime`, a Python module the host provides (see [`ModuleHost`] for what it defines), and `sqlload_api._host` for transforms.
pub fn module_host(runtime: &str) -> Arc<dyn PythonHost> {
    Arc::new(ModuleHost {
        runtime: runtime.to_owned(),
    })
}
