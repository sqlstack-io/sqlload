//! The host the `sqlload` command of the Python package gives the loader.

use std::sync::Arc;

use sqlload::python_sources::types::PythonHost;
use sqlload_python_host::host::main::module_host::module_host;

use crate::bindings::constants::RUNTIME_MODULE;

/// A host that runs `sl.source`s in this interpreter, through `sqlload._runtime`.
pub(crate) fn extension_host() -> Arc<dyn PythonHost> {
    module_host(RUNTIME_MODULE)
}
