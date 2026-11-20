use crate::project::models::ProjectConfig;

pub(crate) struct ParseProjectTestCase {
    pub(crate) description: &'static str,
    /// Reads the value under test from the parsed project, as debug text.
    pub(crate) read: fn(&ProjectConfig) -> String,
    pub(crate) expected_value: &'static str,
}

pub(crate) struct ParseProjectErrorTestCase {
    pub(crate) description: &'static str,
    pub(crate) text: &'static str,
    /// Text the error message must contain.
    pub(crate) expected_message: &'static str,
}

pub(crate) struct IsProjectTestCase {
    pub(crate) description: &'static str,
    pub(crate) text: &'static str,
    pub(crate) expected_is_project: bool,
}

pub(crate) struct ParsePythonProjectTestCase {
    pub(crate) description: &'static str,
    /// TOML after the shared connections and target.
    pub(crate) toml: &'static str,
    /// The discovered Python sources, as the host's JSON.
    pub(crate) sources: &'static str,
    /// Reads the value under test from the parsed project, as debug text.
    pub(crate) read: fn(&crate::project::models::ProjectConfig) -> String,
    /// The value read, or text the error message must contain.
    pub(crate) expected_value: Result<&'static str, &'static str>,
}

/// A setting's path in the project file, and the `file:line` it is written at.
pub(crate) struct ConfigAtTestCase {
    pub(crate) description: &'static str,
    pub(crate) path: &'static [&'static str],
    pub(crate) expected_at: Option<&'static str>,
}

/// A connection, and the `${ENV:NAME}` variables its settings read.
pub(crate) struct ConnectionVariablesTestCase {
    pub(crate) description: &'static str,
    pub(crate) connection: &'static str,
    pub(crate) expected_variables: &'static [&'static str],
}

/// A MotherDuck connection's `load_method` and where it stages.
pub(crate) struct MotherDuckStageTestCase {
    pub(crate) description: &'static str,
    pub(crate) text: String,
    /// The stage and whether it keeps its files (`Some(None)` for a `local` load); `None` when the project is refused.
    pub(crate) expected_stage: Option<Option<(crate::store_paths::models::Location, bool)>>,
    /// Text the error message must contain; empty when there is none.
    pub(crate) expected_error: &'static str,
}
