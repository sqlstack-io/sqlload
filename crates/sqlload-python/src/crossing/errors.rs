use pyo3::PyErr;

/// Why a table generator's data could not be read.
#[derive(Debug)]
pub enum CrossError {
    /// The generator raised, or yielded something that is not table data.
    Python(PyErr),
    /// The data could not become Arrow (malformed JSON, a non-object row).
    Data(String),
}

impl std::fmt::Display for CrossError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Python(error) => write!(formatter, "{error}"),
            Self::Data(reason) => formatter.write_str(reason),
        }
    }
}
