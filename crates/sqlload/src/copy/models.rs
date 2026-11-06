use std::collections::BTreeMap;

use crate::copy::constants::{DESTINATION_PASSWORD_VARIABLE, SOURCE_PASSWORD_VARIABLE};

/// The project `sqlload copy` runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyProject {
    /// The project's TOML: passwords only as `${ENV:NAME}` references.
    pub text: String,
    /// What `--print-toml` prints: the same without named connections, so it can be added to the sqlload.toml that has them.
    pub printed: String,
    /// Values for the references to literal URI passwords, read before the process environment.
    pub variables: BTreeMap<String, String>,
    /// Things to tell the user before the copy runs.
    pub warnings: Vec<String>,
}

/// Which end of the copy an endpoint is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Source,
    Destination,
}

impl Role {
    /// The variable a literal password at this end is kept in.
    pub fn password_variable(self) -> &'static str {
        match self {
            Self::Source => SOURCE_PASSWORD_VARIABLE,
            Self::Destination => DESTINATION_PASSWORD_VARIABLE,
        }
    }

    /// How messages name this end.
    pub fn label(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Destination => "destination",
        }
    }
}

/// One end of the copy: its connection's settings and what it names there.
#[derive(Debug, Clone, PartialEq)]
pub struct Endpoint {
    /// The connection's name in the project; set once the copy's table is known for a URI or folder.
    pub connection: String,
    /// Named in sqlload.toml rather than given as a URI or folder.
    pub named: bool,
    /// The connection's settings as TOML, `${ENV:NAME}` references unresolved.
    pub settings: toml::Table,
    /// After the connection: `schema.table` for databases and warehouses, a folder for files.
    pub locator: String,
    /// A literal password moved out of the text: its variable and value.
    pub secret: Option<(String, String)>,
    /// The endpoint as messages show it, passwords redacted.
    pub shown: String,
    /// The endpoint for the table's description (its warehouse comment): no user, password or host.
    pub described: String,
}

/// What `sqlload copy` was asked to do.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CopyRequest {
    /// `connection:schema.table`, a URI, or a local folder.
    pub source: String,
    /// `connection:SCHEMA.TABLE`, a URI, or a local folder.
    pub destination: String,
    pub write_strategy: Option<String>,
    pub unique_key: Vec<String>,
    pub cursor_column: Option<String>,
    /// The format of a files source's files.
    pub format: Option<String>,
}
