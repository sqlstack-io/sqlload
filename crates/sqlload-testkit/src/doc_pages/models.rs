use testcontainers::{ContainerAsync, GenericImage};

/// The server a page's project loads from, started for it, and the runtime its client runs on.
#[derive(Debug, Clone, Copy)]
pub struct PageServer<'a> {
    pub postgres: &'a PagePostgres,
    pub runtime: &'a tokio::runtime::Runtime,
}

/// The Postgres server a page starts, a client connected as its `postgres` user, and its port.
#[derive(Debug)]
pub struct PagePostgres {
    pub container: ContainerAsync<GenericImage>,
    pub client: tokio_postgres::Client,
    pub port: u16,
}

/// A fenced block of a page: its language, the line numbers of its first content line and its closing fence, and its lines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Block {
    pub(crate) language: String,
    pub(crate) first: usize,
    pub(crate) end: usize,
    pub(crate) lines: Vec<String>,
}

/// Where a page's run is: its folder, environment and seed SQL, each difference found, and each block's lines from the run.
#[derive(Debug, Default)]
pub(crate) struct PageRun {
    pub(crate) dir: std::path::PathBuf,
    pub(crate) env: Vec<(String, String)>,
    pub(crate) seed: String,
    pub(crate) differences: Vec<String>,
    pub(crate) replacements: Vec<(usize, usize, Vec<String>)>,
}
