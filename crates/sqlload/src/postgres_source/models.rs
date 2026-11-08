use std::path::Path;

use serde::Deserialize;

use crate::column_types::models::ColumnType;

/// One column of a source table, as the catalog describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceColumn {
    pub name: String,
    /// The type as Postgres prints it, for example `numeric(18,2)` or `timestamp with time zone`.
    pub postgres_type: String,
    /// The Postgres type family, which decides how it can be read.
    pub pg_type: PgType,
    /// The portable type it maps to, or why it has none (then a type must be pinned).
    pub column_type: Result<ColumnType, String>,
    pub nullable: bool,
    /// The name as the source's own SQL spells it, when it differs from `name` (a Snowflake upper-case name loads in lower case).
    pub source_spelling: Option<String>,
}

/// One source table or view, as the catalog describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceTable {
    pub schema: String,
    pub name: String,
    pub columns: Vec<SourceColumn>,
    pub primary_key: Vec<String>,
    /// Columns that lead a btree index, so reading the table in their order is cheap.
    pub indexed_columns: Vec<String>,
    /// The planner's row estimate (`pg_class.reltuples`); negative when never analysed.
    pub estimated_rows: i64,
    pub estimated_bytes: i64,
    /// The table's own comment in the source (`COMMENT ON TABLE`), the description of a discovered table.
    pub comment: Option<String>,
}

impl SourceTable {
    /// `schema.name`.
    pub fn qualified_name(&self) -> String {
        match self.schema.is_empty() {
            true => self.name.clone(),
            false => format!("{}.{}", self.schema, self.name),
        }
    }
}

/// The Postgres types SQLLoad reads natively; everything else is read as text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgType {
    Bool,
    Int2,
    Int4,
    Int8,
    Oid,
    Float4,
    Float8,
    Numeric,
    Text,
    Varchar,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Json,
    Uuid,
    Bytea,
    Array,
    /// Enums, intervals, network types, `timetz`, extension types and so on.
    Other,
}

/// How a column travels in the binary `COPY` stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireType {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Numeric,
    /// UTF-8 text: `text`, `varchar`, and anything cast to text in the `COPY` query.
    Text,
    Uuid,
    Bytea,
    Date,
    Time,
    /// `timestamp` and `timestamptz`, both microseconds since 2000-01-01 UTC.
    Timestamp,
}

/// One column to read: what to select, how it arrives, and the type to produce.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadColumn {
    pub name: String,
    /// The select-list expression, already quoted (for example `"total"` or `"tags"::text`).
    pub expression: String,
    pub wire: WireType,
    pub output: ColumnType,
}

/// Where and as whom to connect.
#[derive(Debug, Clone)]
pub struct PostgresAddress<'a> {
    pub host: &'a str,
    pub port: u16,
    pub user: &'a str,
    pub password: Option<&'a str>,
    pub database: &'a str,
    pub tls: PostgresTls<'a>,
    /// The bastion `host` is reached through; `host` and `port` are then as the bastion sees them.
    pub ssh: Option<&'a crate::ssh_tunnel::models::SshTunnel>,
}

impl PostgresAddress<'_> {
    /// The address as errors show it: `host:port/database`, and the bastion it goes through.
    pub fn shown(&self) -> String {
        let route = self
            .ssh
            .map(|tunnel| format!(" via {}", tunnel.route()))
            .unwrap_or_default();
        format!("{}:{}/{}{route}", self.host, self.port, self.database)
    }
}

/// How a connection uses TLS, under libpq's names.
#[derive(Debug, Clone, Copy, Default)]
pub struct PostgresTls<'a> {
    pub mode: SslMode,
    /// The CA certificates (PEM) to trust instead of the public roots built into SQLLoad; with `require` it also turns on chain checks, as in libpq.
    pub root_cert: Option<&'a Path>,
    /// A client certificate chain (PEM), with `key`.
    pub cert: Option<&'a Path>,
    /// The client certificate's private key (PEM).
    pub key: Option<&'a Path>,
}

/// libpq's `sslmode`: whether to encrypt and what to check of the server's certificate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SslMode {
    /// Plain text only.
    Disable,
    /// TLS when the server offers it, plain text otherwise; the certificate is not checked.
    #[default]
    Prefer,
    /// TLS or no connection; the certificate is not checked (unless a root certificate is set).
    Require,
    /// TLS, with a certificate signed by a trusted CA.
    VerifyCa,
    /// TLS, with a certificate signed by a trusted CA for the host connected to.
    VerifyFull,
}

/// What protects a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encryption {
    /// Plain text.
    None,
    /// TLS, without checking the server's certificate.
    Unchecked,
    /// TLS, with a certificate signed by a trusted CA.
    CaChecked,
    /// TLS, with a certificate signed by a trusted CA for the host connected to.
    HostChecked,
}

impl std::fmt::Display for Encryption {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::None => "not encrypted",
            Self::Unchecked => "TLS, certificate not checked",
            Self::CaChecked => "TLS, CA checked",
            Self::HostChecked => "TLS, CA and host name checked",
        })
    }
}

/// What to read from one table.
#[derive(Debug)]
pub struct CopyRequest<'a> {
    pub table: &'a SourceTable,
    pub columns: Vec<ReadColumn>,
    /// A condition such as `"updated_at" >= '2026-11-04 09:12:03+00'`, already rendered and quoted.
    pub filter: Option<String>,
    /// `ORDER BY` this column (a quoted name), for reads that commit along the way in cursor order.
    pub order_by: Option<String>,
    pub batch_rows: usize,
    /// A batch ends once its rows hold about this many bytes, even with fewer than `batch_rows` rows.
    pub batch_bytes: usize,
    /// More connections to the same database, idle and outside any transaction, to read a large table in parallel; empty reads in one pass.
    pub helpers: &'a [tokio_postgres::Client],
}
