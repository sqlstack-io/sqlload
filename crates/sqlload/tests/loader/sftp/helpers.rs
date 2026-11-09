use sqlload_testkit::ssh::models::Bastion;

use crate::helpers::{gzip, parquet_bytes};
use crate::test_types::ParquetFile;

/// Two orders, gzip CSV.
pub(crate) fn first_orders() -> Result<bytes::Bytes, String> {
    gzip("id,name\n1,ann\n2,bob\n")
}

/// A third order.
pub(crate) fn second_orders() -> Result<bytes::Bytes, String> {
    gzip("id,name\n3,cy\n")
}

/// The first file rewritten with a fourth order.
pub(crate) fn rewritten_orders() -> Result<bytes::Bytes, String> {
    gzip("id,name\n1,ann\n2,bob\n4,dee\n")
}

/// Two events, Parquet.
pub(crate) fn first_events() -> Result<bytes::Bytes, String> {
    parquet_bytes(&ParquetFile {
        key: "",
        ids: &[1, 2],
        score: 1.5,
        extra: None,
    })
}

/// A third event.
pub(crate) fn second_events() -> Result<bytes::Bytes, String> {
    parquet_bytes(&ParquetFile {
        key: "",
        ids: &[3],
        score: 2.5,
        extra: None,
    })
}

/// The project: gzip CSV orders in a folder below the login folder (written with `~/`), Parquet events by absolute path, both appended by file into DuckDB.
pub(crate) fn project_text(bastion: &Bastion, sign_in: &str, database: &std::path::Path) -> String {
    let sign_in = sign_in.replace("KEYS", &bastion.files.path().display().to_string());
    format!(
        r#"
[connections.partner]
kind = "sftp"
host = "127.0.0.1"
port = {port}
user = "loader"
{sign_in}

[connections.local]
kind = "duckdb"
database = "{database}"

[targets.dev]
connection = "local"
loader_schema = "raw"

[sources.orders]
kind = "files"
description = "Orders the partner drops."
connection = "partner"
path = "~/drop/orders/"
format = "csv"
write_strategy = "append"
[sources.orders.tables.orders]
description = "Test table."
columns = {{ id = "bigint" }}

[sources.events]
kind = "files"
description = "Events the partner drops."
connection = "partner"
path = "/config/drop/events/"
format = "parquet"
write_strategy = "append"
[sources.events.tables.events]
description = "Test table."
"#,
        port = bastion.port,
        database = database.display(),
    )
}

/// A project loading the SFTP orders into the fake Snowflake at `base_url`, over an external stage that covers `s3://fake-bucket/`, with `settings` added to the source.
pub(crate) fn snowflake_project(bastion: &Bastion, base_url: &str, settings: &str) -> String {
    let keys = bastion.files.path().display();
    format!(
        r#"
[connections.partner]
kind = "sftp"
host = "127.0.0.1"
port = {port}
user = "loader"
private_key_file = "{keys}/client.key"
known_hosts = "{keys}/known_hosts"

[connections.wh]
kind = "snowflake"
account = "acct"
user = "loader"
auth = {{ method = "pat", token = "t" }}
database = "DB"
base_url = "{base_url}"
load_method = {{ type = "external_stage", url = "s3://fake-bucket/", storage_integration = "LAKE_INT", region = "us-east-1", endpoint = "{base_url}" }}

[targets.dev]
connection = "wh"
loader_schema = "RAW"

[sources.orders]
kind = "files"
description = "Orders the partner drops."
connection = "partner"
path = "drop/orders/"
format = "csv"
write_strategy = "append"
{settings}
"#,
        port = bastion.port,
    )
}
