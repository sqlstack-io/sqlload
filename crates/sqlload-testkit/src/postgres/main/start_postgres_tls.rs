//! Start Postgres that accepts TLS connections only.

use crate::postgres::_helpers::tls::{certificates, tls_container};
use crate::postgres::main::connect::connect;
use crate::postgres::models::TlsPostgres;

/// A Postgres server, once it accepts connections, with logical replication enabled, whose `pg_hba.conf` refuses every connection without TLS, its certificate for `localhost` signed by a test CA; user `CERT_USER` must also present a client certificate. The CA, an unrelated CA and that client certificate are written to a temporary folder.
pub async fn start_postgres_tls() -> Result<TlsPostgres, String> {
    let (files, server) = certificates()?;
    let postgres = tls_container(server).await?;
    connect(&postgres).await?;
    Ok(TlsPostgres { postgres, files })
}
