pub const IMAGE: &str = "postgres";
pub const TAG: &str = "17-alpine";
pub const USER: &str = "sqlload";
pub const PASSWORD: &str = "sqlload";
pub const DATABASE: &str = "app";
/// Connection attempts while the server finishes starting.
pub const CONNECT_ATTEMPTS: usize = 60;
pub const CONNECT_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(500);
/// The user that must present a client certificate on the TLS-only server.
pub const CERT_USER: &str = "certified";
/// The TLS-only server's `pg_hba.conf`: TLS for every TCP connection, replication included, and a client certificate for [`CERT_USER`].
pub const TLS_ONLY_HBA: &str = "local all all trust
hostssl all certified all scram-sha-256 clientcert=verify-full
hostssl all all all scram-sha-256
hostssl replication all all scram-sha-256
";
/// The TLS-only server's start: the key readable by the server alone, as Postgres requires, then the image's entrypoint with TLS on.
pub const TLS_START: &str = "mkdir -p /tls && cp /tls-in/* /tls/ && chown postgres:postgres /tls/* && chmod 600 /tls/server.key && exec docker-entrypoint.sh postgres -c wal_level=logical -c ssl=on -c ssl_cert_file=/tls/server.pem -c ssl_key_file=/tls/server.key -c ssl_ca_file=/tls/ca.pem -c hba_file=/tls/pg_hba.conf";
/// Checks for a free replication slot before a load starts, [`SLOT_RETRY_DELAY`] apart: a minute.
pub const SLOT_ATTEMPTS: usize = 600;
pub const SLOT_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(100);
