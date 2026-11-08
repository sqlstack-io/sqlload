//! The TLS-only test server's certificates and container.

use std::time::Duration;

use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair, KeyUsagePurpose};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

use crate::postgres::constants::{
    CERT_USER, DATABASE, IMAGE, PASSWORD, TAG, TLS_ONLY_HBA, TLS_START, USER,
};
use crate::postgres::models::Postgres;

/// A folder holding `ca.pem`, `other_ca.pem`, `client.pem` and `client.key`, and the server's certificate, key and CA in PEM.
pub(crate) fn certificates() -> Result<(tempfile::TempDir, [String; 3]), String> {
    let ca = authority("sqlload test CA")?;
    let other = authority("unrelated test CA")?;
    let server = leaf(&ca.1, vec!["localhost".to_owned()], "localhost")?;
    let client = leaf(&ca.1, Vec::new(), CERT_USER)?;
    let files = tempfile::tempdir().map_err(|error| error.to_string())?;
    for (name, text) in [
        ("ca.pem", &ca.0),
        ("other_ca.pem", &other.0),
        ("client.pem", &client.0),
        ("client.key", &client.1),
    ] {
        std::fs::write(files.path().join(name), text).map_err(|error| error.to_string())?;
    }
    Ok((files, [server.0, server.1, ca.0]))
}

/// The TLS-only server's container, given its certificate, key and CA in PEM.
pub(crate) async fn tls_container([certificate, key, ca]: [String; 3]) -> Result<Postgres, String> {
    let container = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", USER)
        .with_env_var("POSTGRES_PASSWORD", PASSWORD)
        .with_env_var("POSTGRES_DB", DATABASE)
        .with_copy_to("/tls-in/server.pem", certificate.into_bytes())
        .with_copy_to("/tls-in/server.key", key.into_bytes())
        .with_copy_to("/tls-in/ca.pem", ca.into_bytes())
        .with_copy_to("/tls-in/pg_hba.conf", TLS_ONLY_HBA.as_bytes().to_vec())
        .with_copy_to(
            "/docker-entrypoint-initdb.d/cert_user.sql",
            format!("CREATE ROLE {CERT_USER} LOGIN PASSWORD '{PASSWORD}';").into_bytes(),
        )
        .with_cmd(["sh", "-c", TLS_START])
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .map_err(|error| format!("cannot start Postgres: {error}"))?;
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(|error| error.to_string())?;
    Ok(Postgres {
        host: "127.0.0.1".to_owned(),
        port,
        container,
    })
}

/// A self-signed CA named `name`: its certificate in PEM, and the issuer that signs with it.
fn authority(name: &str) -> Result<(String, Issuer<'static, KeyPair>), String> {
    let key = KeyPair::generate().map_err(|error| error.to_string())?;
    let mut params =
        CertificateParams::new(Vec::<String>::new()).map_err(|error| error.to_string())?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.distinguished_name.push(DnType::CommonName, name);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let certificate = params
        .self_signed(&key)
        .map_err(|error| error.to_string())?;
    Ok((certificate.pem(), Issuer::new(params, key)))
}

/// A certificate for `names` with common name `common_name`, signed by `issuer`: certificate and key in PEM.
fn leaf(
    issuer: &Issuer<'static, KeyPair>,
    names: Vec<String>,
    common_name: &str,
) -> Result<(String, String), String> {
    let key = KeyPair::generate().map_err(|error| error.to_string())?;
    let mut params = CertificateParams::new(names).map_err(|error| error.to_string())?;
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    let certificate = params
        .signed_by(&key, issuer)
        .map_err(|error| error.to_string())?;
    Ok((certificate.pem(), key.serialize_pem()))
}
