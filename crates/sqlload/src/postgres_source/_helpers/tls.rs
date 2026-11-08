//! The rustls settings behind a connection's `sslmode`, `sslrootcert`, `sslcert` and `sslkey`.

use std::path::Path;
use std::sync::Arc;

use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};

use crate::postgres_source::models::{Encryption, PostgresTls, SslMode};

/// What a TLS session under `tls` checks of the server's certificate; as in libpq, `require` checks the chain when a root certificate is set.
pub(crate) fn certificate_check(tls: &PostgresTls<'_>) -> Encryption {
    match (tls.mode, tls.root_cert) {
        (SslMode::VerifyFull, _) => Encryption::HostChecked,
        (SslMode::VerifyCa, _) | (SslMode::Require, Some(_)) => Encryption::CaChecked,
        _ => Encryption::Unchecked,
    }
}

/// The client settings for a TLS session under `tls`, or why they cannot be built (an unreadable certificate file, say).
pub(crate) fn client_config(tls: &PostgresTls<'_>) -> Result<Arc<ClientConfig>, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|error| error.to_string())?;
    let verifier: Arc<dyn ServerCertVerifier> = match certificate_check(tls) {
        Encryption::Unchecked | Encryption::None => Arc::new(Unchecked(provider)),
        check => {
            let roots = Arc::new(trusted_roots(tls.root_cert)?);
            let full = WebPkiServerVerifier::builder_with_provider(roots, provider)
                .build()
                .map_err(|error| error.to_string())?;
            match check {
                Encryption::HostChecked => full,
                _ => Arc::new(AnyHost(full)),
            }
        }
    };
    let builder = builder
        .dangerous()
        .with_custom_certificate_verifier(verifier);
    let config = match (tls.cert, tls.key) {
        (Some(cert), Some(key)) => builder
            .with_client_auth_cert(certificates(cert)?, private_key(key)?)
            .map_err(|error| {
                format!("sslcert and sslkey do not make a client certificate: {error}")
            })?,
        (None, None) => builder.with_no_client_auth(),
        _ => return Err("sslcert and sslkey must be set together".to_owned()),
    };
    Ok(Arc::new(config))
}

/// The CA certificates in `root_cert`, or the public roots built into SQLLoad.
fn trusted_roots(root_cert: Option<&Path>) -> Result<RootCertStore, String> {
    let Some(path) = root_cert else {
        return Ok(RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        });
    };
    let mut roots = RootCertStore::empty();
    let (added, _) = roots.add_parsable_certificates(certificates(path)?);
    match added {
        0 => Err(format!(
            "sslrootcert {} holds no CA certificate",
            path.display()
        )),
        _ => Ok(roots),
    }
}

/// Every certificate in PEM file `path`.
fn certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let read = CertificateDer::pem_file_iter(path)
        .and_then(Iterator::collect::<Result<Vec<_>, _>>)
        .map_err(|error| format!("cannot read certificates from {}: {error}", path.display()))?;
    match read.is_empty() {
        true => Err(format!("{} holds no PEM certificate", path.display())),
        false => Ok(read),
    }
}

/// The private key in PEM file `path` (PKCS#8, PKCS#1 or SEC1).
fn private_key(path: &Path) -> Result<PrivateKeyDer<'static>, String> {
    PrivateKeyDer::from_pem_file(path)
        .map_err(|error| format!("cannot read a private key from {}: {error}", path.display()))
}

/// Accepts any certificate (`prefer`, `require`), as libpq does, while still checking the handshake's signatures.
#[derive(Debug)]
struct Unchecked(Arc<CryptoProvider>);

impl ServerCertVerifier for Unchecked {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            signature,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            signature,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// Checks the chain to a trusted CA but not the host name (`verify-ca`).
#[derive(Debug)]
struct AnyHost(Arc<WebPkiServerVerifier>);

impl ServerCertVerifier for AnyHost {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self
            .0
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
        {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            checked => checked,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls12_signature(message, cert, signature)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls13_signature(message, cert, signature)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}
