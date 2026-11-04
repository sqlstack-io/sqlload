//! Choose the TLS crypto provider.

/// Install ring as the process's rustls provider; every HTTP client, ours or a library's, uses it.
pub fn install_tls_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
