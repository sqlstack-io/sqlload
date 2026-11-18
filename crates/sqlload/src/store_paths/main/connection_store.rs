//! The store a connection's files are in.

use crate::project::models::ConnectionConfig;
use crate::ssh_tunnel::models::SshTunnel;
use crate::store_paths::models::StoreKind;

/// The store `connection` holds files in: an `s3` or `gcs` bucket, a `filesystem` root, or an `sftp` server below the login folder; `None` for a connection to anything else.
pub fn connection_store(connection: &ConnectionConfig) -> Option<StoreKind> {
    Some(match connection {
        ConnectionConfig::S3 {
            bucket,
            region,
            endpoint,
            profile,
        } => StoreKind::S3 {
            bucket: bucket.clone(),
            region: region.clone(),
            endpoint: endpoint.clone(),
            profile: profile.clone(),
        },
        ConnectionConfig::Gcs {
            bucket,
            service_account_key,
            endpoint,
        } => StoreKind::Gcs {
            bucket: bucket.clone(),
            service_account_key: service_account_key.clone(),
            endpoint: endpoint.clone(),
        },
        ConnectionConfig::Filesystem { root } => StoreKind::Filesystem { root: root.clone() },
        ConnectionConfig::Sftp {
            host,
            port,
            user,
            private_key_file,
            private_key_passphrase,
            password,
            known_hosts,
        } => StoreKind::Sftp {
            server: Box::new(SshTunnel {
                host: host.clone(),
                port: *port,
                user: user.clone(),
                private_key_file: private_key_file.clone(),
                private_key_passphrase: private_key_passphrase.clone(),
                password: password.clone(),
                known_hosts: Some(known_hosts.clone()),
            }),
            root: String::new(),
        },
        _ => return None,
    })
}
