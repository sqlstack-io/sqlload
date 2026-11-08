use testcontainers::{ContainerAsync, GenericImage};

/// A running bastion and the files a connection names to reach it; the container stops when this is dropped.
#[derive(Debug)]
pub struct Bastion {
    /// The host port sshd answers on, at 127.0.0.1.
    pub port: u16,
    /// `client.key` (accepted), `other.key` (unknown to the bastion), `known_hosts` (the bastion's real host keys), `changed_hosts` (another Ed25519 key in place of the bastion's) and `empty_hosts`.
    pub files: tempfile::TempDir,
    /// The bastion's own Docker network: names on it resolve at the bastion only, so a connection that reaches them went through the tunnel.
    pub network: String,
    pub container: ContainerAsync<GenericImage>,
}
