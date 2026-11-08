use std::time::Duration;

/// The bastion's port when the `ssh` table sets none.
pub const SSH_PORT: u16 = 22;

/// How long to wait for the bastion to answer and sign the client in.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// How often an idle tunnel asks the bastion whether it is still there, so long reads and `load --follow` survive idle firewalls.
pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);

/// Unanswered keepalives after which the tunnel counts as dropped.
pub const KEEPALIVE_MAX: usize = 3;
