/// How long a relay waits for its client to connect.
pub const RELAY_ACCEPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// How long a loopback relay gives each connection to send its whole startup message, so a silent or slow one is dropped without holding up the client.
pub const RELAY_STARTUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// The shortest startup message: its length and the protocol version.
pub const STARTUP_MESSAGE_MINIMUM: usize = 8;

/// The longest startup message a loopback relay reads: Postgres's own limit (`MAX_STARTUP_PACKET_LENGTH`).
pub const STARTUP_MESSAGE_LIMIT: usize = 10_000;

/// Protocol 3.0, as the replication client's startup message gives it.
pub const PROTOCOL_VERSION: [u8; 4] = [0, 3, 0, 0];

/// The application name SQLLoad's sessions show the server.
pub const APPLICATION_NAME: &str = "sqlload";
