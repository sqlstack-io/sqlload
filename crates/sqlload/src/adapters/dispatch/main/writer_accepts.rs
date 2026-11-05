//! What a destination takes.

use crate::adapters::dispatch::models::Destination;
use crate::adapters::snowflake::main::stage::_writer_accepts::writer_accepts as snowflake_accepts;
use crate::adapters::snowflake::models::StageRoute;
use crate::routing::models::WriterAccepts;

/// What `destination` takes over the chosen `stage`: Arrow batches, or what its stage route takes.
pub fn writer_accepts(destination: &Destination, stage: Option<&StageRoute>) -> WriterAccepts {
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(_) => WriterAccepts {
            arrow: true,
            ..WriterAccepts::default()
        },
        Destination::Files(_) => WriterAccepts {
            arrow: true,
            ..WriterAccepts::default()
        },
        Destination::Snowflake(_) => snowflake_accepts(stage),
    }
}
