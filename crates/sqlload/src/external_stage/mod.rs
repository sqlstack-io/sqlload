//! Snowflake external stages on S3: the stage object, staged Parquet, and `COPY` straight from a files source's own files.

pub(crate) mod _helpers;
pub mod constants;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
