//! Signing in to Google APIs: Google's application default credentials (a service-account key, a gcloud login, the metadata server, an impersonated service account, workload identity federation), and the access tokens each gives.

pub(crate) mod _helpers;
pub mod constants;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
