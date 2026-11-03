/// TOML lines added to an `s3` test configuration, by section.
#[derive(Debug, Clone, Copy, Default)]
pub struct S3Settings<'a> {
    /// Inside `[staging]`, e.g. `load = "copy"`.
    pub staging: &'a str,
    /// Inside `[staging.s3]`, e.g. a `path` template.
    pub s3: &'a str,
    /// Whole sections after the staging ones.
    pub extra: &'a str,
}
