/// A file the playground writes: its path below the project, and its text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BundledFile {
    pub path: &'static str,
    pub text: &'static str,
}
