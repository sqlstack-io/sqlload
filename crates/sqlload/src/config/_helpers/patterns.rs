/// True when the glob `pattern` matches `topic`; invalid patterns match nothing.
pub(crate) fn pattern_matches(pattern: &str, topic: &str) -> bool {
    match globset::Glob::new(pattern) {
        Ok(glob) => glob.compile_matcher().is_match(topic),
        Err(_) => false,
    }
}
