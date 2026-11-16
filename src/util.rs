//! Small helpers shared across modules.

/// Cut `s` to at most `max` characters, adding an ellipsis if anything was
/// dropped. Used to keep server response bodies short in error messages.
pub(crate) fn truncate(s: &str, max: usize) -> String {
    match s.char_indices().nth(max) {
        Some((i, _)) => format!("{}…", &s[..i]),
        None => s.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::truncate;

    #[test]
    fn keeps_short_strings() {
        assert_eq!(truncate("abc", 3), "abc");
    }

    #[test]
    fn cuts_on_char_boundaries() {
        assert_eq!(truncate("añbc", 2), "añ…");
    }
}
