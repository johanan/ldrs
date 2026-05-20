use std::collections::HashSet;

use schemars::{schema_for, JsonSchema};
use serde_yaml::Value;

#[derive(Debug, PartialEq, Clone)]
pub struct UnknownKey {
    pub key: String,
    pub suggestions: Vec<String>,
}

/// Walk a YAML Mapping's top-level keys and return any that aren't in `allowed`.
/// Keys like `pq.filename` are stripped to their final segment before comparison;
/// the original full key is preserved in `UnknownKey::key` for display.
/// Non-Mapping inputs yield an empty vec.
pub fn find_unknown_keys(value: &Value, allowed: &HashSet<&str>) -> Vec<UnknownKey> {
    let Value::Mapping(map) = value else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (k, _) in map {
        let Some(full) = k.as_str() else { continue };
        let bare = full
            .rsplit_once('.')
            .map(|(_, suffix)| suffix)
            .unwrap_or(full);
        if allowed.contains(bare) {
            continue;
        }
        let suggestions = close_matches(bare, allowed)
            .into_iter()
            .map(String::from)
            .collect();
        out.push(UnknownKey {
            key: full.to_string(),
            suggestions,
        });
    }
    out
}

/// Top-level field names defined on `T` according to its schemars-derived schema.
/// `#[serde(flatten)]` fields are surfaced inline in schemars 1.x output, so a
pub fn extract_props<T: JsonSchema>() -> Vec<String> {
    let schema = schema_for!(T);
    let value = schema.to_value();
    value
        .as_object()
        .and_then(|o| o.get("properties"))
        .and_then(|p| p.as_object())
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default()
}

/// All candidates within Levenshtein distance 2 of `needle`, sorted by distance
/// then lexicographically.
fn close_matches<'a>(needle: &str, allowed: &HashSet<&'a str>) -> Vec<&'a str> {
    let mut hits: Vec<(&'a str, usize)> = allowed
        .iter()
        .map(|c| (*c, levenshtein(needle, c)))
        .filter(|(_, d)| *d <= 2)
        .collect();
    hits.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(b.0)));
    hits.into_iter().map(|(c, _)| c).collect()
}

fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let (n, m) = (a.len(), b.len());
    if n == 0 {
        return m;
    }
    if m == 0 {
        return n;
    }
    let mut prev: Vec<usize> = (0..=m).collect();
    let mut curr = vec![0usize; m + 1];
    for i in 1..=n {
        curr[0] = i;
        for j in 1..=m {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            curr[j] = (curr[j - 1] + 1).min(prev[j] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[m]
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[allow(dead_code)]
    struct ExampleStruct {
        foo: String,
        bar: Option<i32>,
        baz: Vec<String>,
    }

    #[test]
    fn extract_props_returns_struct_fields() {
        let mut got = extract_props::<ExampleStruct>();
        got.sort();
        assert_eq!(
            got,
            vec!["bar".to_string(), "baz".to_string(), "foo".to_string()]
        );
    }

    #[test]
    fn levenshtein_basics() {
        assert_eq!(levenshtein("", ""), 0);
        assert_eq!(levenshtein("", "abc"), 3);
        assert_eq!(levenshtein("abc", ""), 3);
        assert_eq!(levenshtein("abc", "abc"), 0);
        assert_eq!(levenshtein("kitten", "sitting"), 3);
        assert_eq!(levenshtein("filename", "fileanme"), 2);
        assert_eq!(levenshtein("merge_keys", "mergekeys"), 1);
    }

    #[test]
    fn close_matches_returns_within_threshold_sorted() {
        let allowed: HashSet<&str> = ["filename", "name", "columns"].into_iter().collect();
        assert_eq!(close_matches("fileanme", &allowed), vec!["filename"]);
        assert_eq!(close_matches("nme", &allowed), vec!["name"]);
    }

    #[test]
    fn close_matches_far_miss_returns_empty() {
        let allowed: HashSet<&str> = ["filename"].into_iter().collect();
        assert_eq!(close_matches("xyzzy", &allowed), Vec::<&str>::new());
    }

    #[test]
    fn close_matches_empty_allowed_returns_empty() {
        let allowed: HashSet<&str> = HashSet::new();
        assert_eq!(close_matches("filename", &allowed), Vec::<&str>::new());
    }

    #[test]
    fn close_matches_multiple_candidates_sorted_by_distance_then_lex() {
        // "abc" → distance 1 from "abcd" (insert) and "xbc" (substitute), distance 0 from "abc".
        let allowed: HashSet<&str> = ["abcd", "xbc", "abc"].into_iter().collect();
        assert_eq!(close_matches("abc", &allowed), vec!["abc", "abcd", "xbc"]);
    }

    #[test]
    fn find_unknown_keys_non_mapping_is_empty() {
        let allowed: HashSet<&str> = ["filename"].into_iter().collect();
        let value: Value = serde_yaml::from_str("- 1\n- 2").unwrap();
        assert!(find_unknown_keys(&value, &allowed).is_empty());
    }

    #[test]
    fn find_unknown_keys_known_key_passes() {
        let allowed: HashSet<&str> = ["filename", "name"].into_iter().collect();
        let value: Value = serde_yaml::from_str("filename: x.parquet\nname: foo").unwrap();
        assert!(find_unknown_keys(&value, &allowed).is_empty());
    }

    #[test]
    fn find_unknown_keys_flags_with_suggestion() {
        let allowed: HashSet<&str> = ["filename", "name"].into_iter().collect();
        let value: Value = serde_yaml::from_str("fileanme: y.parquet").unwrap();
        assert_eq!(
            find_unknown_keys(&value, &allowed),
            vec![UnknownKey {
                key: "fileanme".to_string(),
                suggestions: vec!["filename".to_string()],
            }],
        );
    }

    #[test]
    fn find_unknown_keys_flags_without_suggestion() {
        let allowed: HashSet<&str> = ["filename"].into_iter().collect();
        let value: Value = serde_yaml::from_str("xyzzy: 1").unwrap();
        assert_eq!(
            find_unknown_keys(&value, &allowed),
            vec![UnknownKey {
                key: "xyzzy".to_string(),
                suggestions: vec![],
            }],
        );
    }

    #[test]
    fn find_unknown_keys_strips_prefix_before_check() {
        let allowed: HashSet<&str> = ["filename"].into_iter().collect();
        let value: Value = serde_yaml::from_str("pq.filename: x").unwrap();
        assert!(find_unknown_keys(&value, &allowed).is_empty());
    }

    #[test]
    fn find_unknown_keys_strips_only_last_segment() {
        let allowed: HashSet<&str> = ["merge_keys"].into_iter().collect();
        let value: Value = serde_yaml::from_str("delta.merge.merge_keys: [id]").unwrap();
        assert!(find_unknown_keys(&value, &allowed).is_empty());
    }

    #[test]
    fn find_unknown_keys_preserves_full_key_in_finding() {
        let allowed: HashSet<&str> = ["filename"].into_iter().collect();
        let value: Value = serde_yaml::from_str("pq.fileanme: x").unwrap();
        assert_eq!(
            find_unknown_keys(&value, &allowed),
            vec![UnknownKey {
                key: "pq.fileanme".to_string(),
                suggestions: vec!["filename".to_string()],
            }],
        );
    }
}
