use crate::row_ids::main::key_row_ids::key_row_ids;
use crate::row_ids::main::random_row_ids::random_row_ids;
use crate::row_ids::tests::helpers::{
    escaped_text, floats, instant_and_negative, one_integer, two_texts,
};
use crate::row_ids::tests::test_types::{KeyRowIdsTestCase, RandomRowIdsTestCase};

// Every expected id is base64(shake_128(text).digest(10)) without padding, computed with Python's hashlib from the text orjson writes for the key (checked with orjson 3.11): dlt 1.32.0's digest128b over its key hash.

#[test]
fn given_merge_keys_when_deriving_root_row_ids_then_they_match_dlts_key_hash() {
    let test_cases = [
        KeyRowIdsTestCase {
            description: "an integer key: {\"id\":1}, the same id for the same key",
            batch: one_integer,
            key: &[("id", 0)],
            expected_ids: &["z5jIxXIbUnmbKw", "z5jIxXIbUnmbKw"],
        },
        KeyRowIdsTestCase {
            description: "a two-column key is hashed with its names sorted, not in key or batch order: {\"id\":\"a\",\"region\":\"eu\"}",
            batch: two_texts,
            key: &[("region", 0), ("id", 1)],
            expected_ids: &["TCYcFZTzIsyu7A"],
        },
        KeyRowIdsTestCase {
            description: "an instant as orjson writes a UTC datetime, with its microseconds, beside a negative integer: {\"at\":\"2026-01-02T03:04:05.000006+00:00\",\"id\":-7}",
            batch: instant_and_negative,
            key: &[("id", 0), ("at", 1)],
            expected_ids: &["QpvSNQr/JtZ6tg"],
        },
        KeyRowIdsTestCase {
            description: "text keeps non-ASCII as UTF-8 and escapes a quote: {\"id\":\"é\\\"x\"}",
            batch: escaped_text,
            key: &[("id", 0)],
            expected_ids: &["WuIes0a9JyISbQ"],
        },
        KeyRowIdsTestCase {
            description: "floats as orjson writes them, decimal from 1e-5 to below 1e16: {\"id\":1.0}, 1e20, null for NaN, 0.00001, 1.5e-7, 9999999999999998.0, -0.0",
            batch: floats,
            key: &[("id", 0)],
            expected_ids: &[
                "cvXT4XHjvZjFxg",
                "wVvTTQGv5Kgk/g",
                "ljdFSlj0Tv4BqQ",
                "/xfddsYNuBNu8A",
                "niawGqL3zSjZXQ",
                "xxUDOy480OnYog",
                "skUCgLHpatPvkg",
            ],
        },
    ];
    for test_case in &test_cases {
        let key: Vec<(String, usize)> = test_case
            .key
            .iter()
            .map(|(name, index)| ((*name).to_owned(), *index))
            .collect();
        assert_eq!(
            key_row_ids(&(test_case.batch)(), &key),
            Ok(test_case
                .expected_ids
                .iter()
                .map(|id| (*id).to_owned())
                .collect()),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_root_rows_without_a_merge_key_when_giving_them_ids_then_each_is_fourteen_base64_characters_and_new()
-> Result<(), String> {
    let test_cases = [
        RandomRowIdsTestCase {
            description: "three rows: three different ids of 14 base64 characters",
            rows: 3,
            expected_distinct: 3,
            expected_lengths: &[14, 14, 14],
        },
        RandomRowIdsTestCase {
            description: "no rows: no ids",
            rows: 0,
            expected_distinct: 0,
            expected_lengths: &[],
        },
    ];
    for test_case in &test_cases {
        let ids = random_row_ids(test_case.rows)?;
        let distinct: std::collections::BTreeSet<&String> = ids.iter().collect();
        let lengths: Vec<usize> = ids.iter().map(String::len).collect();
        let base64 = ids.iter().all(|id| {
            id.chars().all(|character| {
                character.is_ascii_alphanumeric() || character == '+' || character == '/'
            })
        });
        assert_eq!(
            (distinct.len(), lengths, base64),
            (
                test_case.expected_distinct,
                test_case.expected_lengths.to_vec(),
                true
            ),
            "{}",
            test_case.description
        );
    }
    Ok(())
}
