#![cfg(feature = "bloom")]

mod support;

use crate::support::*;
use assert_matches::assert_matches;
use redis::bloom::{
    BloomFilterDumpChunk, BloomFilterDumpIterator, BloomFilterInfoType, BloomFilterInsertOptions,
    BloomFilterScalingOptions,
};
use redis::{TypedCommands, ValueType};
use redis_test::server::Module;
use std::vec;

// Test keys
const KEY_1: &str = "test_bloom_1";
const KEY_2: &str = "test_bloom_2";
const KEY_3: &str = "test_bloom_3";

/// Tries to assure single value updates work
#[test]
fn test_module_bloom_single_value_updates() {
    let ctx = TestContext::with_modules(&[Module::Bloom]);
    let mut con = ctx.connection();

    // Add a single value, and check containment
    assert_eq!(con.bf_add(KEY_1, "foo"), Ok(true));
    assert_eq!(
        con.bf_mexists(KEY_1, &["foo", "bar", "baz"]),
        Ok(vec![true, false, false])
    );

    // Add another, single value, and re-check containment
    assert_eq!(con.bf_add(KEY_1, "bar"), Ok(true));
    assert_eq!(
        con.bf_mexists(KEY_1, &["foo", "bar", "baz"]),
        Ok(vec![true, true, false])
    );

    // Check that adding the first value flags that adding failed, and re-check containment
    assert_eq!(con.bf_add(KEY_1, "foo"), Ok(false));
    assert_eq!(
        con.bf_mexists(KEY_1, &["foo", "bar", "baz"]),
        Ok(vec![true, true, false])
    );

    // Checking that adding to a non-Bloom filter key fails gracefully and does not break the key
    assert_eq!(con.set(KEY_2, "quux"), Ok(()));
    assert_eq!(
        con.bf_add(KEY_2, "foo").unwrap_err().code(),
        Some("WRONGTYPE")
    );
    assert_eq!(con.get(KEY_2), Ok(Some("quux".to_string())));
}

/// Tries to assure that multi-value updates work
#[test]
fn test_module_bloom_multiple_value_updates() {
    let ctx = TestContext::with_modules(&[Module::Bloom]);
    let mut con = ctx.connection();

    // Init the key by adding multiple values at once
    // `foo` occurs twice. The first occurence should work, the second fail, as it already exists.
    assert_eq!(
        con.bf_madd(KEY_1, &["foo", "foo", "bar"]),
        Ok(vec![true, false, true])
    );
    assert_eq!(con.bf_mexists(KEY_1, &["foo", "bar"]), Ok(vec![true, true]));

    // Insert more values (`bar` was already added before, `baz` is new)
    assert_eq!(con.bf_insert(KEY_1, &["bar", "baz"]), Ok(vec![false, true]));
    assert_eq!(
        con.bf_mexists(KEY_1, &["foo", "bar", "baz"]),
        Ok(vec![true, true, true])
    );

    // Inserting to a non-existing key while prohibiting key creation should fail
    let options = BloomFilterInsertOptions::default().nocreate();
    assert_matches!(con.bf_insert_options(KEY_2, &["foo"], options).unwrap_err().detail(), Some(d) if d.contains("not found"));

    // Insert to a non-existing key while allowing key creation
    let options = BloomFilterInsertOptions::default()
        .capacity(2)
        .expansion(BloomFilterScalingOptions::NonScaling);
    assert_eq!(
        con.bf_insert_options(KEY_3, &["foo", "bar"], options),
        Ok(vec![true, true])
    );

    // Adding/Inserting to a full Bloom filter should fail (but not change existing values)
    assert_matches!(con.bf_madd(KEY_3, &["baz"]).unwrap_err().detail(), Some(d) if d.contains("full"));
    assert_eq!(
        con.bf_mexists(KEY_3, &["foo", "bar", "baz"]),
        Ok(vec![true, true, false])
    );
}

/// Tries to assure that information functions work
#[test]
fn test_module_bloom_infos() {
    let ctx = TestContext::with_modules(&[Module::Bloom]);
    let mut con = ctx.connection();

    // Check that getting infos on a not yet existing key does not panic
    assert_eq!(con.bf_card(KEY_1), Ok(0));
    assert_eq!(con.key_type(KEY_1), Ok(ValueType::None));
    assert_matches!(con.bf_info(KEY_1).unwrap_err().detail(), Some(d) if d.contains("not found"));
    assert_matches!(con.bf_info_type(KEY_1, BloomFilterInfoType::Items).unwrap_err().detail(), Some(d) if d.contains("not found"));
    assert_eq!(con.bf_exists(KEY_1, "foo"), Ok(false));
    // As of 2026-04-16, the following command should produce an error according to
    // https://redis.io/docs/latest/commands/bf.mexists/
    // as the key is missing. But instead it returns a proper array result
    assert_eq!(
        con.bf_mexists(KEY_1, &["foo", "bar", "baz"]),
        Ok(vec![false, false, false])
    );

    // Add a single value and check its infos
    assert_eq!(con.bf_add(KEY_1, "foo"), Ok(true));
    assert_eq!(con.bf_card(KEY_1), Ok(1));
    let bf_type = con.key_type(KEY_1).unwrap();
    if ctx.supports(REDIS_BLOOM_ANY) {
        assert_eq!(bf_type, ValueType::BloomFilterRedis);
    } else {
        assert_eq!(bf_type, ValueType::BloomFilterValKey);
    }
    assert_eq!(
        con.bf_info(KEY_1)
            .unwrap()
            .get("Number of items inserted")
            .unwrap(),
        &1.
    );
    assert_eq!(
        con.bf_info_type(KEY_1, BloomFilterInfoType::Items).unwrap(),
        1.
    );
    assert_eq!(con.bf_exists(KEY_1, "foo"), Ok(true));
    assert_eq!(con.bf_exists(KEY_1, "bar"), Ok(false));
    assert_eq!(
        con.bf_mexists(KEY_1, &["foo", "bar", "baz"]),
        Ok(vec![true, false, false])
    );

    // Add a second value and check its infos
    assert_eq!(con.bf_add(KEY_1, "bar"), Ok(true));
    assert_eq!(con.bf_card(KEY_1), Ok(2));
    assert_eq!(con.key_type(KEY_1), Ok(bf_type.clone()));
    assert_eq!(
        con.bf_info(KEY_1)
            .unwrap()
            .get("Number of items inserted")
            .unwrap(),
        &2.
    );
    assert_eq!(
        con.bf_info_type(KEY_1, BloomFilterInfoType::Items).unwrap(),
        2.
    );
    assert_eq!(con.bf_exists(KEY_1, "foo"), Ok(true));
    assert_eq!(con.bf_exists(KEY_1, "bar"), Ok(true));
    assert_eq!(con.bf_exists(KEY_1, "baz"), Ok(false));
    assert_eq!(
        con.bf_mexists(KEY_1, &["foo", "bar", "baz"]),
        Ok(vec![true, true, false])
    );

    // Adding the first value again should not change infos, as that value was already added before.
    assert_eq!(con.bf_add(KEY_1, "foo"), Ok(false));
    assert_eq!(con.bf_card(KEY_1), Ok(2));
    assert_eq!(con.key_type(KEY_1), Ok(bf_type.clone()));
    assert_eq!(
        con.bf_info(KEY_1)
            .unwrap()
            .get("Number of items inserted")
            .unwrap(),
        &2.
    );
    assert_eq!(
        con.bf_info_type(KEY_1, BloomFilterInfoType::Items).unwrap(),
        2.
    );
    assert_eq!(con.bf_exists(KEY_1, "foo"), Ok(true));
    assert_eq!(con.bf_exists(KEY_1, "bar"), Ok(true));
    assert_eq!(con.bf_exists(KEY_1, "baz"), Ok(false));
    assert_eq!(
        con.bf_mexists(KEY_1, &["foo", "bar", "baz"]),
        Ok(vec![true, true, false])
    );

    // Check that getting infos on a not-Bloom-filter key does not panic or change its value
    assert_eq!(con.set(KEY_2, "quux"), Ok(()));
    assert_eq!(con.bf_card(KEY_2).unwrap_err().code(), Some("WRONGTYPE"));
    assert_eq!(con.bf_info(KEY_2).unwrap_err().code(), Some("WRONGTYPE"));
    assert_eq!(
        con.bf_info_type(KEY_2, BloomFilterInfoType::Items)
            .unwrap_err()
            .code(),
        Some("WRONGTYPE")
    );
    let res_exists = con.bf_exists(KEY_2, "foo");
    let res_mexists = con.bf_mexists(KEY_2, &["foo", "bar", "baz"]);
    // We check whether we run Redis' and Valkey's `bloom` module, as they differ in how they react
    // to non-Bloom filter keys.
    if ctx.supports(REDIS_BLOOM_ANY) {
        assert_eq!(res_exists, Ok(false));
        // As of 2026-04-16, the following command should produce an error according to
        // https://redis.io/docs/latest/commands/bf.mexists/
        // as the key is of the wrong type. But instead it returns a proper array result
        assert_eq!(res_mexists, Ok(vec![false, false, false]));
    } else {
        assert_eq!(res_exists.unwrap_err().code(), Some("WRONGTYPE"));
        assert_eq!(res_mexists.unwrap_err().code(), Some("WRONGTYPE"));
    }
    // Check that the value of the non-Bloom filter key did not change
    assert_eq!(con.get(KEY_2), Ok(Some("quux".to_string())));
}

/// Tries to assure that reserving is effective
#[test]
fn test_module_bloom_reserving() {
    let ctx = TestContext::with_modules(&[Module::Bloom]);
    let mut con = ctx.connection();

    // Reserving without options
    assert_eq!(con.bf_reserve(KEY_1, 0.4711, 42), Ok(()));
    assert_eq!(
        con.bf_info_type(KEY_1, BloomFilterInfoType::Capacity)
            .unwrap(),
        42.
    );

    // Reserving with options
    let options = BloomFilterScalingOptions::ExpansionRate(23);
    assert_eq!(con.bf_reserve_options(KEY_2, 0.4711, 42, options), Ok(()));
    assert_eq!(
        con.bf_info_type(KEY_2, BloomFilterInfoType::Capacity)
            .unwrap(),
        42.
    );
    assert_eq!(
        con.bf_info_type(KEY_2, BloomFilterInfoType::Expansion)
            .unwrap(),
        23.
    );

    // Reserving for an existing Bloom filter should fail
    assert_matches!(con.bf_reserve(KEY_2, 0.4711, 42).unwrap_err().detail(), Some(d) if d.contains("exists"));

    // Reserving for a key with different value should fail
    assert_eq!(con.set(KEY_3, "quux"), Ok(()));
    assert_eq!(
        con.bf_reserve(KEY_3, 0.4711, 42).unwrap_err().code(),
        Some("WRONGTYPE")
    );
    assert_eq!(con.get(KEY_3), Ok(Some("quux".to_string())));
}

/// Tries to assure that dumping/loading works
#[test]
fn test_module_bloom_dump_and_load() {
    let ctx = TestContext::with_modules(&[Module::Bloom]);
    skip_if_context_does_not_support!(ctx, REDIS_BLOOM_ANY);
    let mut con = ctx.connection();

    // Create a bloom filter with two elements
    let options = BloomFilterInsertOptions::default()
        .capacity(42)
        .error_rate(0.004711)
        .expansion(BloomFilterScalingOptions::ExpansionRate(23));
    assert_eq!(
        con.bf_insert_options(KEY_1, &["foo", "bar"], options),
        Ok(vec![true, true])
    );

    // Get the filter's info to check against after loading
    let original_info = con.bf_info(KEY_1).unwrap();

    // Dump the filter
    let mut iterator: i64 = 0;
    let mut chunks: Vec<BloomFilterDumpChunk> = Vec::new();
    loop {
        let dump = con.bf_scandump(KEY_1, iterator).unwrap();

        // Check for end of dump
        if dump.iterator == 0 {
            break;
        }

        // Collect the dumped data
        iterator = dump.iterator;
        chunks.push(dump);
    }

    // Delete the original filter to make sure there is no re-use for the upcoming loading
    let deleted = con.del(KEY_1).unwrap();
    assert_eq!(deleted, 1);

    // Load the dumped data to new filter at a different key
    for chunk in chunks {
        con.bf_loadchunk(KEY_2, chunk).unwrap();
    }

    // Assure the original filter's items are present in the loaded filter
    let results: Vec<bool> = con.bf_mexists(KEY_2, &["foo", "bar"]).unwrap();
    assert_eq!(results, vec![true, true]);

    // Assure the original and loaded filter have the same metadata
    let loaded_info = con.bf_info(KEY_2).unwrap();
    assert_eq!(loaded_info, original_info);
}

/// Tries to assure that dumping through an iterator works
#[test]
fn test_module_bloom_dump_iterator() {
    let ctx = TestContext::with_modules(&[Module::Bloom]);
    skip_if_context_does_not_support!(ctx, REDIS_BLOOM_ANY);
    let mut con = ctx.connection();

    // Create a bloom filter with two elements
    let options = BloomFilterInsertOptions::default()
        .capacity(42)
        .error_rate(0.004711)
        .expansion(BloomFilterScalingOptions::ExpansionRate(23));
    assert_eq!(
        con.bf_insert_options(KEY_1, &["foo", "bar"], options),
        Ok(vec![true, true])
    );

    // Get the filter's info to check against after loading
    let original_info = con.bf_info(KEY_1).unwrap();

    // Grab the iterator
    let dump_iterator = BloomFilterDumpIterator::new(&mut con, KEY_1);

    // Walk the iterator and collect the chunks to load them later, while bailing out upon errors
    let chunks = dump_iterator.map(|r| r.unwrap()).collect::<Vec<_>>();

    // Delete the original filter to make sure there is no re-use for the upcoming loading
    let deleted = con.del(KEY_1).unwrap();
    assert_eq!(deleted, 1);

    // Load the dumped data to new filter at a different key
    for chunk in chunks {
        con.bf_loadchunk(KEY_2, chunk).unwrap();
    }

    // Assure the original filter's items are present in the loaded filter
    let results: Vec<bool> = con.bf_mexists(KEY_2, &["foo", "bar"]).unwrap();
    assert_eq!(results, vec![true, true]);

    // Assure the original and loaded filter have the same metadata
    let loaded_info = con.bf_info(KEY_2).unwrap();
    assert_eq!(loaded_info, original_info);
}
