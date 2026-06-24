//! (Unit) tests for the support module
//!
//! As the `support` module gets included in most integration tests, adding its unit tests to the
//! `support` module itself would include (and compile and run) them for each integration test
//! module. This is unwarranted. So we instead collect them in this module.

use crate::support::{
    AvailableComponents, Component, REDIS_CE_6_0, TestContext, TestContextVersioning,
};
use redis_test::{MockCmd, MockRedisConnection};

mod support;

fn build_available_components(input: &str) -> AvailableComponents {
    let mut conn = MockRedisConnection::new(vec![MockCmd::new(redis::cmd("INFO"), Ok(input))]);
    AvailableComponents::from(&mut conn)
}

fn assert_components_eq(components: AvailableComponents, mut expected: Vec<Component>) {
    let mut actual: Vec<Component> = (&components).into();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
}

/// Mock implementation of [`TestContextVersioning`] to allow testing default implementations
struct MockTestContext {
    /// The components to return during `get_available_components`
    components: AvailableComponents,
}

impl MockTestContext {
    /// Builds a new instance
    fn new(input: &str) -> Self {
        let components = build_available_components(input);
        Self { components }
    }
}

impl TestContextVersioning for MockTestContext {
    fn get_available_components(&self) -> AvailableComponents {
        self.components.clone()
    }
}

/// Tries to assure that `support` matches the same version
#[test]
fn ctx_supports_exact_match() {
    // The context to check with
    let mock = MockTestContext::new("foo_version: 42.4711.23");

    // Check for support
    assert!(mock.supports(("foo", (42, 4711, 23))));
}

/// Tries to assure that `support` correctly handles a smaller major number
#[test]
fn ctx_supports_major_smaller() {
    // The context to check with
    let mock = MockTestContext::new("foo_version: 42.4711.23");

    // Check for support
    assert!(mock.supports(("foo", (41, 4712, 24))));
}

/// Tries to assure that `support` correctly handles a bigger major number
#[test]
fn ctx_supports_major_bigger() {
    // The context to check with
    let mock = MockTestContext::new("foo_version: 42.4711.23");

    // Check for support
    assert!(!mock.supports(("foo", (43, 4710, 22))));
}

/// Tries to assure that `support` correctly handles a matching major but bigger minor number
#[test]
fn ctx_supports_major_match_minor_smaller() {
    // The context to check with
    let mock = MockTestContext::new("foo_version: 42.4711.23");

    // Check for support
    assert!(mock.supports(("foo", (42, 4710, 24))));
}

/// Tries to assure that `support` correctly handles a matching major but smaller minor number
#[test]
fn ctx_supports_major_match_minor_bigger() {
    // The context to check with
    let mock = MockTestContext::new("foo_version: 42.4711.23");

    // Check for support
    assert!(!mock.supports(("foo", (42, 4712, 22))));
}

/// Tries to assure that `support` correctly handles a matching major and minor but bigger patch number
#[test]
fn ctx_supports_major_match_minor_match_patch_smaller() {
    // The context to check with
    let mock = MockTestContext::new("foo_version: 42.4711.23");

    // Check for support
    assert!(mock.supports(("foo", (42, 4711, 22))));
}

/// Tries to assure that `support` correctly handles a matching major and minor but smaller patch number
#[test]
fn ctx_supports_major_match_minor_match_patch_bigger() {
    // The context to check with
    let mock = MockTestContext::new("foo_version: 42.4711.23");

    // Check for support
    assert!(!mock.supports(("foo", (42, 4711, 24))));
}

/// Tries to assure that `support` correctly handles a disjunctive match
#[test]
fn ctx_supports_disjunctive() {
    // The context to check with
    let mock = MockTestContext::new("bar_version: 42.4711.23");

    // Check for support when `bar` is `42.0.0` (matches)
    assert!(mock.supports([
        ("foo", (23, 23, 23)),
        ("bar", (42, 0, 0)),
        ("baz", (42, 42, 42)),
    ]));

    // Check for support when `bar` is `43.0.0` (too high)
    assert!(!mock.supports([
        ("foo", (23, 23, 23)),
        ("bar", (43, 0, 0)),
        ("baz", (42, 42, 42)),
    ]));

    // Check for support when `bar` is missing
    assert!(!mock.supports([("foo", (23, 23, 23)), ("baz", (42, 42, 42)),]));
}

/// Tries to assure that `support` correctly handles a conjunctive match
#[test]
fn ctx_supports_conjunctive() {
    // The context to check with
    let input = r#"
foo_version: 42.0.0
bar_version: 4711.0.0
baz_version: 23.0.0
"#;
    let mock = MockTestContext::new(input);

    // Check for successful support
    assert!(mock.supports([
        &[
            ("foo", (43, 0, 0)),  // does not match (too high)
            ("bar", (151, 0, 0))  // matches
        ][..],
        &[("baz", (23, 0, 0))], // matches
    ]));

    // Check for failing support (first disjunctive fails)
    assert!(!mock.supports([
        &[
            ("foo", (43, 0, 0)),   // does not match (too high)
            ("bar", (4712, 0, 0))  // does not match (too high)
        ][..],
        &[("baz", (23, 0, 0))], // matches
    ]));

    // Check for failing support (second disjunctive fails)
    assert!(!mock.supports([
        &[
            ("foo", (43, 0, 0)),  // does not match (too high)
            ("bar", (151, 0, 0))  // matches
        ][..],
        &[("baz", (151, 0, 0))][..], // does not match (too high)
    ]));
}

/// Tries to assure that the current server allows to parse the versions
#[test]
fn ctx_live_test_server() {
    let ctx = TestContext::new();
    let mut conn = ctx.connection();

    let components = AvailableComponents::from(&mut conn);

    assert!(components.supports(REDIS_CE_6_0));
}

/// Tries to assure versions get correctly extracted from a single number with single digit components
#[test]
fn component_parse_single_all_single_digit() {
    let input = "foo_version: 10203";

    let components = build_available_components(input);

    assert_components_eq(components, vec![("foo", (1, 2, 3))]);
}

/// Tries to assure versions get correctly extracted from a single number with multiple digit components and a suffix
#[test]
fn component_parse_single_multiple_digits_suffixed() {
    let input = "foo_version: 1234567-rc23";

    let components = build_available_components(input);

    assert_components_eq(components, vec![("foo", (123, 45, 67))]);
}

/// Tries to assure versions get correctly extracted from a triplet with single digit components
#[test]
fn component_parse_triplet_all_single_digit() {
    let input = "foo_version: 4.2.6";

    let components = build_available_components(input);

    assert_components_eq(components, vec![("foo", (4, 2, 6))]);
}

/// Tries to assure versions get correctly extracted from a triplet with multiple digit components and a suffix
#[test]
fn component_parse_triplet_multiple_digits_suffixed() {
    let input = "foo_version: 42.4711.23-rc121";

    let components = build_available_components(input);

    assert_components_eq(components, vec![("foo", (42, 4711, 23))]);
}

/// Tries to assure that a simple server version gets properly extracted from an info response
#[test]
fn component_parse_multiline_simple_server_version() {
    let input = r#"
bar: baz
foo_version:4711.23.42
quux: quuux
"#;

    let components = build_available_components(input);

    assert_components_eq(components, vec![("foo", (4711, 23, 42))]);
}

/// Tries to assure that a simple module version gets properly extracted from an info response
#[test]
fn component_parse_multiline_simple_module_version() {
    let input = r#"
bar: baz
module:name=foo,ver=47112342
quux: quuux
"#;

    let components = build_available_components(input);

    assert_components_eq(components, vec![("foo", (4711, 23, 42))]);
}

/// Tries to assure that versions properly parse from a complex info response
#[test]
fn component_parse_mix() {
    let input = r#"
   foo_version   :  2600.3.14159  # with whitespace padding
#bar_version:1.2.3 # commented out
this is # partly commented out

   module  :  foo=bar, ver  =  47112342, quux=quuux,  name = baz , baz =
"#;

    let components = build_available_components(input);

    assert_components_eq(
        components,
        vec![("foo", (2600, 3, 14159)), ("baz", (4711, 23, 42))],
    );
}
