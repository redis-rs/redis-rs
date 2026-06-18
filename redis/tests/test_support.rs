//! (Unit) tests for the support module
//!
//! As the `support` module gets included in most integration tests, adding its unit tests to the
//! `support` module itself would include (and compile and run) them for each integration test
//! module. This is unwarranted. So we instead collect them in this module.

use crate::support::{TestContextVersioning, Version};

#[macro_use]
mod support;

/// Mock implementation of [`TestContextVersioning`] to allow testing default implementations
struct MockTestContext {
    /// The version to return during `get_version`
    version: Version,
}

impl MockTestContext {
    /// Builds a new instance
    fn new(version: Version) -> Self {
        Self { version }
    }
}

impl TestContextVersioning for MockTestContext {
    fn get_version(&self) -> Version {
        self.version
    }
}

/// Tries to assure that `support` matches the same version
#[test]
fn support_exact_match() {
    // The context to check with
    let mock = MockTestContext::new((42, 4711, 23));

    // Check for support
    assert!(mock.supports((42, 4711, 23)));
}

/// Tries to assure that `support` correctly handles a smaller major number
#[test]
fn support_major_smaller() {
    // The context to check with
    let mock = MockTestContext::new((42, 4711, 23));

    // Check for support
    assert!(mock.supports((41, 4712, 24)));
}

/// Tries to assure that `support` correctly handles a bigger major number
#[test]
fn support_major_bigger() {
    // The context to check with
    let mock = MockTestContext::new((42, 4711, 23));

    // Check for support
    assert!(!mock.supports((43, 4710, 22)));
}

/// Tries to assure that `support` correctly handles a matching major but bigger minor number
#[test]
fn support_major_match_minor_smaller() {
    // The context to check with
    let mock = MockTestContext::new((42, 4711, 23));

    // Check for support
    assert!(mock.supports((42, 4710, 24)));
}

/// Tries to assure that `support` correctly handles a matching major but smaller minor number
#[test]
fn support_major_match_minor_bigger() {
    // The context to check with
    let mock = MockTestContext::new((42, 4711, 23));

    // Check for support
    assert!(!mock.supports((42, 4712, 22)));
}

/// Tries to assure that `support` correctly handles a matching major and minor but bigger patch number
#[test]
fn support_major_match_minor_match_patch_smaller() {
    // The context to check with
    let mock = MockTestContext::new((42, 4711, 23));

    // Check for support
    assert!(mock.supports((42, 4711, 22)));
}

/// Tries to assure that `support` correctly handles a matching major and minor but smaller patch number
#[test]
fn support_major_match_minor_match_patch_bigger() {
    // The context to check with
    let mock = MockTestContext::new((42, 4711, 23));

    // Check for support
    assert!(!mock.supports((42, 4711, 24)));
}
