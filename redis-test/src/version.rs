//! Tooling to handle server version extraction and matching

use redis::ConnectionLike;
use std::collections::HashMap;

// Version constants for version-gated tests
pub const REDIS_CE_6_0: Component = ("redis", (6, 0, 0));
pub const REDIS_CE_7_0: Component = ("redis", (7, 0, 0));
pub const REDIS_CE_7_2: Component = ("redis", (7, 2, 0));
pub const REDIS_CE_7_4: Component = ("redis", (7, 4, 0));
pub const REDIS_CE_8_0: Component = ("redis", (8, 0, 0));
pub const REDIS_CE_8_2: Component = ("redis", (8, 1, 240));
pub const REDIS_CE_8_4: Component = ("redis", (8, 3, 224));
pub const REDIS_CE_8_6: Component = ("redis", (8, 6, 0));
pub const REDIS_CE_8_8: Component = ("redis", (8, 8, 0));

pub const REDIS_JSON_8_8: Component = ("ReJSON", (8, 8, 0));
pub const REDIS_BLOOM_ANY: Component = ("redis:bf", (0, 0, 0));

// Valkey forked off at Redis 7.2.4 and still reports its Redis version 7.2.4. So tests that run
// on Redis<=7.2.4 automatically also run on any Valkey server, and we only need version guards for
// later versions.
pub const VALKEY_8_1: Component = ("valkey", (8, 1, 0));
pub const VALKEY_9_0: Component = ("valkey", (9, 0, 0));
pub const VALKEY_9_1: Component = ("valkey", (9, 1, 0));

/// Version of a software component
pub type Version = (u32, u32, u32);

/// Software component name along with its version
pub type Component<'a> = (&'a str, Version);

/// Matcher for [`Component`]s
pub struct ComponentMatcher<'a> {
    conjunctive_parts: Vec<Vec<Component<'a>>>,
}

impl ComponentMatcher<'_> {
    /// Checks if this matcher matches the given available components
    ///
    /// # Arguments
    ///
    /// * `available_components` - The available components to check against
    pub fn matches(&self, available_components: &AvailableComponents) -> bool {
        self.conjunctive_parts.iter().all(|disjunctive_parts| {
            disjunctive_parts
                .iter()
                .any(|component| available_components.supports(*component))
        })
    }
}

/// Matcher for a single [`Component`]
impl<'a> From<Component<'a>> for ComponentMatcher<'a> {
    fn from(value: Component<'a>) -> Self {
        Self {
            conjunctive_parts: vec![vec![value]],
        }
    }
}

/// Disjuntive (`OR`) matcher for a list of [`Component`]s
///
/// If any of the given components are supported, it's a match.
impl<'a> From<&[Component<'a>]> for ComponentMatcher<'a> {
    fn from(value: &[Component<'a>]) -> Self {
        Self {
            conjunctive_parts: vec![value.to_vec()],
        }
    }
}

/// Conjunctive (`AND`) matcher for a list of disjunctively (`OR`) matched lists of [`Component`]s
///
/// If all elements have at least one supported subelement, it's a match.
impl<'a> From<&[&[Component<'a>]]> for ComponentMatcher<'a> {
    fn from(value: &[&[Component<'a>]]) -> Self {
        Self {
            conjunctive_parts: value
                .iter()
                .map(|disjunctive_part| disjunctive_part.to_vec())
                .collect(),
        }
    }
}

/// Coercing array implementations for matchers' slice implementations
///
/// Rust can auto-coerce arrays to slices. But with generic arguments, this
/// array-to-slice-auto-coercion does not kick in. So one would have to convert manually. To avoid
/// this, these const-generic `From`s coerce arrays of any length to the corresponding slice matcher.
impl<'a, const N: usize> From<[Component<'a>; N]> for ComponentMatcher<'a> {
    fn from(value: [Component<'a>; N]) -> Self {
        Self::from(value.as_slice())
    }
}

impl<'a, const N: usize> From<[&[Component<'a>]; N]> for ComponentMatcher<'a> {
    fn from(value: [&[Component<'a>]; N]) -> Self {
        Self::from(value.as_slice())
    }
}

#[derive(Clone)]
pub struct AvailableComponents {
    /// The available components' versions by their name
    components: HashMap<String, Version>,
}

impl AvailableComponents {
    /// Extracts the available/used/usable software components from an `INFO` response
    ///
    /// # Panics
    ///
    /// This method panics upon issues
    fn parse_info(info_response: &str) -> HashMap<String, Version> {
        let mut ret = HashMap::new();
        for raw_line in info_response.lines() {
            // Strip off comments
            let line = raw_line.split("#").next().unwrap();

            // Extract key and value
            let mut split = line.splitn(2, ":");
            let (Some(key), Some(value)) = (split.next(), split.next()) else {
                continue;
            };

            // Turn into raw component name and version
            let Some((mut name, version)) = Self::parse_info_kv(key.trim(), value.trim()) else {
                continue;
            };

            // Apply necessary upfixes

            // Both Redis' and Valkey's `bloom` module identify as `bf`, but we need to distinguish
            // between them in test guards. As Redis' version is 8.0.0+, while Valkey's version is
            // still around 1.0.0, we use that discrepancy to identify them for now. A discussion
            // around that is at https://github.com/orgs/valkey-io/discussions/3934
            if name == "bf" {
                if version > (8, 0, 0) {
                    name = "redis:bf".to_string();
                } else {
                    name = "valkey:bf".to_string();
                }
            }

            // Store them
            ret.insert(name, version);
        }
        ret
    }

    /// Parses an `INFO` key/value into component raw name and version
    ///
    /// # Panics
    ///
    /// This method panics upon issues
    fn parse_info_kv(key: &str, value: &str) -> Option<(String, Version)> {
        if key.ends_with("_version") {
            // Direct key/value version (E.g.: `redis_version`, `valkey_version`)

            // Strip the trailing `_version`
            let name = &key[0..key.len() - 8];

            // Yield the extracted data
            return Some((name.to_owned(), Self::extract_version(value)));
        }

        if key == "module" {
            // Module info line (e.g.: 'module:name=foo,ver=10203,...')

            // Collect the module's options
            let mut options = HashMap::new();
            for pair in value.split(",") {
                let mut inner_split = pair.splitn(2, "=");
                match (inner_split.next(), inner_split.next()) {
                    (Some(key), Some(value)) => {
                        options.insert(key.trim(), value.trim());
                    }
                    _ => continue,
                }
            }

            // Extract name and version
            let name = options.get("name")?;
            let version_str = options.get("ver")?;

            // Yield the extracted data
            return Some((name.to_string(), Self::extract_version(version_str)));
        }

        None
    }

    /// Extracts the version triplet of a `&str`
    ///
    /// # Panics
    ///
    /// This method panics upon issues
    fn extract_version(value: &str) -> Version {
        // Cut-off suffixes (e.g.: the trailing `-rc1` in `1.2.3-rc1`)
        let number_str = value.split("-").next().unwrap();

        // Convert each number part to a number
        let numbers = number_str
            .split('.')
            .map(|version| version.parse::<u32>().unwrap())
            .collect::<Vec<_>>();

        // Massage the numbers into a [`Version`]
        let (major, minor, patch) = match numbers.as_slice() {
            // Single number case is used for module versions
            [number] => {
                let mut rest = *number;

                let patch = rest % 100;
                rest = (rest - patch) / 100;

                let minor = rest % 100;
                let major = (rest - minor) / 100;

                (major, minor, patch)
            }

            // 3 number version is used for main Redis/Valkey/... version
            [major, minor, patch] => (*major, *minor, *patch),
            _ => panic!(
                "version number extraction not implemented for {} parts of '{}'",
                numbers.len(),
                value
            ),
        };

        // Yield as [`Version`]
        (major, minor, patch)
    }

    /// Checks if the instance has the given component in at least the given version
    pub fn supports(&self, component: Component) -> bool {
        // Extract the parts we need from the component
        let (name, requested_version) = component;

        // Get the available version for the component
        let Some(available_version) = self.components.get(name) else {
            return false;
        };

        // Compare versions
        available_version >= &requested_version
    }
}

impl<C: ConnectionLike> From<&mut C> for AvailableComponents {
    /// Extracts the available/used/usable software components from a connection
    ///
    /// # Panics
    ///
    /// This method panics upon issues
    fn from(conn: &mut C) -> Self {
        // We'd like to use [`InfoDict`]. But it stores only the last value if a key occurs multiple
        // times. As each module gets reported with another `module` key, we could only get the last
        // module's information, which is not fit for our use case.
        // Hence, we have to parse `INFO` manually.
        let info_response: String = redis::Cmd::new().arg("INFO").query(conn).unwrap();

        let components = Self::parse_info(info_response.as_str());

        Self { components }
    }
}

impl<'a> From<&'a AvailableComponents> for Vec<Component<'a>> {
    fn from(value: &'a AvailableComponents) -> Self {
        value
            .components
            .iter()
            .map(|(name, version)| (name.as_str(), *version))
            .collect::<Vec<Component<'a>>>()
    }
}

/// Server version extraction and matching
pub trait TestContextVersioning {
    /// Gets the components for the first server in the context
    ///
    /// # Panics
    ///
    /// As this function is only meant to be used during testing, it panics upon any issues.
    fn get_available_components(&self) -> AvailableComponents;

    /// Returns whether the context's server has the given component in at least the given version
    fn supports<'a, T: Into<ComponentMatcher<'a>>>(&self, into_matcher: T) -> bool {
        into_matcher
            .into()
            .matches(&self.get_available_components())
    }
}

/// Skips the current test if it does not support the given component
///
/// # Arguments
///
/// * `$ctx` - The context the test uses for its servers
/// * `$component` - The component that has to be present to run the test
/// * `$ret` - (Optional. Default: `()`) The value to return to skip the test
#[macro_export]
macro_rules! skip_if_context_does_not_support {
    ($ctx:expr, $component:expr) => {{ $crate::skip_if_context_does_not_support!($ctx, $component, ()) }};
    ($ctx:expr, $component:expr, $ret:expr) => {{
        if !redis_test::version::TestContextVersioning::supports(&$ctx, $component) {
            eprintln!(
                "Skipping the test because the running server does not support {:?}.",
                $component
            );
            return $ret;
        }
    }};
}

/// Macro to run tests only if the default [`TestContext`] supports the given component
///
/// If the version is insufficient, the test is skipped with a message.
///
/// # Returns
///
/// A [`TestContext`], if `$component` is available
///
/// # Example
///
/// Without modules:
/// ```ignore
/// let ctx = run_test_if_version_supported!(REDIS_CE_8_0);
/// ```
///
/// With modules:
/// ```ignore
/// let ctx = run_test_if_version_supported!(REDIS_CE_8_0, &[Module::Search]);
/// ```
///
/// [`TestContext`]: super::test_context::TestContext
#[macro_export]
macro_rules! run_test_if_version_supported {
    ($component:expr) => {{ run_test_if_version_supported!($component, &[]) }};
    ($component:expr, $modules:expr) => {{
        let ctx = redis_test::test_context::TestContextBuilder::default()
            .modules($modules)
            .build();

        $crate::skip_if_context_does_not_support!(ctx, $component);

        ctx
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MockCmd, MockRedisConnection};

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
    struct MockTestContextVersioning {
        /// The components to return during `get_available_components`
        components: AvailableComponents,
    }

    impl MockTestContextVersioning {
        /// Builds a new instance
        fn new(input: &str) -> Self {
            let components = build_available_components(input);
            Self { components }
        }
    }

    impl TestContextVersioning for MockTestContextVersioning {
        fn get_available_components(&self) -> AvailableComponents {
            self.components.clone()
        }
    }

    /// Tries to assure that `support` matches the same version
    #[test]
    fn context_supports_exact_match() {
        // The context to check with
        let mock = MockTestContextVersioning::new("foo_version: 42.4711.23");

        // Check for support
        assert!(mock.supports(("foo", (42, 4711, 23))));
    }

    /// Tries to assure that `support` correctly handles a smaller major number
    #[test]
    fn context_supports_major_smaller() {
        // The context to check with
        let mock = MockTestContextVersioning::new("foo_version: 42.4711.23");

        // Check for support
        assert!(mock.supports(("foo", (41, 4712, 24))));
    }

    /// Tries to assure that `support` correctly handles a bigger major number
    #[test]
    fn context_supports_major_bigger() {
        // The context to check with
        let mock = MockTestContextVersioning::new("foo_version: 42.4711.23");

        // Check for support
        assert!(!mock.supports(("foo", (43, 4710, 22))));
    }

    /// Tries to assure that `support` correctly handles a matching major but bigger minor number
    #[test]
    fn context_supports_major_match_minor_smaller() {
        // The context to check with
        let mock = MockTestContextVersioning::new("foo_version: 42.4711.23");

        // Check for support
        assert!(mock.supports(("foo", (42, 4710, 24))));
    }

    /// Tries to assure that `support` correctly handles a matching major but smaller minor number
    #[test]
    fn context_supports_major_match_minor_bigger() {
        // The context to check with
        let mock = MockTestContextVersioning::new("foo_version: 42.4711.23");

        // Check for support
        assert!(!mock.supports(("foo", (42, 4712, 22))));
    }

    /// Tries to assure that `support` correctly handles a matching major and minor but bigger patch number
    #[test]
    fn context_supports_major_match_minor_match_patch_smaller() {
        // The context to check with
        let mock = MockTestContextVersioning::new("foo_version: 42.4711.23");

        // Check for support
        assert!(mock.supports(("foo", (42, 4711, 22))));
    }

    /// Tries to assure that `support` correctly handles a matching major and minor but smaller patch number
    #[test]
    fn context_supports_major_match_minor_match_patch_bigger() {
        // The context to check with
        let mock = MockTestContextVersioning::new("foo_version: 42.4711.23");

        // Check for support
        assert!(!mock.supports(("foo", (42, 4711, 24))));
    }

    /// Tries to assure that `support` correctly handles a disjunctive match
    #[test]
    fn context_supports_disjunctive() {
        // The context to check with
        let mock = MockTestContextVersioning::new("bar_version: 42.4711.23");

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
    fn context_supports_conjunctive() {
        // The context to check with
        let input = r#"
foo_version: 42.0.0
bar_version: 4711.0.0
baz_version: 23.0.0
"#;
        let mock = MockTestContextVersioning::new(input);

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
}
