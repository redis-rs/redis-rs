//! Tooling to handle server version extraction and matching

use redis::ConnectionLike;
use std::collections::HashMap;

// A generic macro to create a [`Component`]
macro_rules! version {
    ($major:expr, $minor:expr) => {
        ($major, $minor, 0)
    };
    ($major:expr, $minor:expr, $patch:expr) => {
        ($major, $minor, $patch)
    };
}

// Macro to create a [`Component`] for Redis
macro_rules! redis_version {
    ($($args:tt)*) => {
        ("redis", version!($($args)*))
    };
}

// Macro to create a [`Component`] for Valkey
macro_rules! valkey_version {
    ($($args:tt)*) => {
        ("valkey", version!($($args)*))
    };
}

// Redis version constants for version-gated tests
pub const REDIS_VERSION_CE_6_0: Component = redis_version!(6, 0);
pub const REDIS_VERSION_CE_7_0: Component = redis_version!(7, 0);
pub const REDIS_VERSION_CE_7_2: Component = redis_version!(7, 2);
pub const REDIS_VERSION_CE_7_4: Component = redis_version!(7, 4);
pub const REDIS_VERSION_CE_8_0: Component = redis_version!(8, 0);
pub const REDIS_VERSION_CE_8_2: Component = redis_version!(8, 1, 240);
pub const REDIS_VERSION_CE_8_4: Component = redis_version!(8, 3, 224);
pub const REDIS_VERSION_CE_8_6: Component = redis_version!(8, 6);
pub const REDIS_VERSION_CE_8_8: Component = redis_version!(8, 8);
/// Numbered databases in cluster mode were introduced in Valkey 9.0.
pub const VALKEY_VERSION_CE_9_0: Component = valkey_version!(9, 0);

/// Version of a software component
pub type Version = (u32, u32, u32);

/// Software component name along with its version
pub type Component<'a> = (&'a str, Version);

/// Matcher for [`Component`]s
pub struct ComponentMatcher<'a> {
    conjunctive_parts: Vec<Vec<Component<'a>>>,
}

impl<'a> ComponentMatcher<'a> {
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

/// Macros to provide array implementations for matchers' slice implementations
///
/// Rust can auto-coerce array to slices. But with generic arguments, this
/// array-to-slice-auto-coercion does not kick in. So one would have to convert manually. To avoid
/// this for the common cases, this macro implements coercing `From`s. for a given array length
///
/// # Arguments
///
/// * `$n` - The array lengths to implement coercing `From`s for.
macro_rules! matcher_array_impls {
    ($n:expr) => {
        impl<'a> From<[Component<'a>; $n]> for ComponentMatcher<'a> {
            fn from(value: [Component<'a>; $n]) -> Self {
                let coerced_value: &[Component<'a>] = &value;
                Self::from(coerced_value)
            }
        }

        impl<'a> From<[&[Component<'a>]; $n]> for ComponentMatcher<'a> {
            fn from(value: [&[Component<'a>]; $n]) -> Self {
                let coerced_value: &[&[Component<'a>]] = &value;
                Self::from(coerced_value)
            }
        }
    };
}
matcher_array_impls!(1);
matcher_array_impls!(2);
matcher_array_impls!(3);

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
            let Some((name, version)) = Self::parse_info_kv(key.trim(), value.trim()) else {
                continue;
            };

            // Store them
            ret.insert(name.to_owned(), version);
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
        if !$ctx.supports($component) {
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
/// let ctx = run_test_if_version_supported!(REDIS_VERSION_CE_8_0);
/// ```
///
/// With modules:
/// ```ignore
/// let ctx = run_test_if_version_supported!(REDIS_VERSION_CE_8_0, &[Module::Search]);
/// ```
#[macro_export]
macro_rules! run_test_if_version_supported {
    ($component:expr) => {{ run_test_if_version_supported!($component, &[]) }};
    ($component:expr, $modules:expr) => {{
        let ctx = $crate::support::TestContext::with_modules($modules);

        $crate::skip_if_context_does_not_support!(ctx, $component);

        ctx
    }};
}

// As this module is included in many integration tests, adding unit tests here would also (re)run
// them during each of those integration tests. Hence, we move out unit tests of this module into
// `test_support.rs`
