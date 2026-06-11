//! Tooling to handle server version extraction and matching

use redis::ConnectionLike;
use std::collections::HashMap;

// Redis version constants for version-gated tests
pub const REDIS_VERSION_CE_6_0: Component = ("redis", (6, 0, 0));
pub const REDIS_VERSION_CE_7_0: Component = ("redis", (7, 0, 0));
pub const REDIS_VERSION_CE_7_2: Component = ("redis", (7, 2, 0));
pub const REDIS_VERSION_CE_7_4: Component = ("redis", (7, 4, 0));
pub const REDIS_VERSION_CE_8_0: Component = ("redis", (8, 0, 0));
pub const REDIS_VERSION_CE_8_2: Component = ("redis", (8, 1, 240));
pub const REDIS_VERSION_CE_8_4: Component = ("redis", (8, 3, 224));
pub const REDIS_VERSION_CE_8_6: Component = ("redis", (8, 6, 0));

/// Version of a software component
pub type Version = (u16, u16, u16);

/// Software component name along with its version
pub type Component<'a> = (&'a str, Version);

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

            let mut split = line.splitn(2, ":");
            let (name, version) = match (split.next(), split.next()) {
                (Some(key), Some(value)) => {
                    let key = key.trim();
                    let value = value.trim();
                    if key.ends_with("_version") {
                        // Direct key/value version (E.g.: `redis_version`, `valkey_version`)
                        let name = &key[0..key.len() - 8];
                        (name, Self::extract_version(value))
                    } else if key == "module" {
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
                        let Some(name) = options.get("name") else {
                            continue;
                        };
                        let Some(version_str) = options.get("ver") else {
                            continue;
                        };

                        (*name, Self::extract_version(version_str))
                    } else {
                        continue;
                    }
                }
                _ => continue,
            };
            ret.insert(name.to_owned(), version);
        }
        ret
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
        let (major, minor, patch) = match numbers.len() {
            1 => {
                let mut rest = numbers[0];

                let patch = rest % 100;
                rest = (rest - patch) / 100;

                let minor = rest % 100;
                let major = (rest - minor) / 100;

                (major, minor, patch)
            }
            2 => (numbers[0], numbers[1], 0),
            3 => (numbers[0], numbers[1], numbers[2]),
            _ => panic!(
                "version number extraction not implemented for {} parts of '{}'",
                numbers.len(),
                value
            ),
        };

        // Yield as [`Version`]
        (major as u16, minor as u16, patch as u16)
    }

    /// Checks if the instance has the given component in at least the given version
    pub fn supports(&self, component: &Component) -> bool {
        // Extract the parts we need from the component
        let (name, requested_version) = component;

        // Get the available version for the component
        let Some(available_version) = self.components.get(*name) else {
            return false;
        };

        // Compare versions
        available_version >= requested_version
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
    /// Gets the Redis version for the first server in the context
    ///
    /// # Panics
    ///
    /// As this function is only meant to be used during testing, it panics upon any issues.
    fn get_available_components(&self) -> AvailableComponents;

    /// Returns whether the context's server has the given component in at least the given version
    fn supports(&self, component: &Component) -> bool {
        self.get_available_components().supports(component)
    }
}

/// Skips the current test if it does not support the given Redis version
///
/// # Arguments
///
/// * `$ctx` - The context the test uses for its servers
/// * `$minimum_required_version` - The minimum required Redis version
/// * `$ret` - (Optional. Default: `()`) The value to return to skip the test
#[macro_export]
macro_rules! skip_if_context_does_not_support {
    ($ctx:expr, $minimum_required_version:expr) => {{
        $crate::skip_if_context_does_not_support!($ctx, $minimum_required_version, ())
    }};
    ($ctx:expr, $minimum_required_version:expr, $ret:expr) => {{
        if !$ctx.supports($minimum_required_version) {
            eprintln!("Skipping the test because the current version of Redis doesn't match the minimum required version {:?}.",
            $minimum_required_version);
            return $ret;
        }
    }};
}

/// Macro to run tests only if the Redis version meets the minimum requirement.
/// If the version is insufficient, the test is skipped with a message.
#[macro_export]
macro_rules! run_test_if_version_supported {
    ($minimum_required_version:expr) => {{
        let ctx = $crate::support::TestContext::new();

        $crate::skip_if_context_does_not_support!(ctx, $minimum_required_version);

        ctx
    }};
}

// As this module is included in many integration tests, adding unit tests here would also (re)run
// them during each of those integration tests. Hence, we move out unit tests of this module into
// `test_support.rs`
