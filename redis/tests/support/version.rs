//! Tooling to handle server version extraction and matching

use redis::InfoDict;

// Redis version constants for version-gated tests
pub const REDIS_VERSION_CE_7_0: Version = (7, 0, 0);
pub const REDIS_VERSION_CE_7_2: Version = (7, 2, 0);
pub const REDIS_VERSION_CE_7_4: Version = (7, 4, 0);
pub const REDIS_VERSION_CE_8_0: Version = (8, 0, 0);
pub const REDIS_VERSION_CE_8_2: Version = (8, 1, 240);
pub const REDIS_VERSION_CE_8_4: Version = (8, 3, 224);
pub const REDIS_VERSION_CE_8_6: Version = (8, 6, 0);

pub type Version = (u16, u16, u16);

pub fn parse_version(info: InfoDict) -> Version {
    let version: String = info.get("redis_version").unwrap();
    let versions: Vec<u16> = version
        .split('.')
        .map(|version| version.parse::<u16>().unwrap())
        .collect();
    assert_eq!(versions.len(), 3);
    (versions[0], versions[1], versions[2])
}

pub fn get_version(conn: &mut impl redis::ConnectionLike) -> Version {
    let info: InfoDict = redis::Cmd::new().arg("INFO").query(conn).unwrap();
    parse_version(info)
}

/// Get the Redis server version by running `redis-server --version`.
/// Returns `None` if the binary is not available.
pub fn get_redis_binary_version() -> Option<Version> {
    use std::process::Command;

    let binary = std::env::var("REDISRS_SERVER_BIN").unwrap_or_else(|_| "redis-server".to_string());

    let output = match Command::new(&binary).arg("--version").output() {
        Ok(output) => output,
        Err(_) => {
            eprintln!("Failed to execute redis-server --version");
            return None;
        }
    };

    let full_string =
        String::from_utf8(output.stdout).expect("Invalid UTF-8 in redis-server version output");

    let version_str = full_string
        .split_whitespace()
        .find(|s| s.starts_with("v="))
        .and_then(|s| s.strip_prefix("v="))
        .expect("Could not find version in redis-server output");

    let versions: Vec<u16> = version_str
        .split('.')
        .take(3)
        .map(|v| v.parse::<u16>().expect("Failed to parse version number"))
        .collect();

    assert_eq!(versions.len(), 3, "Expected version format x.y.z");
    Some((versions[0], versions[1], versions[2]))
}

/// Macro to run tests only if the Redis version meets the minimum requirement.
/// If the version is insufficient, the test is skipped with a message.
#[macro_export]
macro_rules! run_test_if_version_supported {
    ($minimum_required_version:expr) => {{
        let ctx = $crate::support::TestContext::new();
        let redis_version = ctx.get_version();

        if redis_version < *$minimum_required_version {
            eprintln!("Skipping the test because the current version of Redis {:?} doesn't match the minimum required version {:?}.",
            redis_version, $minimum_required_version);
            return;
        }

        ctx
    }};
}

/// Macro to run tests only if the version of the Redis binary meets the minimum requirement.
/// If the binary is not available or the version is insufficient, the test is skipped with a message.
///
/// # Example
/// ```rust,no_run
/// #[test]
/// fn test_redis_8_6_feature() {
///     run_test_if_redis_binary_version_supported!(&REDIS_VERSION_CE_8_6);
///     // Only now create the expensive test context
///     let ctx = TestContext::new_with_cert_auth(tls_files);
///     // ...
/// }
/// ```
#[macro_export]
macro_rules! run_test_if_redis_binary_version_supported {
    ($minimum_required_version:expr) => {{
        match $crate::get_redis_binary_version() {
            None => {
                eprintln!(
                    "Skipping the test because the Redis binary was not found."
                );
                return;
            }
            Some(redis_version) => {
                if redis_version < *$minimum_required_version {
                    eprintln!(
                        "Skipping the test because the current version of Redis {:?} doesn't match the minimum required version {:?}.",
                        redis_version, $minimum_required_version
                    );
                    return;
                }
            }
        }
    }};
}
