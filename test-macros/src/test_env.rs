use std::env;

/// The server transports / connection kinds that the test macros can generate.
///
/// These mirror the values accepted by `REDISRS_SERVER_TYPE`:
///   - `tcp`    => plain TCP (`ServerType::Tcp { tls: false }` / `ClusterType::Tcp`)
///   - `tcp+tls`=> TCP with TLS (`ServerType::Tcp { tls: true }` / `ClusterType::TcpTls`)
///   - `unix`   => a unix socket (`ServerType::Unix`)
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ServerKind {
    Tcp,
    Tls,
    Unix,
}

/// Reads and validates `REDISRS_SERVER_TYPE`. Returns `None` when unset (the full matrix should be
/// generated) and the single requested kind when set.
fn requested_server() -> Option<ServerKind> {
    match env::var("REDISRS_SERVER_TYPE").ok().as_deref() {
        Some("tcp") => Some(ServerKind::Tcp),
        Some("tcp+tls") => Some(ServerKind::Tls),
        Some("unix") => Some(ServerKind::Unix),
        Some(val) => panic!("Unknown server type {val:?}"),
        None => None,
    }
}

/// Reads and validates `PROTOCOL`. Returns `None` when unset (the full protocol matrix should be
/// generated) and the single requested protocol when set.
fn requested_protocol() -> Option<&'static str> {
    match env::var("PROTOCOL").ok().as_deref() {
        Some("RESP2") => Some("RESP2"),
        Some("RESP3") => Some("RESP3"),
        Some(val) => panic!("Unknown protocol {val:?}"),
        None => None,
    }
}

/// Whether the given server kind should be generated. Always `true` unless `REDISRS_SERVER_TYPE`
/// is set, in which case only the matching kind is generated.
pub fn server_enabled(kind: ServerKind) -> bool {
    requested_server().is_none_or(|wanted| wanted == kind)
}

/// Whether the given protocol should be generated. Always `true` unless `PROTOCOL` is set, in
/// which case only the matching protocol is generated.
pub fn protocol_enabled(protocol: &str) -> bool {
    requested_protocol().is_none_or(|wanted| wanted == protocol)
}

/// Removes the `REDISRS_SERVER_TYPE`/`PROTOCOL` variables so the full matrix is generated.
///
/// Call inside [`with_env`] (which provides the lock); this is what the full-output oracle tests use
/// so they pass regardless of the shell environment.
#[cfg(test)]
pub(crate) fn clear_env() {
    unsafe {
        std::env::remove_var("REDISRS_SERVER_TYPE");
        std::env::remove_var("PROTOCOL");
    }
}

/// Runs `f` with exclusive access to the global process environment.
///
/// `expand_*` reads `REDISRS_SERVER_TYPE`/`PROTOCOL` from the process env, so any test that sets or
/// clears them must call this (which holds a `Mutex` for the whole window) to avoid racing with
/// other tests running in the same process. `setup` runs first (while the lock is held), and the env
/// vars are cleared again when the call returns so no state leaks.
#[cfg(test)]
pub(crate) fn with_env<R>(setup: impl FnOnce(), f: impl FnOnce() -> R) -> R {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = LOCK.lock().unwrap_or_else(|p| p.into_inner());
    setup();
    let result = f();
    clear_env();
    result
}
