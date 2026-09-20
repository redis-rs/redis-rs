/// The server transports / connection kinds that the test macros can generate.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ServerKind {
    Tcp,
    Tls,
    Unix,
}

/// Removes the `REDISRS_SERVER_TYPE`/`PROTOCOL` variables so the full matrix is generated.
///
/// These variables are deliberately inert in the generated tests (the matrix is driven purely by
/// cargo features); clearing them here keeps the full-output oracle tests deterministic regardless
/// of the shell environment.
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
