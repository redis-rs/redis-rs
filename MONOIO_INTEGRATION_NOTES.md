# Monoio Integration for Redis-rs

## Overview

Monoio is a high-performance, I/O-uring-based async runtime that uses a thread-per-core concurrency model. This document outlines the Monoio runtime integration with redis-rs.

## Implementation Details

### 1. MonoioWrapped Adapter (`redis/src/aio/monoio.rs`)

**Problem:** Monoio's `AsyncReadRent`/`AsyncWriteRent` traits use completion-based I/O with owned buffers, while tokio's `AsyncRead`/`AsyncWrite` use readiness-based I/O with borrowed buffers. This impedance mismatch prevents direct usage.

**Solution:** The `MonoioWrapped<T>` adapter bridges this gap by:
- Maintaining state machines (`ReadState`, `WriteState`) for in-flight operations
- Buffering and managing owned buffers required by monoio
- Implementing tokio's `AsyncRead`/`AsyncWrite` traits

### 2. Send + Sync Implementation

Monoio's thread-per-core model creates a technical challenge:
- Monoio types contain `Rc<T>` and `UnsafeCell<T>`, which are not `Send + Sync`
- Redis-rs requires `Send + Sync` for runtime types
- In practice, monoio guarantees tasks never migrate between threads

**Solution:** We use `unsafe impl Send + Sync` with detailed SAFETY comments explaining:
1. Monoio's thread-per-core model ensures no cross-thread access
2. The wrapped types are only polled on their bound thread
3. Futures contained in state machines are never exposed to other threads

This approach is sound as long as the runtime is used correctly (within monoio's execution model).

### 3. Feature Flags

- `monoio-comp`: Core monoio runtime support
- `monoio-rustls-comp`: TLS support via `monoio-rustls` (not tokio-rustls)

### 4. TLS Support

Uses `monoio-rustls` for TLS connections, avoiding a dependency on tokio. The implementation:
- Reuses existing `create_rustls_config()` infrastructure
- Properly handles server name validation
- Integrates with the `MonoioWrapped` adapter

### 5. Runtime Integration

The `Runtime::locate()` function now:
- Detects monoio availability
- Prefers monoio over tokio/smol when multiple runtimes are available
- Supports `prefer_monoio()` function for explicit selection

## Connection Types Supported

- **TCP**: Standard TCP connections with nodelay setting support
- **Unix sockets**: Unix domain sockets (unix targets)
- **TLS**: TCP + rustls via monoio-rustls

## Testing

Run monoio tests with:
```bash
cargo test --features monoio-comp --lib aio::monoio
```

Or run the comprehensive example:
```bash
cargo run --example monoio-comprehensive --features monoio-comp,streams
```

## Known Limitations

1. **Multi-runtime compilation:** When monoio is compiled alongside tokio/smol, it is preferred. Use `prefer_tokio()` or `prefer_smol()` to override if needed.
2. **Task spawning:** Monoio's spawn returns no handle, so `TaskHandle::Monoio(())` is used for task management.

## Future Improvements

1. Performance optimizations for buffer management
2. Support for additional monoio features
3. Benchmark comparisons with tokio runtime

## References

- [Monoio GitHub](https://github.com/bytedance/monoio)
- [Monoio Documentation](https://docs.rs/monoio)
- [Monoio-rustls](https://docs.rs/monoio-rustls)
