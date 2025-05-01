use cfg_aliases::cfg_aliases;

fn main() {
    // Setup cfg aliases
    cfg_aliases! {
        // Backends
        native_tls_without_rustls: { all(feature = "tls-native-tls", not(feature = "tls-rustls")) },
        smol_native_tls_without_rustls: { all(feature = "smol-native-tls-comp", not(feature = "smol-rustls-comp")) },
        tokio_native_tls_without_rustls: { all(feature = "tokio-native-tls-comp", not(feature = "tokio-rustls-comp")) },
        insecure_or_native_tls: { any(feature = "tls-rustls-insecure", feature = "tls-native-tls") },
        tokio_tls: { any(feature = "tokio-native-tls-comp", feature = "tokio-rustls-comp") },
        smol_tls: { any(feature = "smol-native-tls-comp", feature = "smol-rustls-comp") },
        async_std_tls: { any(feature = "async-std-native-tls-comp", feature = "async-std-rustls-comp") },
        os_that_support_user_timeout: { any(target_os = "android", target_os = "fuchsia", target_os = "linux") },
        number_types: { any(feature = "rust_decimal", feature = "bigdecimal", feature = "num-bigint") },
        sync_tls: { any(feature = "tls-native-tls", feature = "tls-rustls") },
        enabled_tokio_and_another_runtime: { all(feature = "tokio-comp", any(feature = "async-std-comp", feature = "smol-comp"))},
        enabled_smol_and_another_runtime: { all(feature = "smol-comp", any(feature = "tokio-comp", feature = "async-std-comp"))},
        enabled_async_std_and_another_runtime: { all(feature = "async-std-comp", any(feature = "tokio-comp", feature = "smol-comp"))},
        only_tokio: {all(feature = "tokio-comp", not(feature = "async-std-comp"), not(feature = "smol-comp")) },
        only_smol: { all(not(feature = "tokio-comp"), feature = "smol-comp", not(feature = "async-std-comp")) },
        only_async_std: { all(not(feature = "tokio-comp"), not(feature = "smol-comp"), feature = "async-std-comp") },
        multiple_runtimes: { any(enabled_tokio_and_another_runtime, enabled_smol_and_another_runtime, enabled_async_std_and_another_runtime) }
    }
}
