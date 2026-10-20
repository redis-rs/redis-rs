//! Derives the `native_tls_selected` cfg.
//!
//! `native-tls` is only reachable for a runtime whose rustls counterpart is disabled, so the
//! certificate handling it needs is compiled in only when at least one such runtime is enabled.
//! The condition is needed in several modules, so it is named here rather than repeated.

fn main() {
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rustc-check-cfg=cfg(native_tls_selected)");

    let enabled = |feature: &str| {
        let name = feature.to_uppercase().replace('-', "_");
        std::env::var_os(format!("CARGO_FEATURE_{name}")).is_some()
    };

    let selected = (enabled("tls-native-tls") && !enabled("tls-rustls"))
        || (enabled("tokio-native-tls-comp") && !enabled("tokio-rustls-comp"))
        || (enabled("smol-native-tls-comp") && !enabled("smol-rustls-comp"));

    if selected {
        println!("cargo::rustc-cfg=native_tls_selected");
    }
}
