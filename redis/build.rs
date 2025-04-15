use cfg_aliases::cfg_aliases;

fn main() {
    println!("cargo::rustc-check-cfg=cfg(native_tls_without_rustls)");
    // Setup cfg aliases
    cfg_aliases! {
        // Backends
        native_tls_without_rustls: { all(feature = "tls-native-tls", not(feature = "tls-rustls")) },
    }
}
