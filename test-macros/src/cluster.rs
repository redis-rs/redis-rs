use proc_macro2::TokenStream as TokenStream2;
use quote::quote;

use crate::utils::{
    generate_async_cluster_call, generate_sync_call, generate_version_check, ignore_flag,
    parse_cluster_test_args,
};

/// Builds the expansion for `#[cluster_test(...)]`.
pub(crate) fn expand_cluster_test(attr: TokenStream2, input: TokenStream2) -> TokenStream2 {
    let mut item: syn::ItemFn = syn::parse2(input).expect("failed to parse function");
    let test_function_name = item.sig.ident.clone();
    let args = parse_cluster_test_args(&attr);
    let config_expr = args.config;
    let version_check = generate_version_check(&args.supported_versions);

    item.sig.ident = syn::Ident::new(
        &format!("{test_function_name}_internal"),
        test_function_name.span(),
    );
    let function_name = item.sig.ident.clone();
    let ignore_flag = ignore_flag(&item);
    let call_expr = generate_sync_call(&function_name, &item.sig.inputs);

    quote! {
        mod #test_function_name {
            use super::*;

            #item

            #[test]
            #ignore_flag
            #[cfg(feature = "cluster")]
            fn resp2_tcp() {
                #version_check
                let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                    (#config_expr)
                        .cluster_type(redis_test::cluster::ClusterType::Tcp),
                    redis::ProtocolVersion::RESP2,
                );
                #call_expr
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "cluster", any(feature = "tls-rustls", feature = "tls-native-tls")))]
            fn resp2_tls() {
                #version_check
                let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                    (#config_expr)
                        .cluster_type(redis_test::cluster::ClusterType::TcpTls),
                    redis::ProtocolVersion::RESP2,
                );
                #call_expr
            }

            #[test]
            #ignore_flag
            #[cfg(feature = "cluster")]
            fn resp3_tcp() {
                #version_check
                let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                    (#config_expr)
                        .cluster_type(redis_test::cluster::ClusterType::Tcp),
                    redis::ProtocolVersion::RESP3,
                );
                #call_expr
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "cluster", any(feature = "tls-rustls", feature = "tls-native-tls")))]
            fn resp3_tls() {
                #version_check
                let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                    (#config_expr)
                        .cluster_type(redis_test::cluster::ClusterType::TcpTls),
                    redis::ProtocolVersion::RESP3,
                );
                #call_expr
            }
        }
    }
}

/// Builds the expansion for `#[async_cluster_test(...)]`.
pub(crate) fn expand_async_cluster_test(attr: TokenStream2, input: TokenStream2) -> TokenStream2 {
    let mut item: syn::ItemFn = syn::parse2(input).expect("failed to parse function");
    let test_function_name = item.sig.ident.clone();
    let args = parse_cluster_test_args(&attr);
    let config_expr = args.config;
    let version_check = generate_version_check(&args.supported_versions);

    item.sig.ident = syn::Ident::new(
        &format!("{test_function_name}_internal"),
        test_function_name.span(),
    );
    let function_name = item.sig.ident.clone();
    let ignore_flag = ignore_flag(&item);
    let call_expr = generate_async_cluster_call(&function_name, &item.sig.inputs);

    quote! {
        mod #test_function_name {
            use super::*;

            #item

            #[test]
            #ignore_flag
            #[cfg(all(feature = "cluster-async", feature = "tokio-comp"))]
            fn resp2_tcp_tokio() {
                crate::support::block_on_all(async move {
                    #version_check
                    let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                        (#config_expr)
                            .cluster_type(redis_test::cluster::ClusterType::Tcp),
                        redis::ProtocolVersion::RESP2,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Tokio);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "cluster-async", feature = "smol-comp"))]
            fn resp2_tcp_smol() {
                crate::support::block_on_all(async move {
                    #version_check
                    let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                        (#config_expr)
                            .cluster_type(redis_test::cluster::ClusterType::Tcp),
                        redis::ProtocolVersion::RESP2,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Smol);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "cluster-async", any(feature = "tokio-rustls-comp", feature = "tokio-native-tls-comp")))]
            fn resp2_tls_tokio() {
                crate::support::block_on_all(async move {
                    #version_check
                    let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                        (#config_expr)
                            .cluster_type(redis_test::cluster::ClusterType::TcpTls),
                        redis::ProtocolVersion::RESP2,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Tokio);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "cluster-async", any(feature = "smol-rustls-comp", feature = "smol-native-tls-comp")))]
            fn resp2_tls_smol() {
                crate::support::block_on_all(async move {
                    #version_check
                    let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                        (#config_expr)
                            .cluster_type(redis_test::cluster::ClusterType::TcpTls),
                        redis::ProtocolVersion::RESP2,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Smol);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "cluster-async", feature = "tokio-comp"))]
            fn resp3_tcp_tokio() {
                crate::support::block_on_all(async move {
                    #version_check
                    let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                        (#config_expr)
                            .cluster_type(redis_test::cluster::ClusterType::Tcp),
                        redis::ProtocolVersion::RESP3,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Tokio);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "cluster-async", feature = "smol-comp"))]
            fn resp3_tcp_smol() {
                crate::support::block_on_all(async move {
                    #version_check
                    let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                        (#config_expr)
                            .cluster_type(redis_test::cluster::ClusterType::Tcp),
                        redis::ProtocolVersion::RESP3,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Smol);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "cluster-async", any(feature = "tokio-rustls-comp", feature = "tokio-native-tls-comp")))]
            fn resp3_tls_tokio() {
                crate::support::block_on_all(async move {
                    #version_check
                    let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                        (#config_expr)
                            .cluster_type(redis_test::cluster::ClusterType::TcpTls),
                        redis::ProtocolVersion::RESP3,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Tokio);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "cluster-async", any(feature = "smol-rustls-comp", feature = "smol-native-tls-comp")))]
            fn resp3_tls_smol() {
                crate::support::block_on_all(async move {
                    #version_check
                    let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                        (#config_expr)
                            .cluster_type(redis_test::cluster::ClusterType::TcpTls),
                        redis::ProtocolVersion::RESP3,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Smol);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn to_str(tokens: &TokenStream2) -> String {
        tokens.to_string().replace(' ', "")
    }

    fn item() -> TokenStream2 {
        "fn test(ctx: &mut TestContext) {}"
            .to_string()
            .parse()
            .unwrap()
    }

    #[test]
    fn cluster_default_config_and_no_version_check() {
        let out = expand_cluster_test("".parse().unwrap(), item());
        let s = to_str(&out);
        assert!(s.contains("fnresp2_tcp()"));
        assert!(s.contains("fnresp3_tls()"));
        assert!(s.contains("RedisClusterConfiguration::default().insecure_tls()"));
        assert!(!s.contains("TestContextVersioning::supports"));
    }

    #[test]
    fn cluster_config_and_supported_versions() {
        let out = expand_cluster_test(
            "config = \"foo()\", supported_versions = \"Json\""
                .parse()
                .unwrap(),
            item(),
        );
        let s = to_str(&out);
        assert!(s.contains("foo()"));
        assert!(s.contains("TestContextVersioning::supports"));
    }

    #[test]
    fn async_cluster_default_config() {
        let out = expand_async_cluster_test("".parse().unwrap(), item());
        let s = to_str(&out);
        assert!(s.contains("fnresp2_tcp_tokio()"));
        assert!(s.contains("fnresp2_tcp_smol()"));
        assert!(s.contains("fnresp3_tls_tokio()"));
    }
}
