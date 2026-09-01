use proc_macro2::TokenStream as TokenStream2;
use quote::quote;

use crate::utils::{generate_async_call, generate_sync_call, ignore_flag, parse_module_from_attr};

/// Builds the expansion for `#[single_server_test(...)]`.
pub(crate) fn expand_single_server_test(attr: TokenStream2, input: TokenStream2) -> TokenStream2 {
    let mut item: syn::ItemFn = syn::parse2(input).expect("failed to parse function");
    let test_function_name = item.sig.ident.clone();
    let module_expr = parse_module_from_attr(&attr);

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
            fn resp2_tcp() {
                let mut ctx = crate::support::TestContextBuilder::new()
                    #module_expr
                    .protocol(redis::ProtocolVersion::RESP2)
                    .server_type(redis_test::server::ServerType::Tcp { tls: false })
                    .build();
                #call_expr
            }

            #[test]
            #ignore_flag
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            fn resp2_tls() {
                let mut ctx = crate::support::TestContextBuilder::new()
                    #module_expr
                    .protocol(redis::ProtocolVersion::RESP2)
                    .server_type(redis_test::server::ServerType::Tcp { tls: true })
                    .build();
                #call_expr
            }

            #[test]
            #ignore_flag
            #[cfg(unix)]
            fn resp2_unix() {
                let mut ctx = crate::support::TestContextBuilder::new()
                    #module_expr
                    .protocol(redis::ProtocolVersion::RESP2)
                    .server_type(redis_test::server::ServerType::Unix)
                    .build();
                #call_expr
            }

            #[test]
            #ignore_flag
            fn resp3_tcp() {
                let mut ctx = crate::support::TestContextBuilder::new()
                    #module_expr
                    .protocol(redis::ProtocolVersion::RESP3)
                    .server_type(redis_test::server::ServerType::Tcp { tls: false })
                    .build();
                #call_expr
            }

            #[test]
            #ignore_flag
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            fn resp3_tls() {
                let mut ctx = crate::support::TestContextBuilder::new()
                    #module_expr
                    .protocol(redis::ProtocolVersion::RESP3)
                    .server_type(redis_test::server::ServerType::Tcp { tls: true })
                    .build();
                #call_expr
            }

            #[test]
            #ignore_flag
            #[cfg(unix)]
            fn resp3_unix() {
                let mut ctx = crate::support::TestContextBuilder::new()
                    #module_expr
                    .protocol(redis::ProtocolVersion::RESP3)
                    .server_type(redis_test::server::ServerType::Unix)
                    .build();
                #call_expr
            }
        }
    }
}

/// Builds the expansion for `#[async_single_server_test(...)]`.
pub(crate) fn expand_async_single_server_test(
    attr: TokenStream2,
    input: TokenStream2,
) -> TokenStream2 {
    let mut item: syn::ItemFn = syn::parse2(input).expect("failed to parse function");
    let test_function_name = item.sig.ident.clone();
    let module_expr = parse_module_from_attr(&attr);

    item.sig.ident = syn::Ident::new(
        &format!("{test_function_name}_internal"),
        test_function_name.span(),
    );
    let function_name = item.sig.ident.clone();
    let ignore_flag = ignore_flag(&item);
    let call_expr = generate_async_call(&function_name, &item.sig.inputs);

    quote! {
        mod #test_function_name {
            use super::*;
            #item

            #[test]
            #ignore_flag
            #[cfg(feature = "tokio-comp")]
            fn resp2_tcp_tokio() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestContextBuilder::new()
                        #module_expr
                        .protocol(redis::ProtocolVersion::RESP2)
                        .server_type(redis_test::server::ServerType::Tcp { tls: false })
                        .build();
                    #call_expr
                }, crate::support::RuntimeType::Tokio);
            }

            #[test]
            #ignore_flag
            #[cfg(feature = "smol-comp")]
            fn resp2_tcp_smol() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestContextBuilder::new()
                        #module_expr
                        .protocol(redis::ProtocolVersion::RESP2)
                        .server_type(redis_test::server::ServerType::Tcp { tls: false })
                        .build();
                    #call_expr
                }, crate::support::RuntimeType::Smol);
            }

            #[test]
            #ignore_flag
            #[cfg(any(feature = "tokio-rustls-comp", feature = "tokio-native-tls-comp"))]
            fn resp2_tls_tokio() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestContextBuilder::new()
                        #module_expr
                        .protocol(redis::ProtocolVersion::RESP2)
                        .server_type(redis_test::server::ServerType::Tcp { tls: true })
                        .build();
                    #call_expr
                }, crate::support::RuntimeType::Tokio);
            }

            #[test]
            #ignore_flag
            #[cfg(any(feature = "smol-rustls-comp", feature = "smol-native-tls-comp"))]
            fn resp2_tls_smol() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestContextBuilder::new()
                        #module_expr
                        .protocol(redis::ProtocolVersion::RESP2)
                        .server_type(redis_test::server::ServerType::Tcp { tls: true })
                        .build();
                    #call_expr
                }, crate::support::RuntimeType::Smol);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "tokio-comp", unix))]
            fn resp2_unix_tokio() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestContextBuilder::new()
                        #module_expr
                        .protocol(redis::ProtocolVersion::RESP2)
                        .server_type(redis_test::server::ServerType::Unix)
                        .build();
                    #call_expr
                }, crate::support::RuntimeType::Tokio);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "smol-comp", unix))]
            fn resp2_unix_smol() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestContextBuilder::new()
                        #module_expr
                        .protocol(redis::ProtocolVersion::RESP2)
                        .server_type(redis_test::server::ServerType::Unix)
                        .build();
                    #call_expr
                }, crate::support::RuntimeType::Smol);
            }

            #[test]
            #ignore_flag
            #[cfg(feature = "tokio-comp")]
            fn resp3_tcp_tokio() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestContextBuilder::new()
                        #module_expr
                        .protocol(redis::ProtocolVersion::RESP3)
                        .server_type(redis_test::server::ServerType::Tcp { tls: false })
                        .build();
                    #call_expr
                }, crate::support::RuntimeType::Tokio);
            }

            #[test]
            #ignore_flag
            #[cfg(feature = "smol-comp")]
            fn resp3_tcp_smol() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestContextBuilder::new()
                        #module_expr
                        .protocol(redis::ProtocolVersion::RESP3)
                        .server_type(redis_test::server::ServerType::Tcp { tls: false })
                        .build();
                    #call_expr
                }, crate::support::RuntimeType::Smol);
            }

            #[test]
            #ignore_flag
            #[cfg(any(feature = "tokio-rustls-comp", feature = "tokio-native-tls-comp"))]
            fn resp3_tls_tokio() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestContextBuilder::new()
                        #module_expr
                        .protocol(redis::ProtocolVersion::RESP3)
                        .server_type(redis_test::server::ServerType::Tcp { tls: true })
                        .build();
                    #call_expr
                }, crate::support::RuntimeType::Tokio);
            }

            #[test]
            #ignore_flag
            #[cfg(any(feature = "smol-rustls-comp", feature = "smol-native-tls-comp"))]
            fn resp3_tls_smol() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestContextBuilder::new()
                        #module_expr
                        .protocol(redis::ProtocolVersion::RESP3)
                        .server_type(redis_test::server::ServerType::Tcp { tls: true })
                        .build();
                    #call_expr
                }, crate::support::RuntimeType::Smol);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "tokio-comp", unix))]
            fn resp3_unix_tokio() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestContextBuilder::new()
                        #module_expr
                        .protocol(redis::ProtocolVersion::RESP3)
                        .server_type(redis_test::server::ServerType::Unix)
                        .build();
                    #call_expr
                }, crate::support::RuntimeType::Tokio);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "smol-comp", unix))]
            fn resp3_unix_smol() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestContextBuilder::new()
                        #module_expr
                        .protocol(redis::ProtocolVersion::RESP3)
                        .server_type(redis_test::server::ServerType::Unix)
                        .build();
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

    #[test]
    fn single_server_no_attr_emits_resp2_and_resp3_tcp() {
        let out = expand_single_server_test(
            "".parse().unwrap(),
            "fn test(ctx: &mut TestContext) {}"
                .to_string()
                .parse()
                .unwrap(),
        );
        let s = to_str(&out);
        assert!(s.contains("modtest"));
        assert!(s.contains("fntest_internal(ctx:&mutTestContext)"));
        assert!(s.contains("fnresp2_tcp()"));
        assert!(s.contains("fnresp3_tcp()"));
        assert!(s.contains("fnresp2_tls()"));
        assert!(s.contains("fnresp3_unix()"));
        assert!(!s.contains(".module("));
    }

    #[test]
    fn single_server_json_attr_injects_module() {
        let out = expand_single_server_test(
            "json".parse().unwrap(),
            "fn test(ctx: &mut TestContext) {}"
                .to_string()
                .parse()
                .unwrap(),
        );
        let s = to_str(&out);
        assert!(s.contains(".module(redis_test::server::Module::Json)"));
    }

    #[test]
    fn single_server_bloom_attr_injects_module() {
        let out = expand_single_server_test(
            "bloom".parse().unwrap(),
            "fn test(ctx: &mut TestContext) {}"
                .to_string()
                .parse()
                .unwrap(),
        );
        let s = to_str(&out);
        assert!(s.contains(".module(redis_test::server::Module::Bloom)"));
    }

    #[test]
    fn single_server_ignore_flag() {
        let out = expand_single_server_test(
            "".parse().unwrap(),
            "#[ignore]\nfn test(ctx: &mut TestContext) {}"
                .to_string()
                .parse()
                .unwrap(),
        );
        assert!(to_str(&out).contains("#[ignore]"));
    }

    #[test]
    fn async_single_server_emits_tokio_and_smol() {
        let out = expand_async_single_server_test(
            "".parse().unwrap(),
            "fn test(ctx: &mut TestContext) {}"
                .to_string()
                .parse()
                .unwrap(),
        );
        let s = to_str(&out);
        assert!(s.contains("fnresp2_tcp_tokio()"));
        assert!(s.contains("fnresp2_tcp_smol()"));
        assert!(s.contains("fnresp3_tcp_tokio()"));
        assert!(s.contains("fnresp3_unix_smol()"));
    }
}
