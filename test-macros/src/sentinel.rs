use proc_macro2::TokenStream as TokenStream2;
use quote::quote;

use crate::utils::{generate_async_call, generate_sync_call, ignore_flag};

/// Builds the expansion for `#[sentinel_test]`.
pub(crate) fn expand_sentinel_test(_attr: TokenStream2, input: TokenStream2) -> TokenStream2 {
    let mut item: syn::ItemFn = syn::parse2(input).expect("failed to parse function");
    let test_function_name = item.sig.ident.clone();

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
            #[cfg(feature = "sentinel")]
            fn resp2_tcp() {
                let mut ctx = crate::support::TestSentinelContext::new_with_server_type_and_protocol(
                    2,
                    3,
                    3,
                    redis_test::server::ServerType::Tcp { tls: false },
                    redis::ProtocolVersion::RESP2,
                );
                #call_expr
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "sentinel", any(feature = "tls-rustls", feature = "tls-native-tls")))]
            fn resp2_tls() {
                let mut ctx = crate::support::TestSentinelContext::new_with_server_type_and_protocol(
                    2,
                    3,
                    3,
                    redis_test::server::ServerType::Tcp { tls: true },
                    redis::ProtocolVersion::RESP2,
                );
                #call_expr
            }

            #[test]
            #ignore_flag
            #[cfg(feature = "sentinel")]
            fn resp3_tcp() {
                let mut ctx = crate::support::TestSentinelContext::new_with_server_type_and_protocol(
                    2,
                    3,
                    3,
                    redis_test::server::ServerType::Tcp { tls: false },
                    redis::ProtocolVersion::RESP3,
                );
                #call_expr
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "sentinel", any(feature = "tls-rustls", feature = "tls-native-tls")))]
            fn resp3_tls() {
                let mut ctx = crate::support::TestSentinelContext::new_with_server_type_and_protocol(
                    2,
                    3,
                    3,
                    redis_test::server::ServerType::Tcp { tls: true },
                    redis::ProtocolVersion::RESP3,
                );
                #call_expr
            }
        }
    }
}

/// Builds the expansion for `#[async_sentinel_test]`.
pub(crate) fn expand_async_sentinel_test(_attr: TokenStream2, input: TokenStream2) -> TokenStream2 {
    let mut item: syn::ItemFn = syn::parse2(input).expect("failed to parse function");
    let test_function_name = item.sig.ident.clone();

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
            #[cfg(all(feature = "sentinel", feature = "tokio-comp"))]
            fn resp2_tcp_tokio() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestSentinelContext::new_with_server_type_and_protocol(
                        2,
                        3,
                        3,
                        redis_test::server::ServerType::Tcp { tls: false },
                        redis::ProtocolVersion::RESP2,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Tokio);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "sentinel", feature = "smol-comp"))]
            fn resp2_tcp_smol() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestSentinelContext::new_with_server_type_and_protocol(
                        2,
                        3,
                        3,
                        redis_test::server::ServerType::Tcp { tls: false },
                        redis::ProtocolVersion::RESP2,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Smol);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "sentinel", any(feature = "tokio-rustls-comp", feature = "tokio-native-tls-comp")))]
            fn resp2_tls_tokio() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestSentinelContext::new_with_server_type_and_protocol(
                        2,
                        3,
                        3,
                        redis_test::server::ServerType::Tcp { tls: true },
                        redis::ProtocolVersion::RESP2,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Tokio);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "sentinel", any(feature = "smol-rustls-comp", feature = "smol-native-tls-comp")))]
            fn resp2_tls_smol() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestSentinelContext::new_with_server_type_and_protocol(
                        2,
                        3,
                        3,
                        redis_test::server::ServerType::Tcp { tls: true },
                        redis::ProtocolVersion::RESP2,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Smol);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "sentinel", feature = "tokio-comp"))]
            fn resp3_tcp_tokio() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestSentinelContext::new_with_server_type_and_protocol(
                        2,
                        3,
                        3,
                        redis_test::server::ServerType::Tcp { tls: false },
                        redis::ProtocolVersion::RESP3,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Tokio);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "sentinel", feature = "smol-comp"))]
            fn resp3_tcp_smol() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestSentinelContext::new_with_server_type_and_protocol(
                        2,
                        3,
                        3,
                        redis_test::server::ServerType::Tcp { tls: false },
                        redis::ProtocolVersion::RESP3,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Smol);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "sentinel", any(feature = "tokio-rustls-comp", feature = "tokio-native-tls-comp")))]
            fn resp3_tls_tokio() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestSentinelContext::new_with_server_type_and_protocol(
                        2,
                        3,
                        3,
                        redis_test::server::ServerType::Tcp { tls: true },
                        redis::ProtocolVersion::RESP3,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Tokio);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "sentinel", any(feature = "smol-rustls-comp", feature = "smol-native-tls-comp")))]
            fn resp3_tls_smol() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestSentinelContext::new_with_server_type_and_protocol(
                        2,
                        3,
                        3,
                        redis_test::server::ServerType::Tcp { tls: true },
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
    fn sentinel_emits_resp2_and_resp3() {
        let out = expand_sentinel_test("".parse().unwrap(), item());
        let s = to_str(&out);
        assert!(s.contains("fnresp2_tcp()"));
        assert!(s.contains("fnresp2_tls()"));
        assert!(s.contains("fnresp3_tcp()"));
        assert!(s.contains("fnresp3_tls()"));
        assert!(s.contains("new_with_server_type_and_protocol"));
    }

    #[test]
    fn async_sentinel_emits_tokio_and_smol() {
        let out = expand_async_sentinel_test("".parse().unwrap(), item());
        let s = to_str(&out);
        assert!(s.contains("fnresp2_tcp_tokio()"));
        assert!(s.contains("fnresp2_tls_smol()"));
        assert!(s.contains("fnresp3_tcp_smol()"));
        assert!(s.contains("fnresp3_tls_tokio()"));
    }
}
