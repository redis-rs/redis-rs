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

    /// Asserts the produced expansion equals the expected full output. The expected is given as
    /// readable source and parsed to a token stream first, then compared token-by-token via its
    /// canonical string form (a full-output check, not a substring match).
    fn assert_full(actual: TokenStream2, expected_src: &str) {
        let expected: TokenStream2 = expected_src
            .parse()
            .expect("failed to parse expected expansion");
        assert_eq!(actual.to_string(), expected.to_string());
    }

    /// Each case: the input function and the full, explicit expected expansion for that
    /// `#[sentinel_test]` scenario.
    #[rstest::rstest]
    #[case::ctx(
        r#"fn test(ctx: &mut TestContext) {}"#,
        r#"mod test { use super :: * ; fn test_internal (ctx : & mut TestContext) { } #[test]
    #[cfg (feature = "sentinel")]
    fn resp2_tcp () { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : false } , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (& mut ctx) ; }
    #[test]
    #[cfg (all (feature = "sentinel" , any (feature = "tls-rustls" , feature = "tls-native-tls")))]
    fn resp2_tls () { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : true } , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (& mut ctx) ; }
    #[test]
    #[cfg (feature = "sentinel")]
    fn resp3_tcp () { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : false } , redis :: ProtocolVersion :: RESP3 ,) ; test_internal (& mut ctx) ; }
    #[test]
    #[cfg (all (feature = "sentinel" , any (feature = "tls-rustls" , feature = "tls-native-tls")))]
    fn resp3_tls () { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : true } , redis :: ProtocolVersion :: RESP3 ,) ; test_internal (& mut ctx) ; } }"#
    )]
    #[case::bool(
        r#"fn test(flag: bool) {}"#,
        r#"mod test { use super :: * ; fn test_internal (flag : bool) { } #[test]
    #[cfg (feature = "sentinel")]
    fn resp2_tcp () { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : false } , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (true) ; test_internal (false) ; }
    #[test]
    #[cfg (all (feature = "sentinel" , any (feature = "tls-rustls" , feature = "tls-native-tls")))]
    fn resp2_tls () { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : true } , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (true) ; test_internal (false) ; }
    #[test]
    #[cfg (feature = "sentinel")]
    fn resp3_tcp () { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : false } , redis :: ProtocolVersion :: RESP3 ,) ; test_internal (true) ; test_internal (false) ; }
    #[test]
    #[cfg (all (feature = "sentinel" , any (feature = "tls-rustls" , feature = "tls-native-tls")))]
    fn resp3_tls () { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : true } , redis :: ProtocolVersion :: RESP3 ,) ; test_internal (true) ; test_internal (false) ; } }"#
    )]
    fn sentinel_test(#[case] item_src: &str, #[case] expected: &str) {
        let actual = expand_sentinel_test("".parse().unwrap(), item_src.parse().unwrap());
        assert_full(actual, expected);
    }

    /// Each case: the input function and the full, explicit expected expansion for that
    /// `#[async_sentinel_test]` scenario.
    #[rstest::rstest]
    #[case::ctx(
        r#"fn test(ctx: &mut TestContext) {}"#,
        r#"mod test { use super :: * ; fn test_internal (ctx : & mut TestContext) { } #[test]
    #[cfg (all (feature = "sentinel" , feature = "tokio-comp"))]
    fn resp2_tcp_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : false } , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "sentinel" , feature = "smol-comp"))]
    fn resp2_tcp_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : false } , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (all (feature = "sentinel" , any (feature = "tokio-rustls-comp" , feature = "tokio-native-tls-comp")))]
    fn resp2_tls_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : true } , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "sentinel" , any (feature = "smol-rustls-comp" , feature = "smol-native-tls-comp")))]
    fn resp2_tls_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : true } , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (all (feature = "sentinel" , feature = "tokio-comp"))]
    fn resp3_tcp_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : false } , redis :: ProtocolVersion :: RESP3 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "sentinel" , feature = "smol-comp"))]
    fn resp3_tcp_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : false } , redis :: ProtocolVersion :: RESP3 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (all (feature = "sentinel" , any (feature = "tokio-rustls-comp" , feature = "tokio-native-tls-comp")))]
    fn resp3_tls_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : true } , redis :: ProtocolVersion :: RESP3 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "sentinel" , any (feature = "smol-rustls-comp" , feature = "smol-native-tls-comp")))]
    fn resp3_tls_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : true } , redis :: ProtocolVersion :: RESP3 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; } }"#
    )]
    #[case::connection(
        r#"fn test(conn: &mut Connection) {}"#,
        r#"mod test { use super :: * ; fn test_internal (conn : & mut Connection) { } #[test]
    #[cfg (all (feature = "sentinel" , feature = "tokio-comp"))]
    fn resp2_tcp_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : false } , redis :: ProtocolVersion :: RESP2 ,) ; let mut conn = ctx . async_connection () . await . unwrap () ; test_internal (& mut conn) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "sentinel" , feature = "smol-comp"))]
    fn resp2_tcp_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : false } , redis :: ProtocolVersion :: RESP2 ,) ; let mut conn = ctx . async_connection () . await . unwrap () ; test_internal (& mut conn) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (all (feature = "sentinel" , any (feature = "tokio-rustls-comp" , feature = "tokio-native-tls-comp")))]
    fn resp2_tls_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : true } , redis :: ProtocolVersion :: RESP2 ,) ; let mut conn = ctx . async_connection () . await . unwrap () ; test_internal (& mut conn) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "sentinel" , any (feature = "smol-rustls-comp" , feature = "smol-native-tls-comp")))]
    fn resp2_tls_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : true } , redis :: ProtocolVersion :: RESP2 ,) ; let mut conn = ctx . async_connection () . await . unwrap () ; test_internal (& mut conn) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (all (feature = "sentinel" , feature = "tokio-comp"))]
    fn resp3_tcp_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : false } , redis :: ProtocolVersion :: RESP3 ,) ; let mut conn = ctx . async_connection () . await . unwrap () ; test_internal (& mut conn) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "sentinel" , feature = "smol-comp"))]
    fn resp3_tcp_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : false } , redis :: ProtocolVersion :: RESP3 ,) ; let mut conn = ctx . async_connection () . await . unwrap () ; test_internal (& mut conn) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (all (feature = "sentinel" , any (feature = "tokio-rustls-comp" , feature = "tokio-native-tls-comp")))]
    fn resp3_tls_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : true } , redis :: ProtocolVersion :: RESP3 ,) ; let mut conn = ctx . async_connection () . await . unwrap () ; test_internal (& mut conn) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "sentinel" , any (feature = "smol-rustls-comp" , feature = "smol-native-tls-comp")))]
    fn resp3_tls_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : true } , redis :: ProtocolVersion :: RESP3 ,) ; let mut conn = ctx . async_connection () . await . unwrap () ; test_internal (& mut conn) . await ; } , crate :: support :: RuntimeType :: Smol) ; } }"#
    )]
    fn async_sentinel_test(#[case] item_src: &str, #[case] expected: &str) {
        let actual = expand_async_sentinel_test("".parse().unwrap(), item_src.parse().unwrap());
        assert_full(actual, expected);
    }
}
