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

    /// Asserts the produced expansion equals the expected full output. The expected is given as
    /// readable source and parsed to a token stream first, then compared token-by-token via its
    /// canonical string form (a full-output check, not a substring match).
    fn assert_full(actual: TokenStream2, expected_src: &str) {
        let expected: TokenStream2 = expected_src
            .parse()
            .expect("failed to parse expected expansion");
        assert_eq!(actual.to_string(), expected.to_string());
    }

    #[rstest::rstest]
    #[case::no_attr(
        r#""#,
        r#"fn test(ctx: &mut TestContext) {}"#,
        r#"mod test { use super :: * ; fn test_internal (ctx : & mut TestContext) { } #[test]
    fn resp2_tcp () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (& mut ctx) ; }
    #[test]
    #[cfg (any (feature = "tls-rustls" , feature = "tls-native-tls"))]
    fn resp2_tls () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (& mut ctx) ; }
    #[test]
    #[cfg (unix)]
    fn resp2_unix () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (& mut ctx) ; }
    #[test]
    fn resp3_tcp () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (& mut ctx) ; }
    #[test]
    #[cfg (any (feature = "tls-rustls" , feature = "tls-native-tls"))]
    fn resp3_tls () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (& mut ctx) ; }
    #[test]
    #[cfg (unix)]
    fn resp3_unix () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (& mut ctx) ; } }"#
    )]
    #[case::json(
        r#"json"#,
        r#"fn test(ctx: &mut TestContext) {}"#,
        r#"mod test { use super :: * ; fn test_internal (ctx : & mut TestContext) { } #[test]
    fn resp2_tcp () { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Json) . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (& mut ctx) ; }
    #[test]
    #[cfg (any (feature = "tls-rustls" , feature = "tls-native-tls"))]
    fn resp2_tls () { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Json) . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (& mut ctx) ; }
    #[test]
    #[cfg (unix)]
    fn resp2_unix () { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Json) . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (& mut ctx) ; }
    #[test]
    fn resp3_tcp () { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Json) . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (& mut ctx) ; }
    #[test]
    #[cfg (any (feature = "tls-rustls" , feature = "tls-native-tls"))]
    fn resp3_tls () { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Json) . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (& mut ctx) ; }
    #[test]
    #[cfg (unix)]
    fn resp3_unix () { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Json) . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (& mut ctx) ; } }"#
    )]
    #[case::bloom(
        r#"bloom"#,
        r#"fn test(ctx: &mut TestContext) {}"#,
        r#"mod test { use super :: * ; fn test_internal (ctx : & mut TestContext) { } #[test]
    fn resp2_tcp () { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Bloom) . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (& mut ctx) ; }
    #[test]
    #[cfg (any (feature = "tls-rustls" , feature = "tls-native-tls"))]
    fn resp2_tls () { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Bloom) . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (& mut ctx) ; }
    #[test]
    #[cfg (unix)]
    fn resp2_unix () { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Bloom) . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (& mut ctx) ; }
    #[test]
    fn resp3_tcp () { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Bloom) . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (& mut ctx) ; }
    #[test]
    #[cfg (any (feature = "tls-rustls" , feature = "tls-native-tls"))]
    fn resp3_tls () { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Bloom) . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (& mut ctx) ; }
    #[test]
    #[cfg (unix)]
    fn resp3_unix () { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Bloom) . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (& mut ctx) ; } }"#
    )]
    #[case::bool(
        r#""#,
        r#"fn test(flag: bool) {}"#,
        r#"mod test { use super :: * ; fn test_internal (flag : bool) { } #[test]
    fn resp2_tcp () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (true) ; test_internal (false) ; }
    #[test]
    #[cfg (any (feature = "tls-rustls" , feature = "tls-native-tls"))]
    fn resp2_tls () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (true) ; test_internal (false) ; }
    #[test]
    #[cfg (unix)]
    fn resp2_unix () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (true) ; test_internal (false) ; }
    #[test]
    fn resp3_tcp () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (true) ; test_internal (false) ; }
    #[test]
    #[cfg (any (feature = "tls-rustls" , feature = "tls-native-tls"))]
    fn resp3_tls () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (true) ; test_internal (false) ; }
    #[test]
    #[cfg (unix)]
    fn resp3_unix () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (true) ; test_internal (false) ; } }"#
    )]
    #[case::connection(
        r#""#,
        r#"fn test(conn: &mut Connection) {}"#,
        r#"mod test { use super :: * ; fn test_internal (conn : & mut Connection) { } #[test]
    fn resp2_tcp () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; let mut conn = ctx . connection () ; test_internal (& mut conn) ; }
    #[test]
    #[cfg (any (feature = "tls-rustls" , feature = "tls-native-tls"))]
    fn resp2_tls () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; let mut conn = ctx . connection () ; test_internal (& mut conn) ; }
    #[test]
    #[cfg (unix)]
    fn resp2_unix () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; let mut conn = ctx . connection () ; test_internal (& mut conn) ; }
    #[test]
    fn resp3_tcp () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; let mut conn = ctx . connection () ; test_internal (& mut conn) ; }
    #[test]
    #[cfg (any (feature = "tls-rustls" , feature = "tls-native-tls"))]
    fn resp3_tls () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; let mut conn = ctx . connection () ; test_internal (& mut conn) ; }
    #[test]
    #[cfg (unix)]
    fn resp3_unix () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; let mut conn = ctx . connection () ; test_internal (& mut conn) ; } }"#
    )]
    #[case::empty(
        r#""#,
        r#"fn test() {}"#,
        r#"mod test { use super :: * ; fn test_internal () { } #[test]
    fn resp2_tcp () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal () ; }
    #[test]
    #[cfg (any (feature = "tls-rustls" , feature = "tls-native-tls"))]
    fn resp2_tls () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal () ; }
    #[test]
    #[cfg (unix)]
    fn resp2_unix () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal () ; }
    #[test]
    fn resp3_tcp () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal () ; }
    #[test]
    #[cfg (any (feature = "tls-rustls" , feature = "tls-native-tls"))]
    fn resp3_tls () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal () ; }
    #[test]
    #[cfg (unix)]
    fn resp3_unix () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal () ; } }"#
    )]
    /// Each case: the `#[single_server_test]` attribute, the input function, and the full,
    /// explicit expected expansion for that scenario.
    fn single_server(#[case] attr: &str, #[case] item_src: &str, #[case] expected: &str) {
        let actual = expand_single_server_test(attr.parse().unwrap(), item_src.parse().unwrap());
        assert_full(actual, expected);
    }

    #[rstest::rstest]
    #[case::no_attr(
        r#""#,
        r#"fn test(ctx: &mut TestContext) {}"#,
        r#"mod test { use super :: * ; fn test_internal (ctx : & mut TestContext) { } #[test]
    #[cfg (feature = "tokio-comp")]
    fn resp2_tcp_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (feature = "smol-comp")]
    fn resp2_tcp_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (any (feature = "tokio-rustls-comp" , feature = "tokio-native-tls-comp"))]
    fn resp2_tls_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (any (feature = "smol-rustls-comp" , feature = "smol-native-tls-comp"))]
    fn resp2_tls_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (all (feature = "tokio-comp" , unix))]
    fn resp2_unix_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "smol-comp" , unix))]
    fn resp2_unix_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (feature = "tokio-comp")]
    fn resp3_tcp_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (feature = "smol-comp")]
    fn resp3_tcp_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (any (feature = "tokio-rustls-comp" , feature = "tokio-native-tls-comp"))]
    fn resp3_tls_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (any (feature = "smol-rustls-comp" , feature = "smol-native-tls-comp"))]
    fn resp3_tls_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (all (feature = "tokio-comp" , unix))]
    fn resp3_unix_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "smol-comp" , unix))]
    fn resp3_unix_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; } }"#
    )]
    #[case::json(
        r#"json"#,
        r#"fn test(ctx: &mut TestContext) {}"#,
        r#"mod test { use super :: * ; fn test_internal (ctx : & mut TestContext) { } #[test]
    #[cfg (feature = "tokio-comp")]
    fn resp2_tcp_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Json) . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (feature = "smol-comp")]
    fn resp2_tcp_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Json) . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (any (feature = "tokio-rustls-comp" , feature = "tokio-native-tls-comp"))]
    fn resp2_tls_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Json) . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (any (feature = "smol-rustls-comp" , feature = "smol-native-tls-comp"))]
    fn resp2_tls_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Json) . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (all (feature = "tokio-comp" , unix))]
    fn resp2_unix_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Json) . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "smol-comp" , unix))]
    fn resp2_unix_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Json) . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (feature = "tokio-comp")]
    fn resp3_tcp_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Json) . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (feature = "smol-comp")]
    fn resp3_tcp_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Json) . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (any (feature = "tokio-rustls-comp" , feature = "tokio-native-tls-comp"))]
    fn resp3_tls_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Json) . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (any (feature = "smol-rustls-comp" , feature = "smol-native-tls-comp"))]
    fn resp3_tls_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Json) . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (all (feature = "tokio-comp" , unix))]
    fn resp3_unix_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Json) . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "smol-comp" , unix))]
    fn resp3_unix_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new (). module (redis_test :: server :: Module :: Json) . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; } }"#
    )]
    #[case::bool(
        r#""#,
        r#"fn test(flag: bool) {}"#,
        r#"mod test { use super :: * ; fn test_internal (flag : bool) { } #[test]
    #[cfg (feature = "tokio-comp")]
    fn resp2_tcp_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (true) . await ; test_internal (false) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (feature = "smol-comp")]
    fn resp2_tcp_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (true) . await ; test_internal (false) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (any (feature = "tokio-rustls-comp" , feature = "tokio-native-tls-comp"))]
    fn resp2_tls_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (true) . await ; test_internal (false) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (any (feature = "smol-rustls-comp" , feature = "smol-native-tls-comp"))]
    fn resp2_tls_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (true) . await ; test_internal (false) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (all (feature = "tokio-comp" , unix))]
    fn resp2_unix_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (true) . await ; test_internal (false) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "smol-comp" , unix))]
    fn resp2_unix_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (true) . await ; test_internal (false) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (feature = "tokio-comp")]
    fn resp3_tcp_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (true) . await ; test_internal (false) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (feature = "smol-comp")]
    fn resp3_tcp_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (true) . await ; test_internal (false) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (any (feature = "tokio-rustls-comp" , feature = "tokio-native-tls-comp"))]
    fn resp3_tls_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (true) . await ; test_internal (false) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (any (feature = "smol-rustls-comp" , feature = "smol-native-tls-comp"))]
    fn resp3_tls_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (true) . await ; test_internal (false) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (all (feature = "tokio-comp" , unix))]
    fn resp3_unix_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (true) . await ; test_internal (false) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "smol-comp" , unix))]
    fn resp3_unix_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (true) . await ; test_internal (false) . await ; } , crate :: support :: RuntimeType :: Smol) ; } }"#
    )]
    #[case::connection(
        r#""#,
        r#"fn test(conn: &mut Connection) {}"#,
        r#"mod test { use super :: * ; fn test_internal (conn : & mut Connection) { } #[test]
    #[cfg (feature = "tokio-comp")]
    fn resp2_tcp_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; let mut conn = ctx . async_connection () . await . unwrap () ; test_internal (& mut conn) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (feature = "smol-comp")]
    fn resp2_tcp_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; let mut conn = ctx . async_connection () . await . unwrap () ; test_internal (& mut conn) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (any (feature = "tokio-rustls-comp" , feature = "tokio-native-tls-comp"))]
    fn resp2_tls_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; let mut conn = ctx . async_connection () . await . unwrap () ; test_internal (& mut conn) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (any (feature = "smol-rustls-comp" , feature = "smol-native-tls-comp"))]
    fn resp2_tls_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; let mut conn = ctx . async_connection () . await . unwrap () ; test_internal (& mut conn) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (all (feature = "tokio-comp" , unix))]
    fn resp2_unix_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; let mut conn = ctx . async_connection () . await . unwrap () ; test_internal (& mut conn) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "smol-comp" , unix))]
    fn resp2_unix_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; let mut conn = ctx . async_connection () . await . unwrap () ; test_internal (& mut conn) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (feature = "tokio-comp")]
    fn resp3_tcp_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; let mut conn = ctx . async_connection () . await . unwrap () ; test_internal (& mut conn) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (feature = "smol-comp")]
    fn resp3_tcp_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; let mut conn = ctx . async_connection () . await . unwrap () ; test_internal (& mut conn) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (any (feature = "tokio-rustls-comp" , feature = "tokio-native-tls-comp"))]
    fn resp3_tls_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; let mut conn = ctx . async_connection () . await . unwrap () ; test_internal (& mut conn) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (any (feature = "smol-rustls-comp" , feature = "smol-native-tls-comp"))]
    fn resp3_tls_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; let mut conn = ctx . async_connection () . await . unwrap () ; test_internal (& mut conn) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (all (feature = "tokio-comp" , unix))]
    fn resp3_unix_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; let mut conn = ctx . async_connection () . await . unwrap () ; test_internal (& mut conn) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "smol-comp" , unix))]
    fn resp3_unix_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; let mut conn = ctx . async_connection () . await . unwrap () ; test_internal (& mut conn) . await ; } , crate :: support :: RuntimeType :: Smol) ; } }"#
    )]
    #[case::empty(
        r#""#,
        r#"fn test() {}"#,
        r#"mod test { use super :: * ; fn test_internal () { } #[test]
    #[cfg (feature = "tokio-comp")]
    fn resp2_tcp_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal () . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (feature = "smol-comp")]
    fn resp2_tcp_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal () . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (any (feature = "tokio-rustls-comp" , feature = "tokio-native-tls-comp"))]
    fn resp2_tls_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal () . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (any (feature = "smol-rustls-comp" , feature = "smol-native-tls-comp"))]
    fn resp2_tls_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal () . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (all (feature = "tokio-comp" , unix))]
    fn resp2_unix_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal () . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "smol-comp" , unix))]
    fn resp2_unix_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal () . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (feature = "tokio-comp")]
    fn resp3_tcp_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal () . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (feature = "smol-comp")]
    fn resp3_tcp_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal () . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (any (feature = "tokio-rustls-comp" , feature = "tokio-native-tls-comp"))]
    fn resp3_tls_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal () . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (any (feature = "smol-rustls-comp" , feature = "smol-native-tls-comp"))]
    fn resp3_tls_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal () . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (all (feature = "tokio-comp" , unix))]
    fn resp3_unix_tokio () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal () . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "smol-comp" , unix))]
    fn resp3_unix_smol () { crate :: support :: block_on_all (async move { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal () . await ; } , crate :: support :: RuntimeType :: Smol) ; } }"#
    )]
    /// Each case: the `#[async_single_server_test]` attribute, the input function, and the full,
    /// explicit expected expansion for that scenario.
    fn async_single_server(#[case] attr: &str, #[case] item_src: &str, #[case] expected: &str) {
        let actual =
            expand_async_single_server_test(attr.parse().unwrap(), item_src.parse().unwrap());
        assert_full(actual, expected);
    }

    /// `#[single_server_test]` must propagate an `#[ignore]` on the input fn to the internal fn
    /// and every generated test fn.
    #[test]
    fn single_server_ignore_flag() {
        let item: TokenStream2 = "#[ignore]\nfn test(ctx: &mut TestContext) {}"
            .parse()
            .unwrap();
        assert_full(
            expand_single_server_test("".parse().unwrap(), item),
            r#"mod test { use super :: * ; #[ignore] fn test_internal (ctx : & mut TestContext) { } #[test] #[ignore] fn resp2_tcp () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (& mut ctx) ; } #[test] #[ignore] #[cfg (any (feature = "tls-rustls" , feature = "tls-native-tls"))] fn resp2_tls () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (& mut ctx) ; } #[test] #[ignore] #[cfg (unix)] fn resp2_unix () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (& mut ctx) ; } #[test] #[ignore] fn resp3_tcp () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (& mut ctx) ; } #[test] #[ignore] #[cfg (any (feature = "tls-rustls" , feature = "tls-native-tls"))] fn resp3_tls () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (& mut ctx) ; } #[test] #[ignore] #[cfg (unix)] fn resp3_unix () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (& mut ctx) ; } }"#,
        );
    }
}
