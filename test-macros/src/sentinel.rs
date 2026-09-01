use proc_macro2::TokenStream as TokenStream2;
use quote::quote;

use crate::test_env::{ServerKind, protocol_enabled, server_enabled};
use crate::utils::{generate_async_call, generate_sync_call, ignore_flag};

/// A (protocol × server kind) sentinel test variant. Sentinel does not support unix sockets.
struct SentinelVariant {
    protocol: &'static str,
    name: String,
    cfg: TokenStream2,
    tls: bool,
}

/// Builds the full sentinel matrix: protocol × (TCP, TCP+TLS).
fn sentinel_matrix() -> Vec<SentinelVariant> {
    let mut matrix = Vec::new();
    for protocol in ["RESP2", "RESP3"] {
        let proto_lc = protocol.to_ascii_lowercase();
        matrix.push(SentinelVariant {
            protocol,
            name: format!("{}_tcp", proto_lc),
            cfg: quote! { #[cfg(feature = "sentinel")] },
            tls: false,
        });
        matrix.push(SentinelVariant {
            protocol,
            name: format!("{}_tls", proto_lc),
            cfg: quote! {
                #[cfg(all(feature = "sentinel", any(feature = "tls-rustls", feature = "tls-native-tls")))]
            },
            tls: true,
        });
    }
    matrix
}

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

    let tests = sentinel_matrix()
        .into_iter()
        .filter(|v| {
            protocol_enabled(v.protocol)
                && server_enabled(if v.tls { ServerKind::Tls } else { ServerKind::Tcp })
        })
        .map(|v| {
            let SentinelVariant {
                protocol,
                name,
                cfg,
                tls,
            } = v;
            let protocol: syn::Ident =
                syn::Ident::new(protocol, proc_macro2::Span::call_site());
            let name = syn::Ident::new(&name, proc_macro2::Span::call_site());
            quote! {
                #[test]
                #ignore_flag
                #cfg
                fn #name() {
                    let mut ctx = crate::support::TestSentinelContext::new_with_server_type_and_protocol(
                        2,
                        3,
                        3,
                        redis_test::server::ServerType::Tcp { tls: #tls },
                        redis::ProtocolVersion::#protocol,
                    );
                    #call_expr
                }
            }
        });

    quote! {
        mod #test_function_name {
            use super::*;
            #item
            #(#tests)*
        }
    }
}

/// An (protocol × server kind × runtime) async sentinel test variant.
struct AsyncSentinelVariant {
    protocol: &'static str,
    name: String,
    cfg: TokenStream2,
    tls: bool,
    runtime: syn::Ident,
}

/// Builds the full async sentinel matrix: protocol × (TCP, TCP+TLS) × runtime.
fn async_sentinel_matrix() -> Vec<AsyncSentinelVariant> {
    let mut matrix = Vec::new();
    for protocol in ["RESP2", "RESP3"] {
        let proto_lc = protocol.to_ascii_lowercase();
        for (name, tls) in [
            (format!("{}_tcp", proto_lc), false),
            (format!("{}_tls", proto_lc), true),
        ] {
            for (runtime, runtime_lc) in [
                (
                    syn::Ident::new("Tokio", proc_macro2::Span::call_site()),
                    "tokio",
                ),
                (
                    syn::Ident::new("Smol", proc_macro2::Span::call_site()),
                    "smol",
                ),
            ] {
                let cfg = if tls {
                    let rf = syn::LitStr::new(
                        &format!("{}-rustls-comp", runtime_lc),
                        proc_macro2::Span::call_site(),
                    );
                    let nf = syn::LitStr::new(
                        &format!("{}-native-tls-comp", runtime_lc),
                        proc_macro2::Span::call_site(),
                    );
                    quote! { #[cfg(all(feature = "sentinel", any(feature = #rf, feature = #nf)))] }
                } else {
                    let f = syn::LitStr::new(
                        &format!("{}-comp", runtime_lc),
                        proc_macro2::Span::call_site(),
                    );
                    quote! { #[cfg(all(feature = "sentinel", feature = #f))] }
                };
                matrix.push(AsyncSentinelVariant {
                    protocol,
                    name: format!("{}_{}", name, runtime_lc),
                    cfg,
                    tls,
                    runtime: runtime.clone(),
                });
            }
        }
    }
    matrix
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

    let tests = async_sentinel_matrix()
        .into_iter()
        .filter(|v| {
            protocol_enabled(v.protocol)
                && server_enabled(if v.tls { ServerKind::Tls } else { ServerKind::Tcp })
        })
        .map(|v| {
            let AsyncSentinelVariant {
                protocol,
                name,
                cfg,
                tls,
                runtime,
            } = v;
            let protocol: syn::Ident =
                syn::Ident::new(protocol, proc_macro2::Span::call_site());
            let name = syn::Ident::new(&name, proc_macro2::Span::call_site());
            quote! {
                #[test]
                #ignore_flag
                #cfg
                fn #name() {
                    crate::support::block_on_all(async move {
                        let mut ctx = crate::support::TestSentinelContext::new_with_server_type_and_protocol(
                            2,
                            3,
                            3,
                            redis_test::server::ServerType::Tcp { tls: #tls },
                            redis::ProtocolVersion::#protocol,
                        );
                        #call_expr
                    }, crate::support::RuntimeType::#runtime);
                }
            }
        });

    quote! {
        mod #test_function_name {
            use super::*;
            #item
            #(#tests)*
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_env::{clear_env, with_env};

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
        with_env(clear_env, || {
            let actual = expand_sentinel_test("".parse().unwrap(), item_src.parse().unwrap());
            assert_full(actual, expected);
        });
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
        with_env(clear_env, || {
            let actual = expand_async_sentinel_test("".parse().unwrap(), item_src.parse().unwrap());
            assert_full(actual, expected);
        });
    }

    /// `REDISRS_SERVER_TYPE`/`PROTOCOL` filter which sentinel variants are generated. Scenarios run
    /// inside `with_env`, which holds the process-env lock so they don't race the oracle tests.
    #[test]
    fn env_filtered() {
        let expand = |server: &str, protocol: Option<&str>| {
            with_env(
                || unsafe {
                    std::env::set_var("REDISRS_SERVER_TYPE", server);
                    if let Some(p) = protocol {
                        std::env::set_var("PROTOCOL", p);
                    }
                },
                || {
                    expand_sentinel_test(
                        "".parse().unwrap(),
                        "fn test(ctx: &mut TestContext) {}".parse().unwrap(),
                    )
                    .to_string()
                },
            )
        };

        assert_eq!(
            expand("tcp", None),
            r#"mod test { use super :: * ; fn test_internal (ctx : & mut TestContext) { } #[test] #[cfg (feature = "sentinel")] fn resp2_tcp () { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : false } , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (& mut ctx) ; } #[test] #[cfg (feature = "sentinel")] fn resp3_tcp () { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : false } , redis :: ProtocolVersion :: RESP3 ,) ; test_internal (& mut ctx) ; } }"#
                .parse::<TokenStream2>()
                .unwrap()
                .to_string()
        );

        assert_eq!(
            expand("tcp+tls", Some("RESP2")),
            r#"mod test { use super :: * ; fn test_internal (ctx : & mut TestContext) { } #[test] #[cfg (all (feature = "sentinel" , any (feature = "tls-rustls" , feature = "tls-native-tls")))] fn resp2_tls () { let mut ctx = crate :: support :: TestSentinelContext :: new_with_server_type_and_protocol (2 , 3 , 3 , redis_test :: server :: ServerType :: Tcp { tls : true } , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (& mut ctx) ; } }"#
                .parse::<TokenStream2>()
                .unwrap()
                .to_string()
        );
    }
}
