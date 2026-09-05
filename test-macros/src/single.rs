use proc_macro2::TokenStream as TokenStream2;
use quote::quote;

use crate::test_env::{ServerKind, protocol_enabled, server_enabled};
use crate::utils::{
    generate_async_call, generate_sync_call, ignore_flag, parse_module_from_attr,
    parse_mtls_from_attr,
};

/// A single (protocol × server kind) sync test variant.
struct SyncVariant {
    protocol: &'static str,
    kind: ServerKind,
    name: String,
    cfg: TokenStream2,
    server_type: TokenStream2,
}

/// The full sync single-server matrix (protocol × server type).
fn single_server_matrix() -> Vec<SyncVariant> {
    let tcp = quote! { redis_test::server::ServerType::Tcp { tls: false } };
    let tls = quote! { redis_test::server::ServerType::Tcp { tls: true } };
    let unix = quote! { redis_test::server::ServerType::Unix };
    let tls_cfg = quote! { #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))] };
    let unix_cfg = quote! { #[cfg(unix)] };
    let mut matrix = Vec::new();
    for protocol in ["RESP2", "RESP3"] {
        let proto_lc = protocol.to_ascii_lowercase();
        matrix.push(SyncVariant {
            protocol,
            kind: ServerKind::Tcp,
            name: format!("{proto_lc}_tcp"),
            cfg: quote! {},
            server_type: tcp.clone(),
        });
        matrix.push(SyncVariant {
            protocol,
            kind: ServerKind::Tls,
            name: format!("{proto_lc}_tls"),
            cfg: tls_cfg.clone(),
            server_type: tls.clone(),
        });
        matrix.push(SyncVariant {
            protocol,
            kind: ServerKind::Unix,
            name: format!("{proto_lc}_unix"),
            cfg: unix_cfg.clone(),
            server_type: unix.clone(),
        });
    }
    matrix
}

/// An (protocol × server kind × runtime) async test variant.
struct AsyncVariant {
    protocol: &'static str,
    kind: ServerKind,
    name: String,
    cfg: TokenStream2,
    server_type: TokenStream2,
    runtime: syn::Ident,
}

/// Builds the async single-server matrix (protocol × server type × runtime).
fn async_single_server_matrix() -> Vec<AsyncVariant> {
    let tcp = quote! { redis_test::server::ServerType::Tcp { tls: false } };
    let tls = quote! { redis_test::server::ServerType::Tcp { tls: true } };
    let unix = quote! { redis_test::server::ServerType::Unix };
    let mut matrix = Vec::new();
    for protocol in ["RESP2", "RESP3"] {
        let proto_lc = protocol.to_ascii_lowercase();
        for (kind, name, server_type) in [
            (ServerKind::Tcp, format!("{proto_lc}_tcp"), tcp.clone()),
            (ServerKind::Tls, format!("{proto_lc}_tls"), tls.clone()),
            (ServerKind::Unix, format!("{proto_lc}_unix"), unix.clone()),
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
                let feature = |name: &str| {
                    syn::LitStr::new(
                        &format!("{runtime_lc}-{name}"),
                        proc_macro2::Span::call_site(),
                    )
                };
                let cfg = match kind {
                    ServerKind::Tcp => {
                        let f = syn::LitStr::new(
                            &format!("{runtime_lc}-comp"),
                            proc_macro2::Span::call_site(),
                        );
                        quote! { #[cfg(feature = #f)] }
                    }
                    ServerKind::Tls => {
                        let rf = feature("rustls-comp");
                        let nf = feature("native-tls-comp");
                        quote! { #[cfg(any(feature = #rf, feature = #nf))] }
                    }
                    ServerKind::Unix => {
                        let f = syn::LitStr::new(
                            &format!("{runtime_lc}-comp"),
                            proc_macro2::Span::call_site(),
                        );
                        quote! { #[cfg(all(feature = #f, unix))] }
                    }
                };
                matrix.push(AsyncVariant {
                    protocol,
                    kind,
                    name: format!("{name}_{runtime_lc}"),
                    cfg,
                    server_type: server_type.clone(),
                    runtime: runtime.clone(),
                });
            }
        }
    }
    matrix
}

/// Builds the expansion for `#[single_server_test(...)]`.
pub(crate) fn expand_single_server_test(attr: TokenStream2, input: TokenStream2) -> TokenStream2 {
    let mut item: syn::ItemFn = syn::parse2(input).expect("failed to parse function");
    let test_function_name = item.sig.ident.clone();
    let module_expr = parse_module_from_attr(&attr);
    let mtls_expr = parse_mtls_from_attr(&attr);
    let is_mtls = !mtls_expr.is_empty();

    item.sig.ident = syn::Ident::new(
        &format!("{test_function_name}_internal"),
        test_function_name.span(),
    );
    let function_name = item.sig.ident.clone();
    let ignore_flag = ignore_flag(&item);
    let call_expr = generate_sync_call(&function_name, &item.sig.inputs);

    let tests = single_server_matrix()
        .into_iter()
        .filter(|v| {
            protocol_enabled(v.protocol)
                && server_enabled(v.kind)
                && (!is_mtls || v.kind == ServerKind::Tls)
        })
        .map(|v| {
            let SyncVariant {
                protocol,
                name,
                cfg,
                server_type,
                ..
            } = v;
            let protocol: syn::Ident = syn::Ident::new(protocol, proc_macro2::Span::call_site());
            let name = syn::Ident::new(&name, proc_macro2::Span::call_site());
            quote! {
                #[test]
                #ignore_flag
                #cfg
                fn #name() {
                    let mut ctx = crate::support::TestContextBuilder::new()
                        #module_expr
                        #mtls_expr
                        .protocol(redis::ProtocolVersion::#protocol)
                        .server_type(#server_type)
                        .build();
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

/// Builds the expansion for `#[async_single_server_test(...)]`.
pub(crate) fn expand_async_single_server_test(
    attr: TokenStream2,
    input: TokenStream2,
) -> TokenStream2 {
    let mut item: syn::ItemFn = syn::parse2(input).expect("failed to parse function");
    let test_function_name = item.sig.ident.clone();
    let module_expr = parse_module_from_attr(&attr);
    let mtls_expr = parse_mtls_from_attr(&attr);
    let is_mtls = !mtls_expr.is_empty();

    item.sig.ident = syn::Ident::new(
        &format!("{test_function_name}_internal"),
        test_function_name.span(),
    );
    let function_name = item.sig.ident.clone();
    let ignore_flag = ignore_flag(&item);
    let call_expr = generate_async_call(&function_name, &item.sig.inputs);

    let tests = async_single_server_matrix()
        .into_iter()
        .filter(|v| {
            protocol_enabled(v.protocol)
                && server_enabled(v.kind)
                && (!is_mtls || v.kind == ServerKind::Tls)
        })
        .map(|v| {
            let AsyncVariant {
                protocol,
                name,
                cfg,
                server_type,
                runtime,
                ..
            } = v;
            let protocol: syn::Ident = syn::Ident::new(protocol, proc_macro2::Span::call_site());
            let name = syn::Ident::new(&name, proc_macro2::Span::call_site());
            quote! {
                #[test]
                #ignore_flag
                #cfg
                fn #name() {
                    crate::support::block_on_all(async move {
                        let mut ctx = crate::support::TestContextBuilder::new()
                            #module_expr
                            #mtls_expr
                            .protocol(redis::ProtocolVersion::#protocol)
                            .server_type(#server_type)
                            .build();
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
        with_env(clear_env, || {
            let actual =
                expand_single_server_test(attr.parse().unwrap(), item_src.parse().unwrap());
            assert_full(actual, expected);
        });
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
        with_env(clear_env, || {
            let actual =
                expand_async_single_server_test(attr.parse().unwrap(), item_src.parse().unwrap());
            assert_full(actual, expected);
        });
    }

    /// `#[single_server_test]` must propagate an `#[ignore]` on the input fn to the internal fn
    /// and every generated test fn.
    #[test]
    fn single_server_ignore_flag() {
        with_env(clear_env, || {
            let item: TokenStream2 = "#[ignore]\nfn test(ctx: &mut TestContext) {}"
                .parse()
                .unwrap();
            assert_full(
                expand_single_server_test("".parse().unwrap(), item),
                r#"mod test { use super :: * ; #[ignore] fn test_internal (ctx : & mut TestContext) { } #[test] #[ignore] fn resp2_tcp () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (& mut ctx) ; } #[test] #[ignore] #[cfg (any (feature = "tls-rustls" , feature = "tls-native-tls"))] fn resp2_tls () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (& mut ctx) ; } #[test] #[ignore] #[cfg (unix)] fn resp2_unix () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (& mut ctx) ; } #[test] #[ignore] fn resp3_tcp () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (& mut ctx) ; } #[test] #[ignore] #[cfg (any (feature = "tls-rustls" , feature = "tls-native-tls"))] fn resp3_tls () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (& mut ctx) ; } #[test] #[ignore] #[cfg (unix)] fn resp3_unix () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (& mut ctx) ; } }"#,
            );
        });
    }

    /// `REDISRS_SERVER_TYPE` and `PROTOCOL` filter which variants are generated. Each scenario
    /// expands under a specific env combination inside `with_env`, which holds the process-env lock
    /// so it cannot race with the full-output oracle tests in other threads.
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
                    let out = expand_single_server_test(
                        "".parse().unwrap(),
                        "fn test(ctx: &mut TestContext) {}".parse().unwrap(),
                    );
                    out.to_string()
                },
            )
        };

        assert_eq!(
            expand("tcp", None),
            r#"mod test { use super :: * ; fn test_internal (ctx : & mut TestContext) { } #[test] fn resp2_tcp () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (& mut ctx) ; } #[test] fn resp3_tcp () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : false }) . build () ; test_internal (& mut ctx) ; } }"#
                .parse::<TokenStream2>()
                .unwrap()
                .to_string()
        );

        assert_eq!(
            expand("tcp+tls", None),
            r#"mod test { use super :: * ; fn test_internal (ctx : & mut TestContext) { } #[test] #[cfg (any (feature = "tls-rustls" , feature = "tls-native-tls"))] fn resp2_tls () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP2) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (& mut ctx) ; } #[test] #[cfg (any (feature = "tls-rustls" , feature = "tls-native-tls"))] fn resp3_tls () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Tcp { tls : true }) . build () ; test_internal (& mut ctx) ; } }"#
                .parse::<TokenStream2>()
                .unwrap()
                .to_string()
        );

        assert_eq!(
            expand("unix", Some("RESP3")),
            r#"mod test { use super :: * ; fn test_internal (ctx : & mut TestContext) { } #[test] #[cfg (unix)] fn resp3_unix () { let mut ctx = crate :: support :: TestContextBuilder :: new () . protocol (redis :: ProtocolVersion :: RESP3) . server_type (redis_test :: server :: ServerType :: Unix) . build () ; test_internal (& mut ctx) ; } }"#
                .parse::<TokenStream2>()
                .unwrap()
                .to_string()
        );
    }

    /// `mtls = true` must only generate TLS variants and include `.mtls(true)` in the builder.
    #[test]
    fn mtls_only_tls_variants() {
        with_env(clear_env, || {
            let out = expand_single_server_test(
                r#"mtls = true"#.parse().unwrap(),
                "fn test(ctx: &mut TestContext) {}".parse().unwrap(),
            );
            let s = out.to_string();
            // Must contain .mtls(true)
            assert!(s.contains(". mtls (true)"), "missing .mtls(true)");
            // Must contain only tls variants
            assert!(s.contains("resp2_tls"), "missing resp2_tls");
            assert!(s.contains("resp3_tls"), "missing resp3_tls");
            assert!(!s.contains("resp2_tcp"), "should not contain resp2_tcp");
            assert!(!s.contains("resp3_tcp"), "should not contain resp3_tcp");
            assert!(!s.contains("resp2_unix"), "should not contain resp2_unix");
            assert!(!s.contains("resp3_unix"), "should not contain resp3_unix");
        });
    }

    /// `mtls = true` must only generate TLS variants for async too.
    #[test]
    fn mtls_async_only_tls_variants() {
        with_env(clear_env, || {
            let out = expand_async_single_server_test(
                r#"mtls = true"#.parse().unwrap(),
                "fn test(ctx: &mut TestContext) {}".parse().unwrap(),
            );
            let s = out.to_string();
            assert!(s.contains(". mtls (true)"), "missing .mtls(true)");
            assert!(s.contains("resp2_tls_tokio"), "missing resp2_tls_tokio");
            assert!(s.contains("resp3_tls_tokio"), "missing resp3_tls_tokio");
            assert!(
                !s.contains("resp2_tcp_tokio"),
                "should not contain tcp variants"
            );
            assert!(
                !s.contains("resp2_unix_tokio"),
                "should not contain unix variants"
            );
        });
    }

    /// `module = "json"` named-arg form must work the same as bare `json`.
    #[test]
    fn module_named_arg_json() {
        with_env(clear_env, || {
            let out = expand_single_server_test(
                r#"module = "json""#.parse().unwrap(),
                "fn test(ctx: &mut TestContext) {}".parse().unwrap(),
            );
            let s = out.to_string();
            assert!(
                s.contains(". module (redis_test :: server :: Module :: Json)"),
                "missing Module::Json"
            );
            // Should still have all 6 variants (no filtering)
            assert!(s.contains("resp2_tcp"));
            assert!(s.contains("resp3_tcp"));
            assert!(s.contains("resp2_tls"));
            assert!(s.contains("resp3_tls"));
            assert!(s.contains("resp2_unix"));
            assert!(s.contains("resp3_unix"));
        });
    }

    /// `module = "json", mtls = true` must combine both: module loaded + TLS-only variants.
    #[test]
    fn module_and_mtls_combined() {
        with_env(clear_env, || {
            let out = expand_single_server_test(
                r#"module = "json", mtls = true"#.parse().unwrap(),
                "fn test(ctx: &mut TestContext) {}".parse().unwrap(),
            );
            let s = out.to_string();
            assert!(
                s.contains(". module (redis_test :: server :: Module :: Json)"),
                "missing Module::Json"
            );
            assert!(s.contains(". mtls (true)"), "missing .mtls(true)");
            assert!(s.contains("resp2_tls"));
            assert!(s.contains("resp3_tls"));
            assert!(!s.contains("resp2_tcp"));
            assert!(!s.contains("resp2_unix"));
        });
    }
}
