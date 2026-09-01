use proc_macro2::TokenStream as TokenStream2;
use quote::quote;

use crate::test_env::{ServerKind, protocol_enabled, server_enabled};
use crate::utils::{
    generate_async_cluster_call, generate_sync_call, generate_version_check, ignore_flag,
    parse_cluster_test_args,
};

/// A (protocol × server kind) cluster test variant. Clusters do not support unix sockets.
struct ClusterVariant {
    protocol: &'static str,
    kind: ServerKind,
    name: String,
    cfg: TokenStream2,
    cluster_type: syn::Ident,
}

/// Builds the full cluster matrix: protocol × (TCP, TCP+TLS).
fn cluster_matrix() -> Vec<ClusterVariant> {
    let mut matrix = Vec::new();
    for protocol in ["RESP2", "RESP3"] {
        let proto_lc = protocol.to_ascii_lowercase();
        matrix.push(ClusterVariant {
            protocol,
            kind: ServerKind::Tcp,
            name: format!("{}_tcp", proto_lc),
            cfg: quote! { #[cfg(feature = "cluster")] },
            cluster_type: syn::Ident::new("Tcp", proc_macro2::Span::call_site()),
        });
        matrix.push(ClusterVariant {
            protocol,
            kind: ServerKind::Tls,
            name: format!("{}_tls", proto_lc),
            cfg: quote! {
                #[cfg(all(feature = "cluster", any(feature = "tls-rustls", feature = "tls-native-tls")))]
            },
            cluster_type: syn::Ident::new("TcpTls", proc_macro2::Span::call_site()),
        });
    }
    matrix
}

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

    let tests = cluster_matrix()
        .into_iter()
        .filter(|v| protocol_enabled(v.protocol) && server_enabled(v.kind))
        .map(|v| {
            let ClusterVariant {
                protocol,
                name,
                cfg,
                cluster_type,
                ..
            } = v;
            let protocol: syn::Ident = syn::Ident::new(protocol, proc_macro2::Span::call_site());
            let name = syn::Ident::new(&name, proc_macro2::Span::call_site());
            quote! {
                #[test]
                #ignore_flag
                #cfg
                fn #name() {
                    #version_check
                    let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                        (#config_expr)
                            .cluster_type(redis_test::cluster::ClusterType::#cluster_type),
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

/// An (protocol × server kind × runtime) async cluster test variant.
struct AsyncClusterVariant {
    protocol: &'static str,
    kind: ServerKind,
    name: String,
    cfg: TokenStream2,
    cluster_type: syn::Ident,
    runtime: syn::Ident,
}

/// Builds the full async cluster matrix: protocol × (TCP, TCP+TLS) × runtime.
fn async_cluster_matrix() -> Vec<AsyncClusterVariant> {
    let mut matrix = Vec::new();
    for protocol in ["RESP2", "RESP3"] {
        let proto_lc = protocol.to_ascii_lowercase();
        for (kind, name, cluster_type) in [
            (
                ServerKind::Tcp,
                format!("{}_tcp", proto_lc),
                syn::Ident::new("Tcp", proc_macro2::Span::call_site()),
            ),
            (
                ServerKind::Tls,
                format!("{}_tls", proto_lc),
                syn::Ident::new("TcpTls", proc_macro2::Span::call_site()),
            ),
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
                let cfg = match kind {
                    ServerKind::Tcp => {
                        let f = syn::LitStr::new(
                            &format!("{}-comp", runtime_lc),
                            proc_macro2::Span::call_site(),
                        );
                        quote! { #[cfg(all(feature = "cluster-async", feature = #f))] }
                    }
                    ServerKind::Tls => {
                        let rf = syn::LitStr::new(
                            &format!("{}-rustls-comp", runtime_lc),
                            proc_macro2::Span::call_site(),
                        );
                        let nf = syn::LitStr::new(
                            &format!("{}-native-tls-comp", runtime_lc),
                            proc_macro2::Span::call_site(),
                        );
                        quote! {
                            #[cfg(all(feature = "cluster-async", any(feature = #rf, feature = #nf)))]
                        }
                    }
                    ServerKind::Unix => unreachable!("clusters do not support unix sockets"),
                };
                matrix.push(AsyncClusterVariant {
                    protocol,
                    kind,
                    name: format!("{}_{}", name, runtime_lc),
                    cfg,
                    cluster_type: cluster_type.clone(),
                    runtime: runtime.clone(),
                });
            }
        }
    }
    matrix
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

    let tests = async_cluster_matrix()
        .into_iter()
        .filter(|v| protocol_enabled(v.protocol) && server_enabled(v.kind))
        .map(|v| {
            let AsyncClusterVariant {
                protocol,
                name,
                cfg,
                cluster_type,
                runtime,
                ..
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
                        #version_check
                        let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                            (#config_expr)
                                .cluster_type(redis_test::cluster::ClusterType::#cluster_type),
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

    /// Each case: the `#[cluster_test(...)]` attribute and the full, explicit expected
    /// expansion for that scenario.
    #[rstest::rstest]
    #[case::default(
        r#""#,
        r#"mod test { use super :: * ; fn test_internal (ctx : & mut TestContext) { } #[test]
    #[cfg (feature = "cluster")]
    fn resp2_tcp () {  let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((redis_test :: cluster :: RedisClusterConfiguration :: default () . insecure_tls ()) . cluster_type (redis_test :: cluster :: ClusterType :: Tcp) , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (& mut ctx) ; }
    #[test]
    #[cfg (all (feature = "cluster" , any (feature = "tls-rustls" , feature = "tls-native-tls")))]
    fn resp2_tls () {  let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((redis_test :: cluster :: RedisClusterConfiguration :: default () . insecure_tls ()) . cluster_type (redis_test :: cluster :: ClusterType :: TcpTls) , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (& mut ctx) ; }
    #[test]
    #[cfg (feature = "cluster")]
    fn resp3_tcp () {  let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((redis_test :: cluster :: RedisClusterConfiguration :: default () . insecure_tls ()) . cluster_type (redis_test :: cluster :: ClusterType :: Tcp) , redis :: ProtocolVersion :: RESP3 ,) ; test_internal (& mut ctx) ; }
    #[test]
    #[cfg (all (feature = "cluster" , any (feature = "tls-rustls" , feature = "tls-native-tls")))]
    fn resp3_tls () {  let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((redis_test :: cluster :: RedisClusterConfiguration :: default () . insecure_tls ()) . cluster_type (redis_test :: cluster :: ClusterType :: TcpTls) , redis :: ProtocolVersion :: RESP3 ,) ; test_internal (& mut ctx) ; } }"#
    )]
    #[case::config_and_versions(
        r#"config = "foo()", supported_versions = "Json""#,
        r#"mod test { use super :: * ; fn test_internal (ctx : & mut TestContext) { } #[test]
    #[cfg (feature = "cluster")]
    fn resp2_tcp () { { let version_check_ctx = crate :: support :: TestContextBuilder :: new () . build () ; if ! crate :: support :: TestContextVersioning :: supports (& version_check_ctx , Json) { eprintln ! ("Skipping the test because the running server does not support {:?}." , Json) ; return ; } } let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((foo ()) . cluster_type (redis_test :: cluster :: ClusterType :: Tcp) , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (& mut ctx) ; }
    #[test]
    #[cfg (all (feature = "cluster" , any (feature = "tls-rustls" , feature = "tls-native-tls")))]
    fn resp2_tls () { { let version_check_ctx = crate :: support :: TestContextBuilder :: new () . build () ; if ! crate :: support :: TestContextVersioning :: supports (& version_check_ctx , Json) { eprintln ! ("Skipping the test because the running server does not support {:?}." , Json) ; return ; } } let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((foo ()) . cluster_type (redis_test :: cluster :: ClusterType :: TcpTls) , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (& mut ctx) ; }
    #[test]
    #[cfg (feature = "cluster")]
    fn resp3_tcp () { { let version_check_ctx = crate :: support :: TestContextBuilder :: new () . build () ; if ! crate :: support :: TestContextVersioning :: supports (& version_check_ctx , Json) { eprintln ! ("Skipping the test because the running server does not support {:?}." , Json) ; return ; } } let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((foo ()) . cluster_type (redis_test :: cluster :: ClusterType :: Tcp) , redis :: ProtocolVersion :: RESP3 ,) ; test_internal (& mut ctx) ; }
    #[test]
    #[cfg (all (feature = "cluster" , any (feature = "tls-rustls" , feature = "tls-native-tls")))]
    fn resp3_tls () { { let version_check_ctx = crate :: support :: TestContextBuilder :: new () . build () ; if ! crate :: support :: TestContextVersioning :: supports (& version_check_ctx , Json) { eprintln ! ("Skipping the test because the running server does not support {:?}." , Json) ; return ; } } let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((foo ()) . cluster_type (redis_test :: cluster :: ClusterType :: TcpTls) , redis :: ProtocolVersion :: RESP3 ,) ; test_internal (& mut ctx) ; } }"#
    )]
    fn cluster_test(#[case] attr: &str, #[case] expected: &str) {
        with_env(clear_env, || {
            let actual = expand_cluster_test(
                attr.parse().unwrap(),
                "fn test(ctx: &mut TestContext) {}".parse().unwrap(),
            );
            assert_full(actual, expected);
        });
    }

    /// Each case: the `#[async_cluster_test(...)]` attribute and the full, explicit expected
    /// expansion for that scenario.
    #[rstest::rstest]
    #[case::default(
        r#""#,
        r#"mod test { use super :: * ; fn test_internal (ctx : & mut TestContext) { } #[test]
    #[cfg (all (feature = "cluster-async" , feature = "tokio-comp"))]
    fn resp2_tcp_tokio () { crate :: support :: block_on_all (async move {  let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((redis_test :: cluster :: RedisClusterConfiguration :: default () . insecure_tls ()) . cluster_type (redis_test :: cluster :: ClusterType :: Tcp) , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "cluster-async" , feature = "smol-comp"))]
    fn resp2_tcp_smol () { crate :: support :: block_on_all (async move {  let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((redis_test :: cluster :: RedisClusterConfiguration :: default () . insecure_tls ()) . cluster_type (redis_test :: cluster :: ClusterType :: Tcp) , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (all (feature = "cluster-async" , any (feature = "tokio-rustls-comp" , feature = "tokio-native-tls-comp")))]
    fn resp2_tls_tokio () { crate :: support :: block_on_all (async move {  let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((redis_test :: cluster :: RedisClusterConfiguration :: default () . insecure_tls ()) . cluster_type (redis_test :: cluster :: ClusterType :: TcpTls) , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "cluster-async" , any (feature = "smol-rustls-comp" , feature = "smol-native-tls-comp")))]
    fn resp2_tls_smol () { crate :: support :: block_on_all (async move {  let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((redis_test :: cluster :: RedisClusterConfiguration :: default () . insecure_tls ()) . cluster_type (redis_test :: cluster :: ClusterType :: TcpTls) , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (all (feature = "cluster-async" , feature = "tokio-comp"))]
    fn resp3_tcp_tokio () { crate :: support :: block_on_all (async move {  let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((redis_test :: cluster :: RedisClusterConfiguration :: default () . insecure_tls ()) . cluster_type (redis_test :: cluster :: ClusterType :: Tcp) , redis :: ProtocolVersion :: RESP3 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "cluster-async" , feature = "smol-comp"))]
    fn resp3_tcp_smol () { crate :: support :: block_on_all (async move {  let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((redis_test :: cluster :: RedisClusterConfiguration :: default () . insecure_tls ()) . cluster_type (redis_test :: cluster :: ClusterType :: Tcp) , redis :: ProtocolVersion :: RESP3 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (all (feature = "cluster-async" , any (feature = "tokio-rustls-comp" , feature = "tokio-native-tls-comp")))]
    fn resp3_tls_tokio () { crate :: support :: block_on_all (async move {  let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((redis_test :: cluster :: RedisClusterConfiguration :: default () . insecure_tls ()) . cluster_type (redis_test :: cluster :: ClusterType :: TcpTls) , redis :: ProtocolVersion :: RESP3 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "cluster-async" , any (feature = "smol-rustls-comp" , feature = "smol-native-tls-comp")))]
    fn resp3_tls_smol () { crate :: support :: block_on_all (async move {  let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((redis_test :: cluster :: RedisClusterConfiguration :: default () . insecure_tls ()) . cluster_type (redis_test :: cluster :: ClusterType :: TcpTls) , redis :: ProtocolVersion :: RESP3 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; } }"#
    )]
    #[case::config(
        r#"config = "foo()""#,
        r#"mod test { use super :: * ; fn test_internal (ctx : & mut TestContext) { } #[test]
    #[cfg (all (feature = "cluster-async" , feature = "tokio-comp"))]
    fn resp2_tcp_tokio () { crate :: support :: block_on_all (async move {  let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((foo ()) . cluster_type (redis_test :: cluster :: ClusterType :: Tcp) , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "cluster-async" , feature = "smol-comp"))]
    fn resp2_tcp_smol () { crate :: support :: block_on_all (async move {  let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((foo ()) . cluster_type (redis_test :: cluster :: ClusterType :: Tcp) , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (all (feature = "cluster-async" , any (feature = "tokio-rustls-comp" , feature = "tokio-native-tls-comp")))]
    fn resp2_tls_tokio () { crate :: support :: block_on_all (async move {  let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((foo ()) . cluster_type (redis_test :: cluster :: ClusterType :: TcpTls) , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "cluster-async" , any (feature = "smol-rustls-comp" , feature = "smol-native-tls-comp")))]
    fn resp2_tls_smol () { crate :: support :: block_on_all (async move {  let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((foo ()) . cluster_type (redis_test :: cluster :: ClusterType :: TcpTls) , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (all (feature = "cluster-async" , feature = "tokio-comp"))]
    fn resp3_tcp_tokio () { crate :: support :: block_on_all (async move {  let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((foo ()) . cluster_type (redis_test :: cluster :: ClusterType :: Tcp) , redis :: ProtocolVersion :: RESP3 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "cluster-async" , feature = "smol-comp"))]
    fn resp3_tcp_smol () { crate :: support :: block_on_all (async move {  let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((foo ()) . cluster_type (redis_test :: cluster :: ClusterType :: Tcp) , redis :: ProtocolVersion :: RESP3 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; }
    #[test]
    #[cfg (all (feature = "cluster-async" , any (feature = "tokio-rustls-comp" , feature = "tokio-native-tls-comp")))]
    fn resp3_tls_tokio () { crate :: support :: block_on_all (async move {  let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((foo ()) . cluster_type (redis_test :: cluster :: ClusterType :: TcpTls) , redis :: ProtocolVersion :: RESP3 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Tokio) ; }
    #[test]
    #[cfg (all (feature = "cluster-async" , any (feature = "smol-rustls-comp" , feature = "smol-native-tls-comp")))]
    fn resp3_tls_smol () { crate :: support :: block_on_all (async move {  let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((foo ()) . cluster_type (redis_test :: cluster :: ClusterType :: TcpTls) , redis :: ProtocolVersion :: RESP3 ,) ; test_internal (& mut ctx) . await ; } , crate :: support :: RuntimeType :: Smol) ; } }"#
    )]
    fn async_cluster_test(#[case] attr: &str, #[case] expected: &str) {
        with_env(clear_env, || {
            let actual = expand_async_cluster_test(
                attr.parse().unwrap(),
                "fn test(ctx: &mut TestContext) {}".parse().unwrap(),
            );
            assert_full(actual, expected);
        });
    }

    /// `REDISRS_SERVER_TYPE`/`PROTOCOL` filter which cluster variants are generated. Scenarios run
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
                    expand_cluster_test(
                        "".parse().unwrap(),
                        "fn test(ctx: &mut TestContext) {}".parse().unwrap(),
                    )
                    .to_string()
                },
            )
        };

        assert_eq!(
            expand("tcp", None),
            r#"mod test { use super :: * ; fn test_internal (ctx : & mut TestContext) { } #[test] #[cfg (feature = "cluster")] fn resp2_tcp () { let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((redis_test :: cluster :: RedisClusterConfiguration :: default () . insecure_tls ()) . cluster_type (redis_test :: cluster :: ClusterType :: Tcp) , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (& mut ctx) ; } #[test] #[cfg (feature = "cluster")] fn resp3_tcp () { let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((redis_test :: cluster :: RedisClusterConfiguration :: default () . insecure_tls ()) . cluster_type (redis_test :: cluster :: ClusterType :: Tcp) , redis :: ProtocolVersion :: RESP3 ,) ; test_internal (& mut ctx) ; } }"#
                .parse::<TokenStream2>()
                .unwrap()
                .to_string()
        );

        assert_eq!(
            expand("tcp+tls", Some("RESP2")),
            r#"mod test { use super :: * ; fn test_internal (ctx : & mut TestContext) { } #[test] #[cfg (all (feature = "cluster" , any (feature = "tls-rustls" , feature = "tls-native-tls")))] fn resp2_tls () { let mut ctx = crate :: support :: TestClusterContext :: new_with_config_and_protocol ((redis_test :: cluster :: RedisClusterConfiguration :: default () . insecure_tls ()) . cluster_type (redis_test :: cluster :: ClusterType :: TcpTls) , redis :: ProtocolVersion :: RESP2 ,) ; test_internal (& mut ctx) ; } }"#
                .parse::<TokenStream2>()
                .unwrap()
                .to_string()
        );
    }
}
