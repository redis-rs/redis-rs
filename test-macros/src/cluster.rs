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
        let actual = expand_cluster_test(
            attr.parse().unwrap(),
            "fn test(ctx: &mut TestContext) {}".parse().unwrap(),
        );
        assert_full(actual, expected);
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
        let actual = expand_async_cluster_test(
            attr.parse().unwrap(),
            "fn test(ctx: &mut TestContext) {}".parse().unwrap(),
        );
        assert_full(actual, expected);
    }
}
