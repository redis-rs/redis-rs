use proc_macro::TokenStream;
use quote::quote;
use syn::parse_macro_input;

fn has_ignore_attr(item: &syn::ItemFn) -> bool {
    item.attrs.iter().any(|a| a.path().is_ident("ignore"))
}

fn ignore_flag(item: &syn::ItemFn) -> proc_macro2::TokenStream {
    if has_ignore_attr(item) {
        quote! { #[ignore] }
    } else {
        quote! {}
    }
}

fn is_connection_type(ty: &syn::Type) -> bool {
    let type_str = quote!(#ty).to_string();
    type_str.contains("Connection")
        || type_str.contains("ConnectionLike")
        || type_str.contains("AsyncCommands")
        || type_str.contains("AsyncTypedCommands")
}

fn is_bool_type(ty: &syn::Type) -> bool {
    let type_str = quote!(#ty).to_string();
    type_str == "bool"
}

fn parse_module_from_attr(attr: &TokenStream) -> proc_macro2::TokenStream {
    let attr_str = attr.to_string();
    if attr_str.contains("json") {
        quote! { .module(redis_test::server::Module::Json) }
    } else if attr_str.contains("bloom") {
        quote! { .module(redis_test::server::Module::Bloom) }
    } else {
        quote! {}
    }
}

fn generate_sync_call(
    function_name: &syn::Ident,
    inputs: &syn::punctuated::Punctuated<syn::FnArg, syn::token::Comma>,
) -> proc_macro2::TokenStream {
    if inputs.is_empty() {
        quote! { #function_name(); }
    } else {
        let arg = inputs.first().unwrap();
        if let syn::FnArg::Typed(pat_type) = arg {
            let is_ref = matches!(&*pat_type.ty, syn::Type::Reference(_));
            let is_conn = is_connection_type(&pat_type.ty);
            let is_bool = is_bool_type(&pat_type.ty);

            if is_bool {
                quote! {
                    #function_name(true);
                    #function_name(false);
                }
            } else if is_conn {
                if is_ref {
                    quote! {
                        let mut conn = ctx.connection();
                        #function_name(&mut conn);
                    }
                } else {
                    quote! {
                        let conn = ctx.connection();
                        #function_name(conn);
                    }
                }
            } else if is_ref {
                quote! {
                    #function_name(&mut ctx);
                }
            } else {
                quote! {
                    #function_name(ctx);
                }
            }
        } else {
            quote! { #function_name(); }
        }
    }
}

fn generate_async_call(
    function_name: &syn::Ident,
    inputs: &syn::punctuated::Punctuated<syn::FnArg, syn::token::Comma>,
) -> proc_macro2::TokenStream {
    if inputs.is_empty() {
        quote! { #function_name().await; }
    } else {
        let arg = inputs.first().unwrap();
        if let syn::FnArg::Typed(pat_type) = arg {
            let is_ref = matches!(&*pat_type.ty, syn::Type::Reference(_));
            let is_conn = is_connection_type(&pat_type.ty);
            let is_bool = is_bool_type(&pat_type.ty);

            if is_bool {
                quote! {
                    #function_name(true).await;
                    #function_name(false).await;
                }
            } else if is_conn {
                if is_ref {
                    quote! {
                        let mut conn = ctx.async_connection().await.unwrap();
                        #function_name(&mut conn).await;
                    }
                } else {
                    quote! {
                        let conn = ctx.async_connection().await.unwrap();
                        #function_name(conn).await;
                    }
                }
            } else if is_ref {
                quote! {
                    #function_name(&mut ctx).await;
                }
            } else {
                quote! {
                    #function_name(ctx).await;
                }
            }
        } else {
            quote! { #function_name().await; }
        }
    }
}

fn generate_async_cluster_call(
    function_name: &syn::Ident,
    inputs: &syn::punctuated::Punctuated<syn::FnArg, syn::token::Comma>,
) -> proc_macro2::TokenStream {
    if inputs.is_empty() {
        quote! { #function_name().await; }
    } else {
        let arg = inputs.first().unwrap();
        if let syn::FnArg::Typed(pat_type) = arg {
            let is_ref = matches!(&*pat_type.ty, syn::Type::Reference(_));
            let is_conn = is_connection_type(&pat_type.ty);

            if is_conn {
                if is_ref {
                    quote! {
                        let mut conn = ctx.async_connection().await;
                        #function_name(&mut conn).await;
                    }
                } else {
                    quote! {
                        let conn = ctx.async_connection().await;
                        #function_name(conn).await;
                    }
                }
            } else if is_ref {
                quote! {
                    #function_name(&mut ctx).await;
                }
            } else {
                quote! {
                    #function_name(ctx).await;
                }
            }
        } else {
            quote! { #function_name().await; }
        }
    }
}

#[proc_macro_attribute]
pub fn single_server_test(attr: TokenStream, input: TokenStream) -> TokenStream {
    let mut item = parse_macro_input!(input as syn::ItemFn);
    let test_function_name = item.sig.ident.clone();
    let module_expr = parse_module_from_attr(&attr);

    item.sig.ident = syn::Ident::new(
        &format!("{test_function_name}_internal"),
        test_function_name.span(),
    );
    let function_name = item.sig.ident.clone();
    let ignore_flag = ignore_flag(&item);
    let call_expr = generate_sync_call(&function_name, &item.sig.inputs);

    let expanded = quote! {
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
    };

    expanded.into()
}

#[proc_macro_attribute]
pub fn async_single_server_test(attr: TokenStream, input: TokenStream) -> TokenStream {
    let mut item = parse_macro_input!(input as syn::ItemFn);
    let test_function_name = item.sig.ident.clone();
    let module_expr = parse_module_from_attr(&attr);

    item.sig.ident = syn::Ident::new(
        &format!("{test_function_name}_internal"),
        test_function_name.span(),
    );
    let function_name = item.sig.ident.clone();
    let ignore_flag = ignore_flag(&item);
    let call_expr = generate_async_call(&function_name, &item.sig.inputs);

    let expanded = quote! {
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
            #[cfg(feature = "tokio-rustls-comp")]
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
            #[cfg(feature = "smol-rustls-comp")]
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
            #[cfg(feature = "tokio-rustls-comp")]
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
            #[cfg(feature = "smol-rustls-comp")]
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
    };

    expanded.into()
}

#[proc_macro_attribute]
pub fn cluster_test(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let mut item = parse_macro_input!(input as syn::ItemFn);
    let test_function_name = item.sig.ident.clone();

    item.sig.ident = syn::Ident::new(
        &format!("{test_function_name}_internal"),
        test_function_name.span(),
    );
    let function_name = item.sig.ident.clone();
    let ignore_flag = ignore_flag(&item);
    let call_expr = generate_sync_call(&function_name, &item.sig.inputs);

    let expanded = quote! {
        mod #test_function_name {
            use super::*;

            #item

            #[test]
            #ignore_flag
            #[cfg(feature = "cluster")]
            fn resp2_tcp() {
                let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                    redis_test::cluster::RedisClusterConfiguration::default().insecure_tls(),
                    redis::ProtocolVersion::RESP2,
                );
                #call_expr
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "cluster", feature = "tls-rustls"))]
            fn resp2_tls() {
                let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                    redis_test::cluster::RedisClusterConfiguration::default()
                        .insecure_tls()
                        .cluster_type(redis_test::cluster::ClusterType::TcpTls),
                    redis::ProtocolVersion::RESP2,
                );
                #call_expr
            }

            #[test]
            #ignore_flag
            #[cfg(feature = "cluster")]
            fn resp3_tcp() {
                let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                    redis_test::cluster::RedisClusterConfiguration::default().insecure_tls(),
                    redis::ProtocolVersion::RESP3,
                );
                #call_expr
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "cluster", feature = "tls-rustls"))]
            fn resp3_tls() {
                let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                    redis_test::cluster::RedisClusterConfiguration::default()
                        .insecure_tls()
                        .cluster_type(redis_test::cluster::ClusterType::TcpTls),
                    redis::ProtocolVersion::RESP3,
                );
                #call_expr
            }
        }
    };

    expanded.into()
}

#[proc_macro_attribute]
pub fn async_cluster_test(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let mut item = parse_macro_input!(input as syn::ItemFn);
    let test_function_name = item.sig.ident.clone();

    item.sig.ident = syn::Ident::new(
        &format!("{test_function_name}_internal"),
        test_function_name.span(),
    );
    let function_name = item.sig.ident.clone();
    let ignore_flag = ignore_flag(&item);
    let call_expr = generate_async_cluster_call(&function_name, &item.sig.inputs);

    let expanded = quote! {
        mod #test_function_name {
            use super::*;

            #item

            #[test]
            #ignore_flag
            #[cfg(all(feature = "cluster-async", feature = "tokio-comp"))]
            fn resp2_tcp_tokio() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                        redis_test::cluster::RedisClusterConfiguration::default().insecure_tls(),
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
                    let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                        redis_test::cluster::RedisClusterConfiguration::default().insecure_tls(),
                        redis::ProtocolVersion::RESP2,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Smol);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "cluster-async", feature = "tokio-rustls-comp"))]
            fn resp2_tls_tokio() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                        redis_test::cluster::RedisClusterConfiguration::default()
                            .insecure_tls()
                            .cluster_type(redis_test::cluster::ClusterType::TcpTls),
                        redis::ProtocolVersion::RESP2,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Tokio);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "cluster-async", feature = "smol-rustls-comp"))]
            fn resp2_tls_smol() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                        redis_test::cluster::RedisClusterConfiguration::default()
                            .insecure_tls()
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
                    let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                        redis_test::cluster::RedisClusterConfiguration::default().insecure_tls(),
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
                    let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                        redis_test::cluster::RedisClusterConfiguration::default().insecure_tls(),
                        redis::ProtocolVersion::RESP3,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Smol);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "cluster-async", feature = "tokio-rustls-comp"))]
            fn resp3_tls_tokio() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                        redis_test::cluster::RedisClusterConfiguration::default()
                            .insecure_tls()
                            .cluster_type(redis_test::cluster::ClusterType::TcpTls),
                        redis::ProtocolVersion::RESP3,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Tokio);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "cluster-async", feature = "smol-rustls-comp"))]
            fn resp3_tls_smol() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestClusterContext::new_with_config_and_protocol(
                        redis_test::cluster::RedisClusterConfiguration::default()
                            .insecure_tls()
                            .cluster_type(redis_test::cluster::ClusterType::TcpTls),
                        redis::ProtocolVersion::RESP3,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Smol);
            }
        }
    };

    expanded.into()
}

#[proc_macro_attribute]
pub fn sentinel_test(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let mut item = parse_macro_input!(input as syn::ItemFn);
    let test_function_name = item.sig.ident.clone();

    item.sig.ident = syn::Ident::new(
        &format!("{test_function_name}_internal"),
        test_function_name.span(),
    );
    let function_name = item.sig.ident.clone();
    let ignore_flag = ignore_flag(&item);
    let call_expr = generate_sync_call(&function_name, &item.sig.inputs);

    let expanded = quote! {
        mod #test_function_name {
            use super::*;

            #item

            #[test]
            #ignore_flag
            #[cfg(feature = "sentinel")]
            fn resp2_tcp() {
                let mut ctx = crate::support::TestSentinelContext::new_with_server_type_and_protocol(
                    2,
                    1,
                    3,
                    redis_test::server::ServerType::Tcp { tls: false },
                    redis::ProtocolVersion::RESP2,
                );
                #call_expr
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "sentinel", feature = "tls-rustls"))]
            fn resp2_tls() {
                let mut ctx = crate::support::TestSentinelContext::new_with_server_type_and_protocol(
                    2,
                    1,
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
                    1,
                    3,
                    redis_test::server::ServerType::Tcp { tls: false },
                    redis::ProtocolVersion::RESP3,
                );
                #call_expr
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "sentinel", feature = "tls-rustls"))]
            fn resp3_tls() {
                let mut ctx = crate::support::TestSentinelContext::new_with_server_type_and_protocol(
                    2,
                    1,
                    3,
                    redis_test::server::ServerType::Tcp { tls: true },
                    redis::ProtocolVersion::RESP3,
                );
                #call_expr
            }
        }
    };

    expanded.into()
}

#[proc_macro_attribute]
pub fn async_sentinel_test(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let mut item = parse_macro_input!(input as syn::ItemFn);
    let test_function_name = item.sig.ident.clone();

    item.sig.ident = syn::Ident::new(
        &format!("{test_function_name}_internal"),
        test_function_name.span(),
    );
    let function_name = item.sig.ident.clone();
    let ignore_flag = ignore_flag(&item);
    let call_expr = generate_async_call(&function_name, &item.sig.inputs);

    let expanded = quote! {
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
                        1,
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
                        1,
                        3,
                        redis_test::server::ServerType::Tcp { tls: false },
                        redis::ProtocolVersion::RESP2,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Smol);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "sentinel", feature = "tokio-rustls-comp"))]
            fn resp2_tls_tokio() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestSentinelContext::new_with_server_type_and_protocol(
                        2,
                        1,
                        3,
                        redis_test::server::ServerType::Tcp { tls: true },
                        redis::ProtocolVersion::RESP2,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Tokio);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "sentinel", feature = "smol-rustls-comp"))]
            fn resp2_tls_smol() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestSentinelContext::new_with_server_type_and_protocol(
                        2,
                        1,
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
                        1,
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
                        1,
                        3,
                        redis_test::server::ServerType::Tcp { tls: false },
                        redis::ProtocolVersion::RESP3,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Smol);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "sentinel", feature = "tokio-rustls-comp"))]
            fn resp3_tls_tokio() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestSentinelContext::new_with_server_type_and_protocol(
                        2,
                        1,
                        3,
                        redis_test::server::ServerType::Tcp { tls: true },
                        redis::ProtocolVersion::RESP3,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Tokio);
            }

            #[test]
            #ignore_flag
            #[cfg(all(feature = "sentinel", feature = "smol-rustls-comp"))]
            fn resp3_tls_smol() {
                crate::support::block_on_all(async move {
                    let mut ctx = crate::support::TestSentinelContext::new_with_server_type_and_protocol(
                        2,
                        1,
                        3,
                        redis_test::server::ServerType::Tcp { tls: true },
                        redis::ProtocolVersion::RESP3,
                    );
                    #call_expr
                }, crate::support::RuntimeType::Smol);
            }
        }
    };

    expanded.into()
}

#[proc_macro_attribute]
pub fn async_test(attr: TokenStream, input: TokenStream) -> TokenStream {
    async_single_server_test(attr, input)
}
