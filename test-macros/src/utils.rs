use proc_macro2::TokenStream as TokenStream2;
use quote::quote;

pub(crate) fn has_ignore_attr(item: &syn::ItemFn) -> bool {
    item.attrs.iter().any(|a| a.path().is_ident("ignore"))
}

pub(crate) fn ignore_flag(item: &syn::ItemFn) -> proc_macro2::TokenStream {
    if has_ignore_attr(item) {
        quote! { #[ignore] }
    } else {
        quote! {}
    }
}

pub(crate) fn is_connection_type(ty: &syn::Type) -> bool {
    let type_str = quote!(#ty).to_string();
    type_str.contains("Connection")
        || type_str.contains("ConnectionLike")
        || type_str.contains("AsyncCommands")
        || type_str.contains("AsyncTypedCommands")
}

pub(crate) fn is_bool_type(ty: &syn::Type) -> bool {
    let type_str = quote!(#ty).to_string();
    type_str == "bool"
}

pub(crate) fn parse_module_from_attr(attr: &TokenStream2) -> proc_macro2::TokenStream {
    let attr_str = attr.to_string();
    if attr_str.contains("json") {
        quote! { .module(redis_test::server::Module::Json) }
    } else if attr_str.contains("bloom") {
        quote! { .module(redis_test::server::Module::Bloom) }
    } else {
        quote! {}
    }
}

pub(crate) struct ClusterTestArgs {
    pub config: proc_macro2::TokenStream,
    pub supported_versions: Option<proc_macro2::TokenStream>,
}

pub(crate) fn parse_cluster_test_args(attr: &TokenStream2) -> ClusterTestArgs {
    let mut config = None;
    let mut supported_versions = None;
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("config") {
            let value: syn::LitStr = meta.value()?.parse()?;
            config = Some(syn::parse_str::<TokenStream2>(&value.value())?);
            Ok(())
        } else if meta.path.is_ident("supported_versions") {
            let value: syn::LitStr = meta.value()?.parse()?;
            supported_versions = Some(syn::parse_str::<TokenStream2>(&value.value())?);
            Ok(())
        } else {
            Err(meta.error(
                "unsupported attribute; expected `config = \"...\"` or `supported_versions = \"...\"`",
            ))
        }
    });
    syn::parse::Parser::parse2(parser, attr.clone()).expect("invalid `#[cluster_test]` attribute");
    ClusterTestArgs {
        config: config.unwrap_or_else(|| {
            quote! { redis_test::cluster::RedisClusterConfiguration::default().insecure_tls() }
        }),
        supported_versions,
    }
}

/// Generates a pre-provisioning version check that starts a single, cheap standalone server and
/// skips the test (returning early) if it does not support the given component(s).
pub(crate) fn generate_version_check(
    supported_versions: &Option<proc_macro2::TokenStream>,
) -> proc_macro2::TokenStream {
    match supported_versions {
        Some(component) => quote! {
            {
                let version_check_ctx = crate::support::TestContextBuilder::new().build();
                if !crate::support::TestContextVersioning::supports(&version_check_ctx, #component) {
                    eprintln!(
                        "Skipping the test because the running server does not support {:?}.",
                        #component
                    );
                    return;
                }
            }
        },
        None => quote! {},
    }
}

pub(crate) fn generate_sync_call(
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

pub(crate) fn generate_async_call(
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

pub(crate) fn generate_async_cluster_call(
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

#[cfg(test)]
mod tests {
    use super::*;
    use quote::ToTokens;

    fn parse_fn(input: &str) -> syn::ItemFn {
        syn::parse_str(input).expect("failed to parse function")
    }

    fn to_string(tokens: &proc_macro2::TokenStream) -> String {
        tokens.to_token_stream().to_string().replace(' ', "")
    }

    fn inputs_of(item: &syn::ItemFn) -> syn::punctuated::Punctuated<syn::FnArg, syn::token::Comma> {
        item.sig.inputs.clone()
    }

    fn attr_stream(input: &str) -> TokenStream2 {
        input.parse().expect("failed to parse attr")
    }

    fn call_contains(tokens: &proc_macro2::TokenStream, needle: &str) -> bool {
        to_string(tokens).contains(&needle.replace(' ', ""))
    }

    #[test]
    fn module_from_attr_json() {
        assert_eq!(
            to_string(&parse_module_from_attr(&attr_stream("json"))),
            ".module(redis_test::server::Module::Json)"
        );
    }

    #[test]
    fn module_from_attr_bloom() {
        assert_eq!(
            to_string(&parse_module_from_attr(&attr_stream("bloom"))),
            ".module(redis_test::server::Module::Bloom)"
        );
    }

    #[test]
    fn module_from_attr_none() {
        assert_eq!(to_string(&parse_module_from_attr(&attr_stream(""))), "");
    }

    #[test]
    fn sync_call_empty_inputs() {
        let item = parse_fn("fn test() {}");
        let call = generate_sync_call(&item.sig.ident, &inputs_of(&item));
        assert_eq!(to_string(&call), "test();");
    }

    #[test]
    fn sync_call_bool_inputs() {
        let item = parse_fn("fn test(flag: bool) {}");
        let call = generate_sync_call(&item.sig.ident, &inputs_of(&item));
        assert!(call_contains(&call, "test(true);"));
        assert!(call_contains(&call, "test(false);"));
    }

    #[test]
    fn sync_call_ctx_ref() {
        let item = parse_fn("fn test(ctx: &mut TestContext) {}");
        let call = generate_sync_call(&item.sig.ident, &inputs_of(&item));
        assert!(call_contains(&call, "test(&mutctx);"));
    }

    #[test]
    fn sync_call_ctx_by_value() {
        let item = parse_fn("fn test(ctx: TestContext) {}");
        let call = generate_sync_call(&item.sig.ident, &inputs_of(&item));
        assert!(call_contains(&call, "test(ctx);"));
    }

    #[test]
    fn sync_call_connection_ref() {
        let item = parse_fn("fn test(conn: &mut Connection) {}");
        let call = generate_sync_call(&item.sig.ident, &inputs_of(&item));
        assert!(call_contains(&call, "letmutconn=ctx.connection();"));
        assert!(call_contains(&call, "test(&mutconn);"));
    }

    #[test]
    fn sync_call_connection_by_value() {
        let item = parse_fn("fn test(conn: Connection) {}");
        let call = generate_sync_call(&item.sig.ident, &inputs_of(&item));
        assert!(call_contains(&call, "letconn=ctx.connection();"));
        assert!(call_contains(&call, "test(conn);"));
    }

    #[test]
    fn async_call_empty_inputs() {
        let item = parse_fn("fn test() {}");
        let call = generate_async_call(&item.sig.ident, &inputs_of(&item));
        assert_eq!(to_string(&call), "test().await;");
    }

    #[test]
    fn async_call_bool_inputs() {
        let item = parse_fn("fn test(flag: bool) {}");
        let call = generate_async_call(&item.sig.ident, &inputs_of(&item));
        assert!(call_contains(&call, "test(true).await;"));
        assert!(call_contains(&call, "test(false).await;"));
    }

    #[test]
    fn async_call_ctx_ref() {
        let item = parse_fn("fn test(ctx: &mut TestContext) {}");
        let call = generate_async_call(&item.sig.ident, &inputs_of(&item));
        assert!(call_contains(&call, "test(&mutctx).await;"));
    }

    #[test]
    fn async_call_ctx_by_value() {
        let item = parse_fn("fn test(ctx: TestContext) {}");
        let call = generate_async_call(&item.sig.ident, &inputs_of(&item));
        assert!(call_contains(&call, "test(ctx).await;"));
    }

    #[test]
    fn async_call_connection_ref() {
        let item = parse_fn("fn test(conn: &mut Connection) {}");
        let call = generate_async_call(&item.sig.ident, &inputs_of(&item));
        assert!(call_contains(
            &call,
            "letmutconn=ctx.async_connection().await.unwrap();"
        ));
        assert!(call_contains(&call, "test(&mutconn).await;"));
    }

    #[test]
    fn async_call_connection_by_value() {
        let item = parse_fn("fn test(conn: Connection) {}");
        let call = generate_async_call(&item.sig.ident, &inputs_of(&item));
        assert!(call_contains(
            &call,
            "letconn=ctx.async_connection().await.unwrap();"
        ));
        assert!(call_contains(&call, "test(conn).await;"));
    }

    #[test]
    fn async_cluster_call_empty_inputs() {
        let item = parse_fn("fn test() {}");
        let call = generate_async_cluster_call(&item.sig.ident, &inputs_of(&item));
        assert_eq!(to_string(&call), "test().await;");
    }

    #[test]
    fn async_cluster_call_connection_ref() {
        let item = parse_fn("fn test(conn: &mut Connection) {}");
        let call = generate_async_cluster_call(&item.sig.ident, &inputs_of(&item));
        assert!(call_contains(
            &call,
            "letmutconn=ctx.async_connection().await;"
        ));
        assert!(call_contains(&call, "test(&mutconn).await;"));
    }

    #[test]
    fn async_cluster_call_connection_by_value() {
        let item = parse_fn("fn test(conn: Connection) {}");
        let call = generate_async_cluster_call(&item.sig.ident, &inputs_of(&item));
        assert!(call_contains(
            &call,
            "letconn=ctx.async_connection().await;"
        ));
        assert!(call_contains(&call, "test(conn).await;"));
    }

    #[test]
    fn async_cluster_call_ctx_ref() {
        let item = parse_fn("fn test(ctx: &mut TestContext) {}");
        let call = generate_async_cluster_call(&item.sig.ident, &inputs_of(&item));
        assert!(call_contains(&call, "test(&mutctx).await;"));
    }

    #[test]
    fn cluster_args_default_config() {
        let args = parse_cluster_test_args(&attr_stream(""));
        assert!(call_contains(
            &args.config,
            "redis_test::cluster::RedisClusterConfiguration::default().insecure_tls()"
        ));
        assert!(args.supported_versions.is_none());
    }

    #[test]
    fn cluster_args_config_only() {
        let args = parse_cluster_test_args(&attr_stream("config = \"foo().bar()\""));
        assert!(call_contains(&args.config, "foo().bar()"));
        assert!(args.supported_versions.is_none());
    }

    #[test]
    fn cluster_args_supported_versions_only() {
        let args = parse_cluster_test_args(&attr_stream("supported_versions = \"Json\""));
        assert!(args.supported_versions.is_some());
        assert!(call_contains(
            args.supported_versions.as_ref().unwrap(),
            "Json"
        ));
    }

    #[test]
    fn cluster_args_both() {
        let args = parse_cluster_test_args(&attr_stream(
            "config = \"foo()\", supported_versions = \"[Bloom]\"",
        ));
        assert!(call_contains(&args.config, "foo()"));
        assert!(call_contains(
            args.supported_versions.as_ref().unwrap(),
            "[Bloom]"
        ));
    }

    #[test]
    fn version_check_none() {
        let vc = generate_version_check(&None);
        assert_eq!(to_string(&vc), "");
    }

    #[test]
    fn version_check_some() {
        let vc = generate_version_check(&Some(quote! { Json }));
        let s = to_string(&vc);
        assert!(s.contains("TestContextBuilder::new().build()"));
        assert!(s.contains("TestContextVersioning::supports"));
        assert!(s.contains("return;"));
    }

    #[test]
    fn ignore_flag_present() {
        let item = parse_fn("#[ignore]\nfn test(ctx: &mut TestContext) {}");
        assert_eq!(to_string(&ignore_flag(&item)), "#[ignore]");
        assert!(has_ignore_attr(&item));
    }

    #[test]
    fn ignore_flag_absent() {
        let item = parse_fn("fn test(ctx: &mut TestContext) {}");
        assert_eq!(to_string(&ignore_flag(&item)), "");
        assert!(!has_ignore_attr(&item));
    }

    #[test]
    fn connection_type_detection() {
        let check = |ty: &str| {
            let item: syn::ItemFn = syn::parse_str(&format!("fn test(x: {ty}) {{}}")).unwrap();
            matches!(
                inputs_of(&item).first(),
                Some(syn::FnArg::Typed(pt))
                    if is_connection_type(&pt.ty)
            )
        };
        assert!(check("Connection"));
        assert!(check("ConnectionLike"));
        assert!(check("AsyncCommands"));
        assert!(check("AsyncTypedCommands"));
        assert!(!check("&mut TestContext"));
        assert!(!check("bool"));
    }

    #[test]
    fn bool_type_detection() {
        let item = parse_fn("fn test(flag: bool) {}");
        if let Some(syn::FnArg::Typed(pt)) = inputs_of(&item).first() {
            assert!(is_bool_type(&pt.ty));
        } else {
            panic!("expected typed arg");
        }
    }
}
