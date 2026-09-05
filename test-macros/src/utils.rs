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

fn module_expr_for_name(name: &str) -> proc_macro2::TokenStream {
    match name {
        "json" => quote! { .module(redis_test::server::Module::Json) },
        "bloom" => quote! { .module(redis_test::server::Module::Bloom) },
        other => {
            let msg = syn::LitStr::new(
                &format!("unsupported module name: \"{other}\""),
                proc_macro2::Span::call_site(),
            );
            quote! { compile_error!(#msg) }
        }
    }
}

pub(crate) fn parse_module_from_attr(attr: &TokenStream2) -> proc_macro2::TokenStream {
    use syn::Meta;
    use syn::parse::Parser;

    let module_exprs: Vec<proc_macro2::TokenStream> =
        syn::punctuated::Punctuated::<Meta, syn::token::Comma>::parse_terminated
            .parse2(attr.clone())
            .unwrap_or_default()
            .into_iter()
            .filter_map(|meta| match meta {
                Meta::NameValue(nv) if nv.path.is_ident("module") => match nv.value {
                    syn::Expr::Lit(lit) => match lit.lit {
                        syn::Lit::Str(s) => Some(module_expr_for_name(&s.value())),
                        _ => None,
                    },
                    _ => None,
                },
                Meta::Path(path) => {
                    if path.is_ident("json") {
                        Some(quote! { .module(redis_test::server::Module::Json) })
                    } else if path.is_ident("bloom") {
                        Some(quote! { .module(redis_test::server::Module::Bloom) })
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect();

    if module_exprs.is_empty() {
        quote! {}
    } else {
        quote! { #(#module_exprs)* }
    }
}

/// Parses `mtls = true` from the attribute, returning `quote! { .mtls(true) }` if present.
pub(crate) fn parse_mtls_from_attr(attr: &TokenStream2) -> proc_macro2::TokenStream {
    use syn::Meta;
    use syn::parse::Parser;

    let has_mtls = syn::punctuated::Punctuated::<Meta, syn::token::Comma>::parse_terminated
        .parse2(attr.clone())
        .unwrap_or_default()
        .into_iter()
        .any(|meta| matches!(meta, Meta::NameValue(nv) if nv.path.is_ident("mtls")));

    if has_mtls {
        quote! { .mtls(true) }
    } else {
        quote! {}
    }
}

pub(crate) struct ClusterTestArgs {
    pub config: proc_macro2::TokenStream,
    pub supported_versions: Option<proc_macro2::TokenStream>,
    pub database_id: Option<proc_macro2::TokenStream>,
}

pub(crate) fn parse_cluster_test_args(attr: &TokenStream2) -> ClusterTestArgs {
    let mut config = None;
    let mut supported_versions = None;
    let mut database_id = None;
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("config") {
            let value: syn::LitStr = meta.value()?.parse()?;
            config = Some(syn::parse_str::<TokenStream2>(&value.value())?);
            Ok(())
        } else if meta.path.is_ident("supported_versions") {
            let value: syn::LitStr = meta.value()?.parse()?;
            supported_versions = Some(syn::parse_str::<TokenStream2>(&value.value())?);
            Ok(())
        } else if meta.path.is_ident("database_id") {
            let value: syn::LitInt = meta.value()?.parse()?;
            database_id = Some(syn::parse_str::<TokenStream2>(value.base10_digits())?);
            Ok(())
        } else {
            Err(meta.error(
                "unsupported attribute; expected `config = \"...\"`, `supported_versions = \"...\"`, or `database_id = N`",
            ))
        }
    });
    syn::parse::Parser::parse2(parser, attr.clone()).expect("invalid cluster test attribute");
    ClusterTestArgs {
        config: config.unwrap_or_else(|| {
            quote! { redis_test::cluster::RedisClusterConfiguration::default().insecure_tls() }
        }),
        supported_versions,
        database_id,
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

    fn parse_fn(input: &str) -> syn::ItemFn {
        syn::parse_str(input).expect("failed to parse function")
    }

    fn to_string(tokens: &proc_macro2::TokenStream) -> String {
        tokens.to_string()
    }

    /// Asserts `actual` equals `expected_src` token-by-token (a full-output check, not a
    /// substring match). The expected is given as readable source and parsed first, so whitespace
    /// between tokens is irrelevant while string literal contents are preserved.
    fn assert_full(actual: &proc_macro2::TokenStream, expected_src: &str) {
        let expected: proc_macro2::TokenStream = expected_src
            .parse()
            .expect("failed to parse expected expansion");
        assert_eq!(to_string(actual), to_string(&expected));
    }

    fn inputs_of(item: &syn::ItemFn) -> syn::punctuated::Punctuated<syn::FnArg, syn::token::Comma> {
        item.sig.inputs.clone()
    }

    fn attr_stream(input: &str) -> TokenStream2 {
        input.parse().expect("failed to parse attr")
    }

    /// Calls a generator with the given input function and asserts the full output.
    fn assert_call(
        generator: fn(
            &syn::Ident,
            &syn::punctuated::Punctuated<syn::FnArg, syn::token::Comma>,
        ) -> proc_macro2::TokenStream,
        fn_src: &str,
        expected: &str,
    ) {
        let item = parse_fn(fn_src);
        let call = generator(&item.sig.ident, &inputs_of(&item));
        assert_full(&call, expected);
    }

    #[test]
    fn module_from_attr_json() {
        assert_full(
            &parse_module_from_attr(&attr_stream("json")),
            ".module(redis_test::server::Module::Json)",
        );
    }

    #[test]
    fn module_from_attr_bloom() {
        assert_full(
            &parse_module_from_attr(&attr_stream("bloom")),
            ".module(redis_test::server::Module::Bloom)",
        );
    }

    #[test]
    fn module_from_attr_none() {
        assert_full(&parse_module_from_attr(&attr_stream("")), "");
    }

    #[test]
    fn module_from_attr_named_json() {
        assert_full(
            &parse_module_from_attr(&attr_stream(r#"module = "json""#)),
            ".module(redis_test::server::Module::Json)",
        );
    }

    #[test]
    fn module_from_attr_named_bloom() {
        assert_full(
            &parse_module_from_attr(&attr_stream(r#"module = "bloom""#)),
            ".module(redis_test::server::Module::Bloom)",
        );
    }

    #[test]
    fn mtls_from_attr_true() {
        assert_full(
            &parse_mtls_from_attr(&attr_stream("mtls = true")),
            ".mtls(true)",
        );
    }

    #[test]
    fn mtls_from_attr_absent() {
        assert_full(&parse_mtls_from_attr(&attr_stream("")), "");
    }

    #[test]
    fn mtls_from_attr_with_json() {
        assert_full(
            &parse_mtls_from_attr(&attr_stream(r#"mtls = true, module = "json""#)),
            ".mtls(true)",
        );
    }

    #[rstest::rstest]
    #[case::empty("fn test() {}", "test();")]
    #[case::bool("fn test(flag: bool) {}", "test(true); test(false);")]
    #[case::ctx_ref("fn test(ctx: &mut TestContext) {}", "test(&mut ctx);")]
    #[case::ctx_value("fn test(ctx: TestContext) {}", "test(ctx);")]
    #[case::conn_ref(
        "fn test(conn: &mut Connection) {}",
        "let mut conn = ctx.connection(); test(&mut conn);"
    )]
    #[case::conn_value(
        "fn test(conn: Connection) {}",
        "let conn = ctx.connection(); test(conn);"
    )]
    fn sync_call(#[case] fn_src: &str, #[case] expected: &str) {
        assert_call(generate_sync_call, fn_src, expected);
    }

    #[rstest::rstest]
    #[case::empty("fn test() {}", "test().await;")]
    #[case::bool("fn test(flag: bool) {}", "test(true).await; test(false).await;")]
    #[case::ctx_ref("fn test(ctx: &mut TestContext) {}", "test(&mut ctx).await;")]
    #[case::ctx_value("fn test(ctx: TestContext) {}", "test(ctx).await;")]
    #[case::conn_ref(
        "fn test(conn: &mut Connection) {}",
        "let mut conn = ctx.async_connection().await.unwrap(); test(&mut conn).await;"
    )]
    #[case::conn_value(
        "fn test(conn: Connection) {}",
        "let conn = ctx.async_connection().await.unwrap(); test(conn).await;"
    )]
    fn async_call(#[case] fn_src: &str, #[case] expected: &str) {
        assert_call(generate_async_call, fn_src, expected);
    }

    #[rstest::rstest]
    #[case::empty("fn test() {}", "test().await;")]
    #[case::ctx_ref("fn test(ctx: &mut TestContext) {}", "test(&mut ctx).await;")]
    #[case::conn_ref(
        "fn test(conn: &mut Connection) {}",
        "let mut conn = ctx.async_connection().await; test(&mut conn).await;"
    )]
    #[case::conn_value(
        "fn test(conn: Connection) {}",
        "let conn = ctx.async_connection().await; test(conn).await;"
    )]
    fn async_cluster_call(#[case] fn_src: &str, #[case] expected: &str) {
        assert_call(generate_async_cluster_call, fn_src, expected);
    }

    #[test]
    fn cluster_args_default_config() {
        let args = parse_cluster_test_args(&attr_stream(""));
        assert_full(
            &args.config,
            "redis_test::cluster::RedisClusterConfiguration::default().insecure_tls()",
        );
        assert!(args.supported_versions.is_none());
    }

    #[test]
    fn cluster_args_config_only() {
        let args = parse_cluster_test_args(&attr_stream("config = \"foo().bar()\""));
        assert_full(&args.config, "foo().bar()");
        assert!(args.supported_versions.is_none());
    }

    #[test]
    fn cluster_args_supported_versions_only() {
        let args = parse_cluster_test_args(&attr_stream("supported_versions = \"Json\""));
        assert!(args.supported_versions.is_some());
        assert_full(args.supported_versions.as_ref().unwrap(), "Json");
    }

    #[test]
    fn cluster_args_both() {
        let args = parse_cluster_test_args(&attr_stream(
            "config = \"foo()\", supported_versions = \"[Bloom]\"",
        ));
        assert_full(&args.config, "foo()");
        assert_full(args.supported_versions.as_ref().unwrap(), "[Bloom]");
    }

    #[test]
    fn cluster_args_database_id_only() {
        let args = parse_cluster_test_args(&attr_stream("database_id = 4"));
        assert!(args.database_id.is_some());
        assert_full(args.database_id.as_ref().unwrap(), "4");
    }

    #[test]
    fn cluster_args_database_id_with_config() {
        let args = parse_cluster_test_args(&attr_stream(r#"config = "foo()", database_id = 7"#));
        assert_full(&args.config, "foo()");
        assert!(args.database_id.is_some());
        assert_full(args.database_id.as_ref().unwrap(), "7");
    }

    #[test]
    fn cluster_args_all_three() {
        let args = parse_cluster_test_args(&attr_stream(
            r#"config = "foo()", supported_versions = "Json", database_id = 2"#,
        ));
        assert_full(&args.config, "foo()");
        assert!(args.supported_versions.is_some());
        assert_full(args.supported_versions.as_ref().unwrap(), "Json");
        assert!(args.database_id.is_some());
        assert_full(args.database_id.as_ref().unwrap(), "2");
    }

    #[test]
    fn version_check_none() {
        let vc = generate_version_check(&None);
        assert_full(&vc, "");
    }

    #[test]
    fn version_check_some() {
        let vc = generate_version_check(&Some(quote! { Json }));
        assert_full(
            &vc,
            r#"{ let version_check_ctx = crate::support::TestContextBuilder::new().build();
                if !crate::support::TestContextVersioning::supports(&version_check_ctx, Json) {
                    eprintln!("Skipping the test because the running server does not support {:?}.", Json);
                    return;
                }
            }"#,
        );
    }

    #[test]
    fn ignore_flag_present() {
        let item = parse_fn("#[ignore]\nfn test(ctx: &mut TestContext) {}");
        assert_full(&ignore_flag(&item), "#[ignore]");
        assert!(has_ignore_attr(&item));
    }

    #[test]
    fn ignore_flag_absent() {
        let item = parse_fn("fn test(ctx: &mut TestContext) {}");
        assert_full(&ignore_flag(&item), "");
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
