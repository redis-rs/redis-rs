use proc_macro::TokenStream;
use quote::quote;

#[proc_macro_attribute]
pub fn async_test(_: TokenStream, input: TokenStream) -> TokenStream {
    let item = syn::parse_macro_input!(input as syn::ItemFn);

    let function_name = item.sig.ident.clone();
    let test_function_name =
        syn::Ident::new(&format!("test_{}", function_name), function_name.span());

    let final_code = if item.sig.inputs.len() == 1 {
        let Some(syn::FnArg::Typed(pat_type)) = item.sig.inputs.last() else {
            return syn::Error::new_spanned(&item.sig.inputs, "Expected typed argument")
                .to_compile_error()
                .into();
        };
        // Verify the argument is a boolean
        let is_bool = matches!(&*pat_type.ty, syn::Type::Path(type_path) 
                if type_path.path.is_ident("bool"));
        let is_connection = matches!(&*pat_type.ty, syn::Type::ImplTrait(impl_trait)
        if impl_trait.bounds.iter().any(|bound| {
            matches!(bound, syn::TypeParamBound::Trait(trait_bound)
                if trait_bound.path.segments.last().is_some_and(|seg| seg.ident == "ConnectionLike" || seg.ident == "AsyncTypedCommands" || seg.ident == "AsyncCommands"))
        }));

        if is_connection {
            let test_multiplexed_connection_function_name = syn::Ident::new(
                &format!("test_multiplexed_connection_{}", function_name),
                function_name.span(),
            );
            let test_connection_manager_function_name = syn::Ident::new(
                &format!("test_connection_manager_{}", function_name),
                function_name.span(),
            );

            quote! {
                #item

                #[rstest]
                #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
                #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
                fn #test_multiplexed_connection_function_name (#[case]runtime: RuntimeType) {
                    let ctx = TestContext::new();
                    block_on_all(async move {
                        let conn = ctx.async_connection().await.unwrap();
                        #function_name (conn).await
                    }, runtime);
                }

                #[rstest]
                #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
                #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
                #[cfg(feature = "connection-manager")]
                fn #test_connection_manager_function_name (#[case]runtime: RuntimeType) {
                    let ctx = TestContext::new();
                    block_on_all(async move {
                        let conn = ctx.client.get_connection_manager().await.unwrap();
                        #function_name (conn).await
                    }, runtime);
                }
            }
        } else if is_bool {
            quote! {
                #item

                #[rstest]
                #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
                #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
                fn #test_function_name (#[case]runtime: RuntimeType, #[values(true, false)] arg: bool) {
                    block_on_all(#function_name (arg), runtime);
                }
            }
        } else {
            return syn::Error::new_spanned(
                &pat_type.ty,
                format!("Unsupported argument type. arg pattern is: {pat_type:?}"),
            )
            .to_compile_error()
            .into();
        }
    } else if item.sig.inputs.is_empty() {
        quote! {
            #item

            #[rstest]
            #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
            #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
            fn #test_function_name (#[case]runtime: RuntimeType) {
                block_on_all(#function_name (), runtime);
            }
        }
    } else {
        return syn::Error::new_spanned(
            &item.sig.inputs,
            "Unsupported number of arguments in function",
        )
        .to_compile_error()
        .into();
    };

    final_code.into()
}
