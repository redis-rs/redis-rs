use proc_macro::TokenStream;
use quote::quote;

#[proc_macro_attribute]
pub fn async_test(_: TokenStream, input: TokenStream) -> TokenStream {
    let item = syn::parse_macro_input!(input as syn::ItemFn);
    let function_name = item.sig.ident.clone();
    let test_function_name =
        syn::Ident::new(&format!("test_{}", function_name), function_name.span());
    let final_code = if item.sig.inputs.len() == 1 {
        if let Some(syn::FnArg::Typed(pat_type)) = item.sig.inputs.last() {
            // Verify the argument is a boolean
            let is_bool = matches!(&*pat_type.ty, syn::Type::Path(type_path) 
                if type_path.path.is_ident("bool"));

            if !is_bool {
                return syn::Error::new_spanned(&pat_type.ty, "Argument must be of type bool")
                    .to_compile_error()
                    .into();
            }
        } else {
            return syn::Error::new_spanned(&item.sig.inputs, "Expected typed argument")
                .to_compile_error()
                .into();
        }
        quote! {
            #item

            #[rstest]
            #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
            #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
            fn #test_function_name (#[case]runtime: RuntimeType, #[values(true, false)] arg: bool) {
                block_on_all(#function_name (arg), runtime).unwrap();
            }
        }
    } else if item.sig.inputs.is_empty() {
        quote! {
            #item

            #[rstest]
            #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
            #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
            fn #test_function_name (#[case]runtime: RuntimeType) {
                block_on_all(#function_name (), runtime).unwrap();
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
