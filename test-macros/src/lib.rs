use proc_macro::TokenStream;
use quote::quote;

#[proc_macro_attribute]
pub fn async_test(_: TokenStream, input: TokenStream) -> TokenStream {
    let item = syn::parse_macro_input!(input as syn::ItemFn);
    let function_name = item.sig.ident.clone();
    let test_function_name =
        syn::Ident::new(&format!("test_{}", function_name), function_name.span());

    let final_code = quote! {
        #item

        #[rstest]
        #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
        #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
        fn #test_function_name (#[case]runtime: RuntimeType) {
            block_on_all(#function_name (), runtime).unwrap();
        }
    };

    final_code.into()
}
