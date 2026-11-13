use proc_macro::TokenStream;

mod single;
mod test_env;
mod utils;

#[proc_macro_attribute]
pub fn single_server_test(attr: TokenStream, input: TokenStream) -> TokenStream {
    single::expand_single_server_test(attr.into(), input.into()).into()
}

#[proc_macro_attribute]
pub fn async_single_server_test(attr: TokenStream, input: TokenStream) -> TokenStream {
    single::expand_async_single_server_test(attr.into(), input.into()).into()
}

#[proc_macro_attribute]
pub fn async_test(attr: TokenStream, input: TokenStream) -> TokenStream {
    single::expand_async_single_server_test(attr.into(), input.into()).into()
}
