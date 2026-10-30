use proc_macro::TokenStream;

mod cluster;
mod sentinel;
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

#[proc_macro_attribute]
pub fn cluster_test(attr: TokenStream, input: TokenStream) -> TokenStream {
    cluster::expand_cluster_test(attr.into(), input.into()).into()
}

#[proc_macro_attribute]
pub fn async_cluster_test(attr: TokenStream, input: TokenStream) -> TokenStream {
    cluster::expand_async_cluster_test(attr.into(), input.into()).into()
}

#[proc_macro_attribute]
pub fn sentinel_test(attr: TokenStream, input: TokenStream) -> TokenStream {
    sentinel::expand_sentinel_test(attr.into(), input.into()).into()
}

#[proc_macro_attribute]
pub fn async_sentinel_test(attr: TokenStream, input: TokenStream) -> TokenStream {
    sentinel::expand_async_sentinel_test(attr.into(), input.into()).into()
}
