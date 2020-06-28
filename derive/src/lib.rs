mod type_primitives;
mod typed;
mod util;

#[proc_macro]
pub fn type_primitives(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    type_primitives::type_primitives(item)
}

#[proc_macro_attribute]
pub fn typed(_: proc_macro::TokenStream, item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    typed::typed(item)
}
