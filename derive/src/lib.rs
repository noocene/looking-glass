use proc_macro_hack::proc_macro_hack;

mod match_any;
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

#[proc_macro_hack]
pub fn match_any(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    match_any::match_any(item)
}
