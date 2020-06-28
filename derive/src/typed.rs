use crate::util::{generate_structure, generate_trait};
use highway::{HighwayBuilder, HighwayHash};
use proc_macro2::TokenStream;
use quote::quote;
use std::env::var;
use syn::{parse_macro_input, Data, DataEnum, DataStruct, DeriveInput, Item};
use synstructure::Structure;

pub fn typed(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let i: TokenStream = item.clone().into();
    let item = parse_macro_input!(item as Item);

    let mut data = i.clone().to_string().into_bytes();

    if let Ok(ver) = var("CARGO_PKG_VERSION") {
        data.extend(ver.as_bytes());
    }

    if let Ok(name) = var("CARGO_PKG_NAME") {
        data.extend(name.as_bytes());
    }

    let hash = HighwayBuilder::default().hash256(data.as_slice());

    match item {
        Item::Struct(data) => {
            let input = DeriveInput {
                attrs: data.attrs,
                generics: data.generics,
                ident: data.ident,
                vis: data.vis,
                data: Data::Struct(DataStruct {
                    fields: data.fields,
                    struct_token: data.struct_token,
                    semi_token: data.semi_token,
                }),
            };
            let structure = Structure::new(&input);
            let mut gen = generate_structure(structure, hash);
            gen.extend(i);
            gen
        }
        Item::Enum(data) => {
            let input = DeriveInput {
                attrs: data.attrs,
                generics: data.generics,
                ident: data.ident,
                vis: data.vis,
                data: Data::Enum(DataEnum {
                    enum_token: data.enum_token,
                    variants: data.variants,
                    brace_token: data.brace_token,
                }),
            };
            let structure = Structure::new(&input);
            let mut gen = generate_structure(structure, hash);
            gen.extend(i);
            gen
        }
        Item::Trait(item_trait) => {
            let mut gen = generate_trait(item_trait, hash);
            gen.extend(i);
            gen
        }
        _ => quote! {
            const _: () = {
                compile_error!("only enums, structs, and traits can be typed")
            };
        },
    }
    .into()
}
