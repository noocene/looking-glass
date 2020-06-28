use crate::util::generate_generics;
use highway::{HighwayBuilder, HighwayHash};
use proc_macro2::TokenStream;
use quote::quote;
use std::env::var;
use syn::{
    braced,
    parse::{Parse, ParseStream, Result},
    parse_macro_input, parse_quote,
    punctuated::Punctuated,
    token, Generics, Token, TypeParam,
};

#[derive(Clone, Debug)]
struct PrimitiveType {
    params: Punctuated<TypeParam, Token![,]>,
    delimiter: Token![|],
    data: TokenStream,
    braced: token::Brace,
}

struct PrimitiveTypes {
    types: Punctuated<PrimitiveType, Token![,]>,
}

impl Parse for PrimitiveType {
    fn parse(input: ParseStream) -> Result<Self> {
        let content;
        Ok(PrimitiveType {
            params: Punctuated::parse_separated_nonempty(input).unwrap_or(Punctuated::new()),
            delimiter: input.parse()?,
            braced: braced!(content in input),
            data: content.parse()?,
        })
    }
}

impl Parse for PrimitiveTypes {
    fn parse(input: ParseStream) -> Result<Self> {
        Ok(PrimitiveTypes {
            types: Punctuated::parse_terminated(input)?,
        })
    }
}

pub fn type_primitives(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let item = parse_macro_input!(item as PrimitiveTypes);

    let mut stream = quote!();

    for mut item in item.types {
        let mut data = format!("{:?}", item.clone()).into_bytes();

        if let Ok(ver) = var("CARGO_PKG_VERSION") {
            data.extend(ver.as_bytes());
        }

        data.extend("looking_glass PRIMITIVE DEFINITION".as_bytes());

        let hash = HighwayBuilder::default().hash256(data.as_slice());

        for param in &mut item.params {
            param.bounds.push(parse_quote!(looking_glass::Typed));
        }

        let params = &item.params;
        let ty = &item.data;

        let g_params = quote!(<#params>);
        let generics: Generics = parse_quote!(#g_params);

        let generate_hash = generate_generics(hash, &generics, vec![]);

        stream.extend(quote! {
            const _: () = {
                impl<#params> looking_glass::Typed for #ty {
                    fn ty() -> looking_glass::Id {
                        use looking_glass::highway::{HighwayBuilder, HighwayHash};

                        looking_glass::Id::hidden_new_do_not_call_manually(#generate_hash)
                    }
                }
            };
        });
    }

    stream.into()
}
