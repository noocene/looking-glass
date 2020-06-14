use highway::{HighwayBuilder, HighwayHash};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use std::env::var;
use syn::{
    braced,
    parse::{Parse, ParseStream, Result},
    parse_macro_input, parse_quote,
    punctuated::Punctuated,
    token, Data, DataEnum, DataStruct, DeriveInput, GenericParam, Generics, Item, ItemTrait, Token,
    TraitItem, TraitItemType, TypeParam,
};
use synstructure::{AddBounds, Structure};

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

#[proc_macro]
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

#[proc_macro_attribute]
pub fn typed(_: proc_macro::TokenStream, item: proc_macro::TokenStream) -> proc_macro::TokenStream {
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

fn generate_generics(hash: [u64; 4], generics: &Generics, tys: Vec<TraitItemType>) -> TokenStream {
    let base = {
        let _0 = hash[0];
        let _1 = hash[1];
        let _2 = hash[2];
        let _3 = hash[3];

        quote! {
            [#_0, #_1, #_2, #_3]
        }
    };

    if generics.params.len() == 0 && tys.len() == 0 {
        base
    } else {
        let mut stream = quote! {
            {
                let base: [u64; 4] = #base;
                for item in &base {
                    hasher.append(item.to_ne_bytes().as_ref());
                }
            }
        };

        for param in &generics.params {
            match param {
                GenericParam::Lifetime(_) => {
                    return quote! {
                        const _: () = {
                            compile_error!("cannot type structure with lifetime parameters")
                        };
                    };
                }
                GenericParam::Type(ty) => {
                    let ident = &ty.ident;

                    stream.extend(quote! {
                        for item in <#ident as looking_glass::Typed>::ty().hidden_extract_do_not_call_manually().as_ref() {
                            hasher.append(item.to_le_bytes().as_ref());
                        }
                    });
                }
                _ => {}
            }
        }

        for ty in tys {
            let ident = format_ident!("__DERIVE_ASSOC_{}", ty.ident);

            stream.extend(quote! {
                for item in <#ident as looking_glass::Typed>::ty().hidden_extract_do_not_call_manually().as_ref() {
                    hasher.append(item.to_le_bytes().as_ref());
                }
            });
        }

        quote! {{
            let mut hasher = HighwayBuilder::default();

            #stream

            hasher.finalize256()
        }}
    }
}

fn generate_structure(mut structure: Structure, hash: [u64; 4]) -> TokenStream {
    let generate_hash = generate_generics(hash, &structure.ast().generics, vec![]);

    structure.add_bounds(AddBounds::Generics);

    structure.gen_impl(quote! {
        extern crate looking_glass;

        gen impl looking_glass::Typed for @Self {
            fn ty() -> looking_glass::Id {
                use looking_glass::highway::{HighwayBuilder, HighwayHash};

                looking_glass::Id::hidden_new_do_not_call_manually(#generate_hash)
            }
        }
    })
}

fn generate_trait(item: ItemTrait, hash: [u64; 4]) -> TokenStream {
    let const_ident = format_ident!("__DERIVE_TYPED_FOR_{}_IMPL", item.ident);
    let ident = &item.ident;

    let tys = item
        .items
        .iter()
        .filter_map(|item| {
            if let TraitItem::Type(item) = item {
                Some(item)
            } else {
                None
            }
        })
        .cloned()
        .collect::<Vec<_>>();

    let generate_hash = generate_generics(hash, &item.generics, tys.clone());

    let mut generics = item.generics.clone();
    let mut type_generics = quote!();

    for param in &generics.params {
        if let GenericParam::Type(ty) = param {
            let ident = &ty.ident;

            type_generics.extend(quote!(#ident,));
        }
    }

    for ty in tys {
        let o_ident = &ty.ident;
        let ident = format_ident!("__DERIVE_ASSOC_{}", &*o_ident);

        generics.params.push(parse_quote!(#ident));
        type_generics.extend(quote!(#o_ident = #ident,));
    }

    for param in &mut generics.params {
        if let GenericParam::Type(ty) = param {
            ty.bounds.push(parse_quote!(looking_glass::Typed));
        } else {
            return quote! {
                const _: () = {
                    compile_error!("cannot type trait object with const or lifetime parameters")
                };
            };
        }
    }

    let (impl_generics, _, where_clause) = generics.split_for_impl();

    type_generics = quote!(<#type_generics>);

    quote! {
        #[allow(non_upper_case_globals, non_camel_case_types)]
        const #const_ident: () = {
            macro_rules! marker_variants {
                ($(
                    $($marker:ident)*
                ),+) => {
                    $(
                        impl #impl_generics looking_glass::Typed for Box<dyn #ident #type_generics $(+ $marker)*> #where_clause {
                            fn ty() -> looking_glass::Id {
                                use looking_glass::highway::{HighwayBuilder, HighwayHash};

                                looking_glass::Id::hidden_new_do_not_call_manually(#generate_hash)
                            }
                        }
                    )+
                }
            }

            marker_variants! {
                ,
                Sync,
                Send, Sync Send
            }
        };
    }
}
