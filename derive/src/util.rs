use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_quote, GenericParam, Generics, ItemTrait, TraitItem, TraitItemType};
use synstructure::{AddBounds, Structure};

pub fn generate_structure(mut structure: Structure, hash: [u64; 4]) -> TokenStream {
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

pub fn generate_trait(item: ItemTrait, hash: [u64; 4]) -> TokenStream {
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

pub fn generate_generics(hash: [u64; 4], generics: &Generics, tys: Vec<TraitItemType>) -> TokenStream {
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
