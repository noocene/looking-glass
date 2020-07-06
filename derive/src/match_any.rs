use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
    Arm, Expr, Token, Type,
};

#[derive(Clone, Debug)]
struct TypeMatchArm {
    ty_annotation: Option<(Token![as], Type, Token![:])>,
    arm: Arm,
}

impl Parse for TypeMatchArm {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        Ok(Self {
            ty_annotation: {
                if input.peek(Token![as]) {
                    let as_token = input.parse()?;
                    let ty = input.parse()?;
                    let col_token = input.parse()?;
                    Some((as_token, ty, col_token))
                } else {
                    None
                }
            },
            arm: input.parse()?,
        })
    }
}

#[derive(Clone, Debug)]
struct TypeMatchInput {
    spawner: Expr,
    comma_1: Token![,],
    expr: Expr,
    comma_2: Token![,],
    arms: Punctuated<TypeMatchArm, Option<Token![,]>>,
}

impl Parse for TypeMatchInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        Ok(Self {
            spawner: input.parse()?,
            comma_1: input.parse()?,
            expr: input.parse()?,
            comma_2: input.parse()?,
            arms: Punctuated::parse_terminated(input)?,
        })
    }
}

pub fn match_any(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input: TypeMatchInput = parse_macro_input!(item);

    let in_expr = input.expr;
    let spawner = input.spawner;

    let mut stream = quote!();
    for arm in input.arms {
        let ty_hint = arm
            .ty_annotation
            .map(|annotation| {
                let annotation = annotation.1;
                quote!(#annotation)
            })
            .unwrap_or_else(|| quote!(_));
        let arm = arm.arm;
        let guard = arm
            .guard
            .map(|guard| {
                let guard = guard.1;
                quote!(#guard)
            })
            .unwrap_or_else(|| quote!(true));
        let pat = arm.pat;
        let expr = arm.body;
        stream.extend(quote! {
            match looking_glass::ProtocolAny::downcast::<#ty_hint, _>(_gen_item.take().unwrap(), _spawner.clone()).await {
                Ok(_temp_data) => {
                    match _temp_data {
                        #pat if #guard => {
                            break Ok({ #expr });
                        }
                        _temp_data => _gen_item = Some(looking_glass::Erase::erase(_temp_data))
                    }
                }
                Err((looking_glass::DowncastError::TypeMismatch, data)) => {
                    _gen_item = data;
                }
                Err((err, data)) => { break Err((looking_glass::ErasedDowncastError::erase_downcast_error(err), data)); }
            }
        });
    }
    (quote! {
        {
            extern crate looking_glass;
            let mut _gen_item = Some({ #in_expr });
            let _spawner = { #spawner };
            async move {
                let a: Result<_, (looking_glass::ErasedDowncastError, Option<looking_glass::ProtocolAny<_>>)> = loop {
                    #stream
                    break Err((looking_glass::ErasedDowncastError::TypeMismatch, _gen_item.take()));
                };
                a
            }
        }
    })
    .into()
}
