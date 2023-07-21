extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(ToU8)]
pub fn to_u8(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = &input.ident;
    let variants = if let syn::Data::Enum(data_enum) = &input.data {
        &data_enum.variants
    } else {
        panic!("ToU8 can only be derived for enums.");
    };

    let has_values = variants.iter().any(|v| !v.fields.is_empty());

    let arms = variants.iter().enumerate().map(|(index, variant)| {
        let variant_name = &variant.ident;
        let to_value = index as u8;
        if variant.fields.is_empty() {
            quote! {
                #name::#variant_name => #to_value,
            }
        } else {
            quote! {
                #name::#variant_name(..) => #to_value,
            }
        }
    });

    let arms_cloned = arms.clone();

    let mut expanded = quote! {
        impl From<&#name> for u8 {
            fn from(value: &#name) -> Self {
                match value {
                    #(#arms)*
                }
            }
        }
    };

    if !has_values {
        expanded = quote! {
            #expanded

            impl From<#name> for u8 {
                fn from(value: #name) -> Self {
                    match value {
                        #(#arms_cloned)*
                    }
                }
            }
        }
    }

    TokenStream::from(expanded)
}
