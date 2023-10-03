extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(FromNum)]
pub fn add_from_num_implementation(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = &input.ident;
    let variants = if let syn::Data::Enum(data_enum) = &input.data {
        &data_enum.variants
    } else {
        panic!("EnumToNum can only be derived for enums.");
    };

    let has_values = variants.iter().any(|v| !v.fields.is_empty());

    let to_type = match variants.len() {
        0..=255 => quote! {u8},
        256..=65535 => quote! {u16},
        65536..=4294967295 => quote! {u32},
        _ => panic!(
            "Whoah there cowboy, how did you manage to get {} values in {}?",
            variants.len(),
            name
        ),
    };

    let arms = variants.iter().enumerate().map(|(index, variant)| {
        let variant_name = &variant.ident;
        let to_value = index as u8;
        if variant.fields.is_empty() {
            quote! {
                #name::#variant_name => #to_value,
            }
        } else {
            match variant.fields {
                syn::Fields::Named(_) => quote! {
                    #name::#variant_name{..} => #to_value,
                },
                syn::Fields::Unnamed(_) => quote! {
                    #name::#variant_name(..) => #to_value,
                },
                _ => quote! {
                    #name::#variant_name => #to_value,
                },
            }
        }
    });

    let arms_cloned = arms.clone();

    let mut expanded = quote! {
        impl From<&#name> for #to_type {
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

            impl From<#name> for #to_type {
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
