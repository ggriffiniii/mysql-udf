#![recursion_limit="128"]
extern crate proc_macro;
extern crate proc_macro2;
extern crate quote;
extern crate syn;
use proc_macro2::TokenStream;
use syn::parse::{Parse, ParseStream};
use syn::{parse_macro_input, Ident, Token, Type};
use syn::export::Span;
use quote::quote;

struct UdfNameAndType {
    name: Ident,
    typ: Type,
}

impl Parse for UdfNameAndType {
    fn parse(input: ParseStream) -> syn::parse::Result<UdfNameAndType> {
        let name: Ident = input.parse()?;
        input.parse::<Token![,]>()?;
        let typ: Type = input.parse()?;
        Ok(UdfNameAndType { name, typ })
    }
}

#[proc_macro]
pub fn create_udf_returning_int(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let UdfNameAndType { name, typ } = parse_macro_input!(input as UdfNameAndType);
    let init_and_deinit = create_init_and_deinit(&name, &typ);
    let expanded = quote! {
        #init_and_deinit

        #[no_mangle]
        pub extern "C" fn #name(
            initid: *mut ::mysql_udf::UDF_INIT,
            args: *mut ::mysql_udf::UDF_ARGS,
            is_null: *mut ::std::os::raw::c_char,
            error: *mut ::std::os::raw::c_char) -> ::std::os::raw::c_longlong {
			unsafe { ::mysql_udf::process_row::<#typ, _>(initid, args, is_null, error) }
        }
    };
    proc_macro::TokenStream::from(expanded)
}

#[proc_macro]
pub fn create_udf_returning_real(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let UdfNameAndType { name, typ } = parse_macro_input!(input as UdfNameAndType);
    let init_and_deinit = create_init_and_deinit(&name, &typ);
    let expanded = quote! {
        #init_and_deinit

        #[no_mangle]
        pub extern "C" fn #name(
            initid: *mut ::mysql_udf::UDF_INIT,
            args: *mut ::mysql_udf::UDF_ARGS,
            is_null: *mut ::std::os::raw::c_char,
            error: *mut ::std::os::raw::c_char) -> ::std::os::raw::c_double {
			unsafe { ::mysql_udf::process_row::<#typ, _>(initid, args, is_null, error) }
        }
    };
    proc_macro::TokenStream::from(expanded)
}

fn create_init_and_deinit(name: &Ident, typ: &Type) -> TokenStream {
    let init_ident = Ident::new(&format!("{}_init", name), Span::call_site());
    let deinit_ident = Ident::new(&format!("{}_deinit", name), Span::call_site());
    quote! {
        #[no_mangle]
        pub extern "C" fn #init_ident(
            initid: *mut ::mysql_udf::UDF_INIT,
            args: *mut ::mysql_udf::UDF_ARGS,
            msg: *mut ::std::os::raw::c_char
        ) -> ::mysql_udf::my_bool {
            unsafe { ::mysql_udf::init::<#typ>(initid, args, msg) }
        }

        #[no_mangle]
        pub extern "C" fn #deinit_ident(initid: *mut ::mysql_udf::UDF_INIT) {
			unsafe { ::mysql_udf::deinit::<#typ>(initid) }
		}
    }
}