#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
extern crate libc;
extern crate mysql_udf_derive;
pub use mysql_udf_derive::{create_udf_returning_int, create_udf_returning_real};
mod mysql_bindings;
pub use mysql_bindings::{UDF_INIT, UDF_ARGS, my_bool};
use mysql_bindings::*;
use std::os::raw::{c_char, c_double, c_longlong, c_uint, c_ulong};

pub struct UdfInit<'a> {
	udf_init: &'a mut UDF_INIT,
}

impl<'a> UdfInit<'a> {
	pub fn set_maybe_null(&mut self, nullable: bool) {
		self.udf_init.maybe_null = nullable as c_char;
	}

	pub fn set_decimals(&mut self, decimals: u16) {
		self.udf_init.decimals = c_uint::from(decimals);
	}

	pub fn set_max_length(&mut self, max_length: u32) {
		self.udf_init.max_length = c_uint::from(max_length);
	}

	pub fn set_const_item(&mut self, is_const_item: bool) {
		self.udf_init.const_item = c_char::from(is_const_item);
	}
}

impl UDF_ARGS {
	fn init_args_iter_mut(&mut self) -> InitUdfArgsIter {
		InitUdfArgsIter {
			idx: 0,
			row_iter: self.row_args_iter_mut(),
		}
	}

	fn row_args_iter_mut(&mut self) -> RowUdfArgsIter {
		RowUdfArgsIter {
			idx: 0,
			udf_args: self,
		}
	}
}

pub struct RowUdfArg<'a> {
	arg_type: &'a mut Item_result,
	arg: *mut c_char,
	length: c_ulong,
}

impl<'a> RowUdfArg<'a> {
	pub fn arg_value(&self) -> ArgValue<'a> {
		ArgValue::new(*self.arg_type, self.arg, self.length)
	}
}

pub struct InitUdfArg<'a> {
	row_arg: RowUdfArg<'a>,
	maybe_null_: bool,
}

impl<'a> InitUdfArg<'a> {
	pub fn arg_value(&self) -> ArgValue<'a> {
		self.row_arg.arg_value()
	}

	pub fn maybe_null(&self) -> bool {
		self.maybe_null_
	}
}

pub struct RowUdfArgsIter<'a> {
	idx: u32,
	udf_args: &'a mut UDF_ARGS,
}

impl<'a> Iterator for RowUdfArgsIter<'a> {
	type Item = RowUdfArg<'a>;
	fn next(&mut self) -> Option<Self::Item> {
		let output = if self.idx < self.udf_args.arg_count {
			let idx = self.idx as isize;
			Some(RowUdfArg {
				arg_type: unsafe { &mut *self.udf_args.arg_type.offset(idx) },
				arg: unsafe { *self.udf_args.args.offset(idx) },
				length: unsafe { *self.udf_args.lengths.offset(idx) },
			})
		} else {
			None
		};
		self.idx += 1;
		output
	}
}

pub struct InitUdfArgsIter<'a> {
	idx: isize,
	row_iter: RowUdfArgsIter<'a>,
}

impl<'a> Iterator for InitUdfArgsIter<'a> {
	type Item = InitUdfArg<'a>;
	fn next(&mut self) -> Option<Self::Item> {
		let output = self.row_iter.next();
		let output = match output {
			None => None,
			Some(row_arg) => Some(InitUdfArg {
				row_arg,
				maybe_null_: unsafe { *self.row_iter.udf_args.maybe_null.offset(self.idx) } != 0,
			}),
		};
		self.idx += 1;
		output
	}
}

pub enum ArgValue<'a> {
	String(Option<&'a [u8]>),
	Real(Option<c_double>),
	Int(Option<c_longlong>),
	Decimal(Option<&'a [u8]>),
}

impl<'a> ArgValue<'a> {
	fn new(arg_type: Item_result, arg: *mut c_char, length: c_ulong) -> ArgValue<'a> {
		match arg_type {
			Item_result_STRING_RESULT => ArgValue::String(unsafe {
				(arg as *const u8)
					.as_ref()
					.map(|arg| ::std::slice::from_raw_parts(arg, length as usize))
			}),
			Item_result_REAL_RESULT => {
				ArgValue::Real(unsafe { (arg as *const c_double).as_ref().cloned() })
			}
			Item_result_INT_RESULT => {
				ArgValue::Int(unsafe { (arg as *const c_longlong).as_ref().cloned() })
			}
			Item_result_DECIMAL_RESULT => ArgValue::Decimal(unsafe {
				(arg as *const u8)
					.as_ref()
					.map(|arg| ::std::slice::from_raw_parts(arg, length as usize))
			}),
			unknown_arg_type => panic!("unsupported arg type: {}", unknown_arg_type),
		}
	}
}

pub trait UdfOutput<T> {
	fn nullable() -> bool;
	fn is_null(&self) -> bool;
	fn output(self) -> T;
}

impl UdfOutput<c_longlong> for c_longlong {
	fn nullable() -> bool {
		false
	}
	fn is_null(&self) -> bool {
		false
	}
	fn output(self) -> c_longlong {
		self
	}
}

impl UdfOutput<c_longlong> for Option<c_longlong> {
	fn nullable() -> bool {
		true
	}
	fn is_null(&self) -> bool {
		true
	}
	fn output(self) -> c_longlong {
		self.unwrap()
	}
}

impl UdfOutput<c_double> for c_double {
	fn nullable() -> bool {
		false
	}
	fn is_null(&self) -> bool {
		false
	}
	fn output(self) -> c_double {
		self
	}
}

impl UdfOutput<c_double> for Option<c_double> {
	fn nullable() -> bool {
		true
	}
	fn is_null(&self) -> bool {
		true
	}
	fn output(self) -> c_double {
		self.unwrap()
	}
}

pub trait Udf<T>: Send + Sync + Sized {
	type Output: UdfOutput<T>;
	fn new(init: &mut UdfInit, init_args: InitUdfArgsIter) -> Result<Self, String>;
	fn process_row(&self, args: RowUdfArgsIter) -> Result<Self::Output, ()>;
}

pub unsafe fn init<U, R>(initid: *mut UDF_INIT, args: *mut UDF_ARGS, msg: *mut c_char) -> my_bool
where
	U: Udf<R>,
{
	let initid: &mut UDF_INIT = &mut *initid;
	let args = &mut *args;
	match safe_init::<U, R>(initid, args) {
		Err(err_msg) => {
			let len = ::std::cmp::min(MYSQL_ERRMSG_SIZE as usize, err_msg.len());
			let err_msg = ::std::ffi::CString::new(&err_msg[..len]).or_else(|_| ::std::ffi::CString::new("unknown error")).unwrap();
			libc::strcpy(msg, err_msg.as_ptr());
			1
		},
		Ok(_) => 0,
	}
}

fn safe_init<U, R>(initid: &mut UDF_INIT, args: &mut UDF_ARGS) -> Result<(), String>
where
	U: Udf<R>,
{
	let args_iter = args.init_args_iter_mut();
	let udf = {
		let mut udf_init = UdfInit { udf_init: initid };
		udf_init.set_maybe_null(U::Output::nullable());
		U::new(&mut udf_init, args_iter)?
	};
	let udf = Box::new(udf);
	let raw_udf = Box::into_raw(udf) as *mut c_char;
	initid.ptr = raw_udf;
	Ok(())
}

pub unsafe fn process_row<U, R>(
	initid: *mut UDF_INIT,
	args: *mut UDF_ARGS,
	is_null: *mut c_char,
	error: *mut c_char,
) -> R
where
	U: Udf<R>,
	R: Default,
{
	let args = &mut *args;
	let initid: &mut UDF_INIT = &mut *initid;
	let udf = &mut *(initid.ptr as *mut U);
	match udf.process_row(args.row_args_iter_mut()) {
		Err(_) => {
			*error = 1;
			Default::default()
		}
		Ok(result) => {
			if result.is_null() {
				*is_null = 1;
				Default::default()
			} else {
				result.output()
			}
		}
	}
}

pub unsafe fn deinit<T>(initid: *mut UDF_INIT) {
	let initid: &mut UDF_INIT = &mut *initid;
	let _owned = Box::from_raw(initid.ptr as *mut T);
}