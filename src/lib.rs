#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
extern crate libc;
mod mysql_bindings;
use mysql_bindings::*;
use std::io::Write;
use std::os::raw::{c_char, c_longlong, c_double, c_ulong, c_uint};

fn debug_file() -> ::std::fs::File {
	::std::fs::OpenOptions::new().create(true).append(true).open("/tmp/debug.log").unwrap()
}

struct UdfInit<'a> {
	udf_init: &'a mut UDF_INIT,
}

impl<'a> UdfInit<'a> {
	fn set_maybe_null(&mut self, nullable: bool) {
		self.udf_init.maybe_null = nullable as c_char;
	}

	fn set_decimals(&mut self, decimals: u16) {
		self.udf_init.decimals = decimals as c_uint;
	}

	fn set_max_length(&mut self, max_length: u32) {
		self.udf_init.max_length = max_length as c_uint;
	}

	fn set_const_item(&mut self, is_const_item: bool) {
		self.udf_init.const_item = is_const_item as c_char;
	}
}

impl UDF_ARGS {
	fn init_args_iter_mut(&mut self) -> InitUdfArgsIter {
		InitUdfArgsIter{idx: 0, row_iter: self.row_args_iter_mut()}
	}

	fn row_args_iter_mut(&mut self) -> RowUdfArgsIter {
		RowUdfArgsIter{idx: 0, udf_args: self}
	}
}

struct RowUdfArg<'a> {
	arg_type: &'a mut Item_result,
	arg: *mut c_char,
	length: c_ulong,
}

impl<'a> RowUdfArg<'a> {
	fn arg_value(&self) -> ArgValue<'a> {
		ArgValue::new(*self.arg_type, self.arg, self.length)
	}
}

struct InitUdfArg<'a> {
	row_arg: RowUdfArg<'a>,
	maybe_null_: bool,
}

impl<'a> InitUdfArg<'a> {
	fn arg_value(&self) -> ArgValue<'a> {
		self.row_arg.arg_value()
	}

	fn maybe_null(&self) -> bool {
		self.maybe_null_
	}
}

struct RowUdfArgsIter<'a> {
	idx: u32,
	udf_args: &'a mut UDF_ARGS,
}

impl<'a> Iterator for RowUdfArgsIter<'a> {
	type Item = RowUdfArg<'a>;
	fn next(&mut self) -> Option<Self::Item> {
		let output = if self.idx < self.udf_args.arg_count {
			let idx = self.idx as isize;
			Some(RowUdfArg {
				arg_type: unsafe {&mut *self.udf_args.arg_type.offset(idx)},
				arg: unsafe {*self.udf_args.args.offset(idx)},
				length: unsafe {*self.udf_args.lengths.offset(idx)},
			})
		} else {
			None
		};
		self.idx += 1;
		output
	}
}

struct InitUdfArgsIter<'a> {
	idx: isize,
	row_iter: RowUdfArgsIter<'a>,
}

impl<'a> Iterator for InitUdfArgsIter<'a> {
	type Item = InitUdfArg<'a>;
	fn next(&mut self) -> Option<Self::Item> {
		let output = self.row_iter.next();
		let output = match output {
			None => None,
			Some(row_arg) => Some(InitUdfArg{row_arg, maybe_null_: unsafe { *self.row_iter.udf_args.maybe_null.offset(self.idx)} != 0 }),
		};
		self.idx += 1;
		output
	}
}

enum ArgValue<'a> {
	String(Option<&'a [u8]>),
	Real(Option<c_double>),
	Int(Option<c_longlong>),
	Decimal(Option<&'a [u8]>),
}

impl<'a> ArgValue<'a> {
	fn new(arg_type: Item_result, arg: *mut c_char, length: c_ulong) -> ArgValue<'a> {
		match arg_type {
			Item_result_STRING_RESULT => {
				ArgValue::String(unsafe { (arg as *const u8).as_ref().map(|arg| unsafe { ::std::slice::from_raw_parts(arg, length as usize) }) })
			},
			Item_result_REAL_RESULT => {
				ArgValue::Real(unsafe { (arg as *const c_double).as_ref().map(|arg| *arg )})
			},
			Item_result_INT_RESULT => {
				ArgValue::Int(unsafe { (arg as *const c_longlong).as_ref().map(|arg| *arg )})
			},
			Item_result_DECIMAL_RESULT => {
				ArgValue::Decimal(unsafe { (arg as *const u8).as_ref().map(|arg| unsafe { ::std::slice::from_raw_parts(arg, length as usize) }) })
			},
			unknown_arg_type => panic!("unsupported arg type: {}", unknown_arg_type),
		}
	}
}

trait UDF: Send + Sync 
where
	Self: Sized
{
	type Output;

	fn new(init: &mut UdfInit, init_args: InitUdfArgsIter) -> Result<Self, String>;
	fn process_row(&self, args: RowUdfArgsIter) -> Result<Self::Output, ()>;
}

struct ArgCount;

impl UDF for ArgCount {
	type Output = c_longlong;

	fn new(init: &mut UdfInit, mut init_args: InitUdfArgsIter) -> Result<Self, String> {
		Ok(ArgCount)
	}

	fn process_row(&self, mut args: RowUdfArgsIter) -> Result<Self::Output, ()> {
		Ok(args.count() as Self::Output)
	}
}

struct Add;
impl UDF for Add {
	type Output = c_longlong;

	fn new(init: &mut UdfInit, mut init_args: InitUdfArgsIter) -> Result<Self, String> {
		for (idx, arg) in init_args.enumerate() {
			match arg.arg_value() {
				ArgValue::Int(_) => {},
				_ => return Err(format!("Add only accepts integer values. Arg {} is not an integer", idx)),
			};
		}
		Ok(Add)
	}

	fn process_row(&self, mut args: RowUdfArgsIter) -> Result<Self::Output, ()> {
		let mut total = 0;
		for arg in args {
			match arg.arg_value() {
				ArgValue::Int(Some(val)) => total += val,
				_ => {},
			}
		}
		Ok(total)
	}
}

struct AddF;
impl UDF for AddF {
	type Output = f64;
	fn new(init: &mut UdfInit, mut init_args: InitUdfArgsIter) -> Result<Self, String> {
		for (idx, arg) in init_args.enumerate() {
			match arg.arg_value() {
				ArgValue::Int(_) => {},
				ArgValue::Real(_) => {},
				_ => return Err(format!("Add only accepts integer or real values. Arg {} is not an integer", idx)),
			};
		}
		Ok(AddF)
	}

	fn process_row(&self, mut args: RowUdfArgsIter) -> Result<Self::Output, ()> {
		let mut total = 0.0;
		for arg in args {
			match arg.arg_value() {
				ArgValue::Int(Some(val)) => total += val as f64,
				ArgValue::Real(Some(val)) => total += val,
				_ => {},
			}
		}
		Ok(total)
	}
}

fn init<T: UDF>(initid: *mut UDF_INIT, mut args: *mut UDF_ARGS, msg: *mut c_char) -> my_bool {
	let initid: &mut UDF_INIT = unsafe {&mut *initid};
	let args = unsafe { &mut *args };
	let args_iter = args.init_args_iter_mut();
	let udf = T::new(&mut UdfInit{udf_init: unsafe {&mut *initid}}, args_iter);
	match udf {
		Err(err_msg) => {
			let len = ::std::cmp::min(80, err_msg.len());  // TODO(use mysql constant rather than 80)
			let err_msg = ::std::ffi::CString::new(&err_msg[..len]).unwrap();
			unsafe {
				libc::strcpy(msg, err_msg.as_ptr());
			}
			1
		},
		Ok(udf) => {
			let udf = Box::new(udf);
			let raw_udf = Box::into_raw(udf) as *mut c_char;
			initid.ptr = raw_udf;
			0
		}
	}
}

fn process_row_primitive_return<T, R>(initid: *mut UDF_INIT, args: *mut UDF_ARGS, is_null: *mut c_char, error: *mut c_char) -> R
where
	T: UDF,
	T::Output: Into<R>,
	R: From<i8>,
{
	let args = unsafe { &mut *args };
	let initid: &mut UDF_INIT = unsafe { &mut *initid };
	let udf = unsafe { &mut *(initid.ptr as *mut T) };
	match udf.process_row(args.row_args_iter_mut()) {
		Err(_) => {
			unsafe { *error = 1 };
			0.into()
		}
		Ok(result) => {
			result.into()
		}
	}
}

fn deinit<T: UDF>(initid: *mut UDF_INIT) {
	let initid: &mut UDF_INIT = unsafe {&mut *initid};
	let owned = unsafe { Box::from_raw(initid.ptr as *mut T); };
}

macro_rules! create_init_fn {
	($name:expr, $ty:ty) => {
		#[no_mangle]
		#[export_name = $name]
		pub extern "C" fn init(initid: *mut UDF_INIT, mut args: *mut UDF_ARGS, msg: *mut c_char) -> my_bool {
			super::init::<$ty>(initid, args, msg)
		}
	}
}

macro_rules! create_deinit_fn {
	($name:expr, $ty:ty) => {
		#[no_mangle]
		#[export_name = $name]
		pub extern "C" fn deinit(initid: *mut UDF_INIT) {
			super::deinit::<$ty>(initid)
		}
	}
}

macro_rules! create_process_row_fn_returning_int {
	($name:expr, $ty:ty) => {
		#[no_mangle]
		#[export_name = $name]
		pub extern "C" fn process_row(initid: *mut UDF_INIT, args: *mut UDF_ARGS, is_null: *mut c_char, error: *mut c_char) -> c_longlong {
			super::process_row_primitive_return::<$ty, c_longlong>(initid, args, is_null, error)
		}
	}
}

macro_rules! create_process_row_fn_returning_double {
	($name:expr, $ty:ty) => {
		#[no_mangle]
		#[export_name = $name]
		pub extern "C" fn process_row(initid: *mut UDF_INIT, args: *mut UDF_ARGS, is_null: *mut c_char, error: *mut c_char) -> c_double {
			super::process_row_primitive_return::<$ty, c_double>(initid, args, is_null, error)
		}
	}
}


macro_rules! create_udf_returning_int {
	($name:ident, $ty:ty) => {
		pub mod $name {
			use super::*;
			create_init_fn!(concat!(stringify!($name), "_init"), $ty);
			create_process_row_fn_returning_int!(stringify!($name), $ty);
			create_deinit_fn!(concat!(stringify!($name), "_deinit"), $ty);
		}
	}
}

macro_rules! create_udf_returning_double {
	($name:ident, $ty:ty) => {
		pub mod $name {
			use super::*;
			create_init_fn!(concat!(stringify!($name), "_init"), $ty);
			create_process_row_fn_returning_double!(stringify!($name), $ty);
			create_deinit_fn!(concat!(stringify!($name), "_deinit"), $ty);
		}
	}
}

create_udf_returning_int!(argcount, ArgCount);
create_udf_returning_int!(my_add, Add);
create_udf_returning_double!(my_addf, AddF);
