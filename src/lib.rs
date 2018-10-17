#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
extern crate libc;
mod mysql_bindings;
use mysql_bindings::*;
use std::io::Write;

fn debug_file() -> ::std::fs::File {
	::std::fs::OpenOptions::new().create(true).append(true).open("/tmp/debug.log").unwrap()
}

struct UdfInit<'a> {
	udf_init: &'a mut UDF_INIT,
}

impl<'a> UdfInit<'a> {
	fn set_maybe_null(&mut self, nullable: bool) {
		self.udf_init.maybe_null = nullable as ::std::os::raw::c_char;
	}

	fn set_decimals(&mut self, decimals: u16) {
		self.udf_init.decimals = decimals as ::std::os::raw::c_uint;
	}

	fn set_max_length(&mut self, max_length: u32) {
		self.udf_init.max_length = max_length as ::std::os::raw::c_uint;
	}

	fn set_const_item(&mut self, is_const_item: bool) {
		self.udf_init.const_item = is_const_item as ::std::os::raw::c_char;
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
	arg: *mut std::os::raw::c_char,
	length: std::os::raw::c_ulong,
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
	Real(Option<std::os::raw::c_double>),
	Int(Option<std::os::raw::c_longlong>),
	Decimal(Option<&'a [u8]>),
}

impl<'a> ArgValue<'a> {
	fn new(arg_type: Item_result, arg: *mut std::os::raw::c_char, length: std::os::raw::c_ulong) -> ArgValue<'a> {
		match arg_type {
			Item_result_STRING_RESULT => {
				ArgValue::String(unsafe { (arg as *const u8).as_ref().map(|arg| unsafe { ::std::slice::from_raw_parts(arg, length as usize) }) })
			},
			Item_result_REAL_RESULT => {
				ArgValue::Real(unsafe { (arg as *const std::os::raw::c_double).as_ref().map(|arg| *arg )})
			},
			Item_result_INT_RESULT => {
				ArgValue::Int(unsafe { (arg as *const std::os::raw::c_longlong).as_ref().map(|arg| *arg )})
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
	type Output = ::std::os::raw::c_longlong;

	fn new(init: &mut UdfInit, mut init_args: InitUdfArgsIter) -> Result<Self, String> {
		writeln!(&debug_file(), "creating new argcount");
		Ok(ArgCount)
	}

	fn process_row(&self, mut args: RowUdfArgsIter) -> Result<Self::Output, ()> {
		writeln!(&debug_file(), "argcount is processing row");
		Ok(args.count() as Self::Output)
	}
}

macro_rules! create_init_fn {
	($name:expr, $ty:ty) => {
		#[no_mangle]
		#[export_name = $name]
		pub extern "C" fn init(initid: *mut UDF_INIT, mut args: *mut UDF_ARGS, msg: *mut std::os::raw::c_char) -> my_bool {
			writeln!(&debug_file(), "argcount_init");
			let initid: &mut UDF_INIT = unsafe {&mut *initid};
			let args = unsafe { &mut *args };
			let args_iter = args.init_args_iter_mut();
			let udf = <$ty as UDF>::new(&mut UdfInit{udf_init: unsafe {&mut *initid}}, args_iter);
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
					let raw_udf = Box::into_raw(udf) as *mut ::std::os::raw::c_char;
					writeln!(&debug_file(), "argcount_pointer: {:?}", raw_udf);
					initid.ptr = raw_udf;
					0
				}
			}
		}
	}
}

macro_rules! create_process_row_fn {
	($name:expr, $ty:ty) => {
		#[no_mangle]
		#[export_name = $name]
		pub extern "C" fn process_row(initid: *mut UDF_INIT, args: *mut UDF_ARGS, is_null: *mut std::os::raw::c_char, error: *mut std::os::raw::c_char) -> std::os::raw::c_longlong {
			writeln!(&debug_file(), "argcount");
			let args = unsafe { &mut *args };
			let initid: &mut UDF_INIT = unsafe { &mut *initid };
			writeln!(&debug_file(), "initid.ptr == {:?}", initid.ptr);
			let udf = unsafe { &mut *(initid.ptr as *mut $ty) };
			match udf.process_row(args.row_args_iter_mut()) {
				Err(_) => {
					unsafe { *error = 1 };
					0
				}
				Ok(result) => {
					result
				}
			}
		}
	}

}

macro_rules! create_deinit_fn {
	($name:expr, $ty:ty) => {
		#[no_mangle]
		#[export_name = $name]
		pub extern "C" fn deinit(initid: *mut UDF_INIT) {
			writeln!(&debug_file(), "argcount_deinit");
			let initid: &mut UDF_INIT = unsafe {&mut *initid};
			writeln!(&debug_file(), "initid.ptr == {:?}", initid.ptr);
			let owned = unsafe { Box::from_raw(initid.ptr as *mut $ty); };
			writeln!(&debug_file(), "owned: {:?}", owned);
		}
	}
}

macro_rules! create_udf {
	($name:ident, $ty:ty) => {
		pub mod $name {
			use super::*;
			create_init_fn!(concat!(stringify!($name), "_init"), $ty);
			create_process_row_fn!(stringify!($name), $ty);
			create_deinit_fn!(concat!(stringify!($name), "_deinit"), $ty);
		}
	}
}

create_udf!(new_argcount, ArgCount);

pub mod argcount {
	use super::*;
	#[no_mangle]
	#[export_name = "argcount_init"]
	pub extern "C" fn init(initid: *mut UDF_INIT, mut args: *mut UDF_ARGS, msg: *mut std::os::raw::c_char) -> my_bool {
		writeln!(&debug_file(), "argcount_init");
		let initid: &mut UDF_INIT = unsafe {&mut *initid};
		let args = unsafe { &mut *args };
		let args_iter = args.init_args_iter_mut();
		let argcount = ArgCount::new(&mut UdfInit{udf_init: unsafe {&mut *initid}}, args_iter);
		match argcount {
			Err(err_msg) => {
				let len = ::std::cmp::min(80, err_msg.len());  // TODO(use mysql constant rather than 80)
				let err_msg = ::std::ffi::CString::new(&err_msg[..len]).unwrap();
				unsafe {
					libc::strcpy(msg, err_msg.as_ptr());
				}
				1
			},
			Ok(argcount) => {
				let argcount = Box::new(argcount);
				let raw_argcount = Box::into_raw(argcount) as *mut ::std::os::raw::c_char;
				writeln!(&debug_file(), "argcount_pointer: {:?}", raw_argcount);
				initid.ptr = raw_argcount;
				0
			}
		}
	}

	#[no_mangle]
	#[export_name = "argcount"]
	pub extern "C" fn process_row(initid: *mut UDF_INIT, args: *mut UDF_ARGS, is_null: *mut std::os::raw::c_char, error: *mut std::os::raw::c_char) -> std::os::raw::c_longlong {
		writeln!(&debug_file(), "argcount");
		let args = unsafe { &mut *args };
		let initid: &mut UDF_INIT = unsafe { &mut *initid };
		writeln!(&debug_file(), "initid.ptr == {:?}", initid.ptr);
		let udf = unsafe { &mut *(initid.ptr as *mut ArgCount) };
		match udf.process_row(args.row_args_iter_mut()) {
			Err(_) => {
				unsafe { *error = 1 };
				0
			}
			Ok(result) => {
				result
			}
		}
	}

	#[no_mangle]
	#[export_name = "argcount_deinit"]
	pub extern "C" fn deinit(initid: *mut UDF_INIT) {
		writeln!(&debug_file(), "argcount_deinit");
		let initid: &mut UDF_INIT = unsafe {&mut *initid};
		writeln!(&debug_file(), "initid.ptr == {:?}", initid.ptr);
		let owned = unsafe { Box::from_raw(initid.ptr as *mut ArgCount); };
		writeln!(&debug_file(), "owned: {:?}", owned);
	}
}