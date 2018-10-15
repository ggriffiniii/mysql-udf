#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
mod mysql_bindings;
use mysql_bindings::*;

struct UdfArg<'a> {
	arg_type: &'a mut Item_result,
	arg: *mut std::os::raw::c_char,
	length: &'a mut std::os::raw::c_ulong,
	maybe_null: &'a mut std::os::raw::c_char,
}

struct UdfArgsIter<'a> {
	idx: u32,
	udf_args: &'a mut UDF_ARGS,
}

impl<'a> Iterator for UdfArgsIter<'a> {
	type Item = UdfArg<'a>;
	fn next(&mut self) -> Option<Self::Item> {
		use std::fs::OpenOptions;
		use std::io::Write;
		let mut file = OpenOptions::new()
            .write(true)
            .create(true)
			.append(true)
            .open("/tmp/udf.log").unwrap();
		write!(&mut file, "idx: {}, arg_count: {}\n", self.idx, self.udf_args.arg_count);
		let output = if self.idx < self.udf_args.arg_count {
			let idx = self.idx as isize;
			Some(UdfArg {
				arg_type: unsafe {&mut *self.udf_args.arg_type.offset(idx)},
				arg: unsafe {*self.udf_args.args.offset(idx)},
				length: unsafe {&mut *self.udf_args.lengths.offset(idx)},
				maybe_null: unsafe {&mut *self.udf_args.maybe_null.offset(idx)},
			})
		} else {
			None
		};
		self.idx += 1;
		output
	}
}

#[no_mangle]
pub extern "C" fn testudf_init(initid: UDF_INIT, mut args: UDF_ARGS, msg: *mut std::os::raw::c_char) -> my_bool {
	let args = UdfArgsIter{idx: 0, udf_args: &mut args};
	0
}

#[no_mangle]
pub extern "C" fn testudf(initid: *mut UDF_INIT, args: *mut UDF_ARGS, is_null: *mut std::os::raw::c_char, error: *mut std::os::raw::c_char) -> std::os::raw::c_longlong {
	let mut args = UdfArgsIter{idx: 0, udf_args: unsafe {&mut *args}};
	args.count() as ::std::os::raw::c_longlong
}

#[no_mangle]
pub extern "C" fn testudf_deinit(initid: UDF_INIT) {
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
