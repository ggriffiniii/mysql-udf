extern crate mysql_udf;
use mysql_udf::{
    create_udf_returning_int, create_udf_returning_real, ArgValue, InitUdfArgsIter, RowUdfArgsIter,
    Udf, UdfInit,
};
use std::os::raw::{c_double, c_longlong};

struct ArgCount;
impl Udf<c_longlong> for ArgCount {
    type Output = c_longlong;

    fn new(_init: &mut UdfInit, _init_args: InitUdfArgsIter) -> Result<Self, String> {
        Ok(ArgCount)
    }

    fn process_row(&self, args: RowUdfArgsIter) -> Result<Self::Output, ()> {
        Ok(args.count() as Self::Output)
    }
}

struct Add;
impl Udf<c_longlong> for Add {
    type Output = c_longlong;

    fn new(_init: &mut UdfInit, init_args: InitUdfArgsIter) -> Result<Self, String> {
        for (idx, arg) in init_args.enumerate() {
            match arg.arg_value() {
                ArgValue::Int(_) => {}
                _ => {
                    return Err(format!(
                        "Add only accepts integer values. Arg {} is not an integer",
                        idx
                    ))
                }
            };
        }
        Ok(Add)
    }

    fn process_row(&self, args: RowUdfArgsIter) -> Result<Self::Output, ()> {
        let mut total = 0;
        for arg in args {
            match arg.arg_value() {
                ArgValue::Int(Some(val)) => total += val,
                _ => {}
            }
        }
        Ok(total)
    }
}

struct AddF;
impl Udf<c_double> for AddF {
    type Output = c_double;
    fn new(_init: &mut UdfInit, init_args: InitUdfArgsIter) -> Result<Self, String> {
        for (idx, arg) in init_args.enumerate() {
            match arg.arg_value() {
                ArgValue::Int(_) => {}
                ArgValue::Real(_) => {}
                _ => {
                    return Err(format!(
                        "Add only accepts integer or real values. Arg {} is not an integer",
                        idx
                    ))
                }
            };
        }
        Ok(AddF)
    }

    fn process_row(&self, args: RowUdfArgsIter) -> Result<Self::Output, ()> {
        let mut total = 0.0;
        for arg in args {
            match arg.arg_value() {
                ArgValue::Int(Some(val)) => total += val as f64,
                ArgValue::Real(Some(val)) => total += val,
                _ => {}
            }
        }
        Ok(total)
    }
}

create_udf_returning_int!(argcount, ArgCount);
create_udf_returning_int!(my_add, Add);
create_udf_returning_real!(my_addf, AddF);
