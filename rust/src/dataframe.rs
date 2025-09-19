use crate::{
    execute_expr_ops, raw_str_array_to_vec, ExecutionContext, ExprOp, FfiResult, RawStr,
    ERROR_INVALID_UTF8, ERROR_NULL_ARGS, ERROR_NULL_HANDLE, ERROR_POLARS_OPERATION,
};
use polars::prelude::*;
use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::ptr;

/// Arguments for reading CSV files
#[repr(C)]
pub struct ReadCsvArgs {
    pub path: RawStr,     // File path using zero-copy RawStr
    pub has_header: bool, // Whether CSV has header row
    pub with_glob: bool,  // Whether to expand glob patterns
}

/// Arguments for select operations
#[repr(C)]
pub struct SelectArgs {
    pub columns: *const RawStr, // Array of column names
    pub column_count: usize,    // Number of columns
}

/// Arguments for group by operations
#[repr(C)]
pub struct GroupByArgs {
    pub columns: *const RawStr, // Array of column names to group by
    pub column_count: usize,    // Number of columns
}

/// Arguments for concatenation operations
#[repr(C)]
pub struct ConcatArgs {
    pub handles: *const usize, // Array of DataFrame handles
    pub count: usize,          // Number of DataFrames to concatenate
}

/// Arguments for filter operations with expressions
#[repr(C)]
pub struct FilterExprArgs {
    pub expr_ops: *const ExprOp, // Array of expression operations
    pub expr_count: usize,       // Number of expression operations
}

/// Dispatch function for creating new empty DataFrame
#[no_mangle]
pub extern "C" fn dispatch_new_empty(_handle: usize, _context: usize) -> FfiResult {
    let df = DataFrame::empty();
    FfiResult::success(df)
}

/// Dispatch function for reading CSV
#[no_mangle]
pub extern "C" fn dispatch_read_csv(_handle: usize, context: usize) -> FfiResult {
    if context == 0 {
        return FfiResult::error(ERROR_NULL_ARGS, "ExecutionContext cannot be null");
    }

    let ctx = unsafe { &*(context as *const ExecutionContext) };
    let args = unsafe { &*(ctx.operation_args as *const ReadCsvArgs) };

    // Convert RawStr to &str using zero-copy approach
    let path_str = match unsafe { args.path.as_str() } {
        Ok(s) => s,
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in path"),
    };

    // Use LazyCsvReader with configurable options
    match LazyCsvReader::new(path_str)
        .with_glob(args.with_glob) // Configurable glob pattern expansion
        .with_has_header(args.has_header) // Configurable header detection
        .finish()
    {
        Ok(lazy_frame) => match lazy_frame.collect() {
            Ok(df) => FfiResult::success(df),
            Err(e) => FfiResult::error(ERROR_POLARS_OPERATION, &e.to_string()),
        },
        Err(e) => FfiResult::error(ERROR_POLARS_OPERATION, &e.to_string()),
    }
}

/// Dispatch function for select operation
#[no_mangle]
pub extern "C" fn dispatch_select(handle: usize, context: usize) -> FfiResult {
    if handle == 0 {
        return FfiResult::error(ERROR_NULL_HANDLE, "DataFrame handle cannot be null");
    }
    if context == 0 {
        return FfiResult::error(ERROR_NULL_ARGS, "ExecutionContext cannot be null");
    }

    let df = unsafe { &*(handle as *const DataFrame) };
    let ctx = unsafe { &*(context as *const ExecutionContext) };
    let args = unsafe { &*(ctx.operation_args as *const SelectArgs) };

    // Convert RawStr array to Vec<String> using helper
    let columns = match unsafe { raw_str_array_to_vec(args.columns, args.column_count) } {
        Ok(cols) => cols,
        Err(msg) => return FfiResult::error(ERROR_NULL_ARGS, msg),
    };

    // Convert Vec<String> to Vec<Expr> for Polars
    let column_exprs: Vec<Expr> = columns.iter().map(|s| col(s)).collect();

    // Perform the select operation
    match df.clone().lazy().select(column_exprs).collect() {
        Ok(new_df) => FfiResult::success(new_df),
        Err(e) => FfiResult::error(ERROR_POLARS_OPERATION, &e.to_string()),
    }
}

/// Dispatch function for group by operation
#[no_mangle]
pub extern "C" fn dispatch_group_by(handle: usize, context: usize) -> FfiResult {
    if handle == 0 {
        return FfiResult::error(1, "DataFrame handle cannot be null");
    }
    if context == 0 {
        return FfiResult::error(2, "ExecutionContext cannot be null");
    }

    let df = unsafe { &*(handle as *const DataFrame) };
    let ctx = unsafe { &*(context as *const ExecutionContext) };
    let args = unsafe { &*(ctx.operation_args as *const GroupByArgs) };

    // Convert RawStr array to Vec<String> using helper
    let columns = match unsafe { raw_str_array_to_vec(args.columns, args.column_count) } {
        Ok(cols) => cols,
        Err(msg) => return FfiResult::error(ERROR_NULL_ARGS, msg),
    };

    // Convert Vec<String> to Vec<&str> for Polars
    let column_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();

    // Perform the group by operation and count
    match df
        .clone()
        .lazy()
        .group_by(column_refs)
        .agg([len().alias("count")])
        .collect()
    {
        Ok(new_df) => FfiResult::success(new_df),
        Err(e) => FfiResult::error(ERROR_POLARS_OPERATION, &e.to_string()),
    }
}

/// Dispatch function for count operation (returns DataFrame with count column)
#[no_mangle]
pub extern "C" fn dispatch_count(handle: usize, _context: usize) -> FfiResult {
    if handle == 0 {
        return FfiResult::error(ERROR_NULL_HANDLE, "DataFrame handle cannot be null");
    }

    let df = unsafe { &*(handle as *const DataFrame) };

    // Count rows in the DataFrame - returns DataFrame with "count" column
    match df.clone().lazy().select([len().alias("count")]).collect() {
        Ok(new_df) => FfiResult::success(new_df),
        Err(e) => FfiResult::error(ERROR_POLARS_OPERATION, &e.to_string()),
    }
}

/// Concatenate multiple DataFrames vertically (union)
/// Note: _handle is unused as this follows functional style concat(df1, df2, df3)
/// rather than method style df1.concat(df2, df3)
#[no_mangle]
pub extern "C" fn dispatch_concat(_handle: usize, context: usize) -> FfiResult {
    if context == 0 {
        return FfiResult::error(ERROR_NULL_ARGS, "ExecutionContext cannot be null");
    }

    let ctx = unsafe { &*(context as *const ExecutionContext) };
    let args = unsafe { &*(ctx.operation_args as *const ConcatArgs) };

    if args.handles.is_null() || args.count == 0 {
        return FfiResult::error(ERROR_NULL_ARGS, "Concat handles cannot be null or empty");
    }

    // Convert handle array to DataFrames
    let handles = unsafe { std::slice::from_raw_parts(args.handles, args.count) };
    let mut dataframes = Vec::new();

    for &handle in handles {
        if handle == 0 {
            return FfiResult::error(ERROR_NULL_HANDLE, "DataFrame handle cannot be null");
        }
        let df = unsafe { &*(handle as *const DataFrame) };
        dataframes.push(df.clone().lazy());
    }

    // Concatenate all DataFrames
    match concat(dataframes, UnionArgs::default()) {
        Ok(lazy_frame) => match lazy_frame.collect() {
            Ok(df) => FfiResult::success(df),
            Err(e) => FfiResult::error(ERROR_POLARS_OPERATION, &e.to_string()),
        },
        Err(e) => FfiResult::error(ERROR_POLARS_OPERATION, &e.to_string()),
    }
}

/// Dispatch function for with_columns operation
#[no_mangle]
pub extern "C" fn dispatch_with_column(handle: usize, context: usize) -> FfiResult {
    if handle == 0 {
        return FfiResult::error(ERROR_NULL_HANDLE, "DataFrame handle cannot be null");
    }
    if context == 0 {
        return FfiResult::error(ERROR_NULL_ARGS, "ExecutionContext cannot be null");
    }

    let ctx = unsafe { &*(context as *const ExecutionContext) };
    let expr_stack = unsafe { &mut *ctx.expr_stack };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "WithColumn operation requires an expression on the stack",
        );
    }

    let expr = expr_stack.pop().unwrap();
    let df = unsafe { &*(handle as *const DataFrame) };

    match df.clone().lazy().with_columns([expr]).collect() {
        Ok(new_df) => FfiResult::success(new_df),
        Err(e) => FfiResult::error(ERROR_POLARS_OPERATION, &e.to_string()),
    }
}

/// Dispatch function for filter with expression
#[no_mangle]
pub extern "C" fn dispatch_filter_expr(handle: usize, context: usize) -> FfiResult {
    if handle == 0 {
        return FfiResult::error(ERROR_NULL_HANDLE, "DataFrame handle cannot be null");
    }
    if context == 0 {
        return FfiResult::error(ERROR_NULL_ARGS, "ExecutionContext cannot be null");
    }

    let ctx = unsafe { &*(context as *const ExecutionContext) };
    let args = unsafe { &*(ctx.operation_args as *const FilterExprArgs) };

    // Convert ExprOp array to slice
    if args.expr_ops.is_null() || args.expr_count == 0 {
        return FfiResult::error(
            ERROR_NULL_ARGS,
            "Expression operations cannot be null or empty",
        );
    }

    let expr_ops = unsafe { std::slice::from_raw_parts(args.expr_ops, args.expr_count) };

    // Execute expression operations to build the filter expression
    let filter_expr = match execute_expr_ops(expr_ops) {
        Ok(expr) => expr,
        Err(msg) => return FfiResult::error(ERROR_POLARS_OPERATION, msg),
    };

    // Apply the filter to the DataFrame
    let df = unsafe { &*(handle as *const DataFrame) };
    match df.clone().lazy().filter(filter_expr).collect() {
        Ok(new_df) => FfiResult::success(new_df),
        Err(e) => FfiResult::error(ERROR_POLARS_OPERATION, &e.to_string()),
    }
}

/// Convert DataFrame to CSV string
#[no_mangle]
pub extern "C" fn dataframe_to_csv(handle: usize) -> *mut c_char {
    if handle == 0 {
        return ptr::null_mut();
    }

    let df = unsafe { &*(handle as *const DataFrame) };

    let mut cursor = std::io::Cursor::new(Vec::new());
    let mut df_clone = df.clone();
    match CsvWriter::new(&mut cursor).finish(&mut df_clone) {
        Ok(_) => {
            let csv_data = cursor.into_inner();
            match CString::new(csv_data) {
                Ok(c_string) => c_string.into_raw(),
                Err(_) => ptr::null_mut(),
            }
        }
        Err(_) => ptr::null_mut(),
    }
}

/// Convert DataFrame to string representation (tabular format)
#[no_mangle]
pub extern "C" fn dataframe_to_string(handle: usize) -> *mut c_char {
    if handle == 0 {
        return ptr::null_mut();
    }

    let df = unsafe { &*(handle as *const DataFrame) };
    let df_string = format!("{}", df);

    match CString::new(df_string) {
        Ok(c_string) => c_string.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

/// Get DataFrame height (number of rows)
#[no_mangle]
pub extern "C" fn dataframe_height(handle: usize) -> usize {
    if handle == 0 {
        return 0;
    }

    let df = unsafe { &*(handle as *const DataFrame) };
    df.height()
}

/// Release DataFrame memory
#[no_mangle]
pub extern "C" fn release_dataframe(handle: usize) -> c_int {
    if handle != 0 {
        unsafe {
            let _ = Box::from_raw(handle as *mut DataFrame);
        }
    }
    0 // Return success
}

/// Free C string memory
#[no_mangle]
pub extern "C" fn free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe {
            let _ = CString::from_raw(ptr);
        }
    }
}

/// Benchmark helper - no-op function for measuring CGO overhead
#[no_mangle]
pub extern "C" fn noop() -> c_int {
    0
}
