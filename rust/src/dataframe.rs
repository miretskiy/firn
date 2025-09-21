use crate::{
    execute_expr_ops, ContextType, ExecutionContext, FfiResult, Operation, PolarsHandle, RawStr,
    ERROR_INVALID_UTF8, ERROR_NULL_ARGS, ERROR_NULL_HANDLE, ERROR_POLARS_OPERATION,
};
use polars::prelude::*;
use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::ptr;

/// Helper function to convert RawStr array to Vec<String>
unsafe fn raw_str_array_to_vec(
    raw_strs: *const RawStr,
    count: usize,
) -> std::result::Result<Vec<String>, &'static str> {
    if raw_strs.is_null() {
        return Err("RawStr array cannot be null");
    }

    let raw_str_slice = std::slice::from_raw_parts(raw_strs, count);
    let mut result = Vec::with_capacity(count);

    for raw_str in raw_str_slice {
        match raw_str.as_str() {
            Ok(s) => result.push(s.to_string()),
            Err(_) => return Err("Invalid UTF-8 in string"),
        }
    }

    Ok(result)
}

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

// Removed AggArgs - will be reimplemented with proper context handling

/// Arguments for concatenation operations
#[repr(C)]
pub struct ConcatArgs {
    pub handles: *const usize, // Array of DataFrame handles
    pub count: usize,          // Number of DataFrames to concatenate
}

/// Arguments for filter operations with expressions
#[repr(C)]
pub struct FilterExprArgs {
    pub expr_ops: *const Operation, // Array of expression operations
    pub expr_count: usize,          // Number of expression operations
}

/// Dispatch function for creating new empty DataFrame
pub fn dispatch_new_empty() -> FfiResult {
    let df = DataFrame::empty();
    FfiResult::success(df)
}

/// Dispatch function for reading CSV
pub fn dispatch_read_csv(_handle: PolarsHandle, context: &ExecutionContext) -> FfiResult {
    let args = unsafe { &*(context.operation_args as *const ReadCsvArgs) };

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
pub fn dispatch_select(handle: PolarsHandle, context: &ExecutionContext) -> FfiResult {
    if handle.handle == 0 {
        return FfiResult::error(ERROR_NULL_HANDLE, "Handle cannot be null");
    }

    let args = unsafe { &*(context.operation_args as *const SelectArgs) };

    // Convert RawStr array to Vec<String> using helper
    let columns = match unsafe { raw_str_array_to_vec(args.columns, args.column_count) } {
        Ok(cols) => cols,
        Err(msg) => return FfiResult::error(ERROR_NULL_ARGS, msg),
    };

    // Convert Vec<String> to Vec<Expr> for Polars
    let column_exprs: Vec<Expr> = columns.iter().map(|s| col(s)).collect();

    // Get context type and perform operation based on current context
    let context_type = match handle.get_context_type() {
        Some(ct) => ct,
        None => return FfiResult::error(ERROR_POLARS_OPERATION, "Invalid context type"),
    };

    match context_type {
        ContextType::DataFrame => {
            // Convert DataFrame to LazyFrame and select
            let df = unsafe { &*(handle.handle as *const DataFrame) };
            let lazy_frame = df.clone().lazy().select(column_exprs);
            FfiResult::success_lazy(lazy_frame)
        }
        ContextType::LazyFrame => {
            // Chain select operation on existing LazyFrame
            let lazy_frame = unsafe { &*(handle.handle as *const LazyFrame) };
            let new_lazy_frame = lazy_frame.clone().select(column_exprs);
            FfiResult::success_lazy(new_lazy_frame)
        }
        ContextType::LazyGroupBy => {
            // Invalid operation - cannot select on grouped data without aggregation
            FfiResult::error(
                ERROR_POLARS_OPERATION,
                "Cannot call select() on grouped data. Call agg() first to resolve grouping.",
            )
        }
    }
}

/// Dispatch function for group by operation
/// Groups the DataFrame by specified columns - this is a complete operation by itself
pub fn dispatch_group_by(handle: PolarsHandle, context: &ExecutionContext) -> FfiResult {
    if handle.handle == 0 {
        return FfiResult::error(ERROR_NULL_HANDLE, "Handle cannot be null");
    }

    // Extract grouping columns from args
    let args = unsafe { &*(context.operation_args as *const GroupByArgs) };

    // Convert RawStr array to Vec<String>
    let group_columns = match unsafe { raw_str_array_to_vec(args.columns, args.column_count) } {
        Ok(cols) => cols,
        Err(msg) => return FfiResult::error(ERROR_NULL_ARGS, msg),
    };

    let column_refs: Vec<&str> = group_columns.iter().map(|s| s.as_str()).collect();

    // Get context type and perform operation based on current context
    let context_type = match handle.get_context_type() {
        Some(ct) => ct,
        None => return FfiResult::error(ERROR_POLARS_OPERATION, "Invalid context type"),
    };

    match context_type {
        ContextType::DataFrame => {
            // Convert DataFrame to LazyFrame and group by with default count aggregation
            let df = unsafe { &*(handle.handle as *const DataFrame) };
            let lazy_frame = df
                .clone()
                .lazy()
                .group_by(column_refs)
                .agg([len().alias("count")]);
            FfiResult::success_lazy(lazy_frame)
        }
        ContextType::LazyFrame => {
            // Chain group by operation on existing LazyFrame with default count aggregation
            let lazy_frame = unsafe { &*(handle.handle as *const LazyFrame) };
            let new_lazy_frame = lazy_frame
                .clone()
                .group_by(column_refs)
                .agg([len().alias("count")]);
            FfiResult::success_lazy(new_lazy_frame)
        }
        ContextType::LazyGroupBy => {
            // Invalid operation - cannot group already grouped data
            FfiResult::error(
                ERROR_POLARS_OPERATION,
                "Cannot call group_by() on already grouped data.",
            )
        }
    }
}

// Removed dispatch_agg - will be reimplemented with proper context handling

/// Dispatch function for count operation (returns DataFrame with count column)
pub fn dispatch_count(handle: PolarsHandle) -> FfiResult {
    if handle.handle == 0 {
        return FfiResult::error(ERROR_NULL_HANDLE, "Handle cannot be null");
    }

    // Get context type and perform operation based on current context
    let context_type = match handle.get_context_type() {
        Some(ct) => ct,
        None => return FfiResult::error(ERROR_POLARS_OPERATION, "Invalid context type"),
    };

    match context_type {
        ContextType::DataFrame => {
            // Convert DataFrame to LazyFrame and count
            let df = unsafe { &*(handle.handle as *const DataFrame) };
            let lazy_frame = df.clone().lazy().select([len().alias("count")]);
            FfiResult::success_lazy(lazy_frame)
        }
        ContextType::LazyFrame => {
            // Chain count operation on existing LazyFrame
            let lazy_frame = unsafe { &*(handle.handle as *const LazyFrame) };
            let new_lazy_frame = lazy_frame.clone().select([len().alias("count")]);
            FfiResult::success_lazy(new_lazy_frame)
        }
        ContextType::LazyGroupBy => {
            // Invalid operation - cannot count grouped data without aggregation
            FfiResult::error(
                ERROR_POLARS_OPERATION,
                "Cannot call count() on grouped data. Call agg() first to resolve grouping.",
            )
        }
    }
}

/// Concatenate multiple DataFrames vertically (union)
/// Note: _handle is unused as this follows functional style concat(df1, df2, df3)
/// rather than method style df1.concat(df2, df3)
pub fn dispatch_concat(_handle: PolarsHandle, context: &ExecutionContext) -> FfiResult {
    let args = unsafe { &*(context.operation_args as *const ConcatArgs) };

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

/// Dispatch function for select with expressions operation
pub fn dispatch_select_expr(handle: PolarsHandle, context: &ExecutionContext) -> FfiResult {
    if handle.handle == 0 {
        return FfiResult::error(ERROR_NULL_HANDLE, "Handle cannot be null");
    }

    let expr_stack = unsafe { &mut *context.expr_stack };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "No expressions available for select operation",
        );
    }

    // Collect ALL expressions from the stack (not just one like with_column)
    let exprs: Vec<_> = expr_stack.drain(..).collect();

    // Get context type and perform operation based on current context
    let context_type = match handle.get_context_type() {
        Some(ct) => ct,
        None => return FfiResult::error(ERROR_POLARS_OPERATION, "Invalid context type"),
    };

    match context_type {
        ContextType::DataFrame => {
            // Convert DataFrame to LazyFrame and select expressions
            let df = unsafe { &*(handle.handle as *const DataFrame) };
            let lazy_frame = df.clone().lazy().select(exprs);
            FfiResult::success_lazy(lazy_frame)
        }
        ContextType::LazyFrame => {
            // Chain select operation on existing LazyFrame
            let lazy_frame = unsafe { &*(handle.handle as *const LazyFrame) };
            let new_lazy_frame = lazy_frame.clone().select(exprs);
            FfiResult::success_lazy(new_lazy_frame)
        }
        ContextType::LazyGroupBy => {
            // Invalid operation - cannot select on grouped data without aggregation
            FfiResult::error(
                ERROR_POLARS_OPERATION,
                "Cannot call select() on grouped data. Call agg() first to resolve grouping.",
            )
        }
    }
}

/// Dispatch function for with_columns operation
pub fn dispatch_with_column(handle: PolarsHandle, context: &ExecutionContext) -> FfiResult {
    if handle.handle == 0 {
        return FfiResult::error(ERROR_NULL_HANDLE, "Handle cannot be null");
    }

    let expr_stack = unsafe { &mut *context.expr_stack };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "WithColumn operation requires an expression on the stack",
        );
    }

    // Collect ALL expressions from the stack (like select_expr)
    let exprs: Vec<_> = expr_stack.drain(..).collect();

    // Get context type and perform operation based on current context
    let context_type = match handle.get_context_type() {
        Some(ct) => ct,
        None => return FfiResult::error(ERROR_POLARS_OPERATION, "Invalid context type"),
    };

    match context_type {
        ContextType::DataFrame => {
            // Convert DataFrame to LazyFrame and add columns
            let df = unsafe { &*(handle.handle as *const DataFrame) };
            let lazy_frame = df.clone().lazy().with_columns(exprs);
            FfiResult::success_lazy(lazy_frame)
        }
        ContextType::LazyFrame => {
            // Chain with_columns operation on existing LazyFrame
            let lazy_frame = unsafe { &*(handle.handle as *const LazyFrame) };
            let new_lazy_frame = lazy_frame.clone().with_columns(exprs);
            FfiResult::success_lazy(new_lazy_frame)
        }
        ContextType::LazyGroupBy => {
            // Invalid operation - cannot add columns to grouped data without aggregation
            FfiResult::error(
                ERROR_POLARS_OPERATION,
                "Cannot call with_columns() on grouped data. Call agg() first to resolve grouping.",
            )
        }
    }
}

/// Dispatch function for filter with expression
pub fn dispatch_filter_expr(handle: PolarsHandle, context: &ExecutionContext) -> FfiResult {
    if handle.handle == 0 {
        return FfiResult::error(ERROR_NULL_HANDLE, "Handle cannot be null");
    }

    let args = unsafe { &*(context.operation_args as *const FilterExprArgs) };

    // Convert Operation array to slice
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

    // Get context type and perform operation based on current context
    let context_type = match handle.get_context_type() {
        Some(ct) => ct,
        None => return FfiResult::error(ERROR_POLARS_OPERATION, "Invalid context type"),
    };

    match context_type {
        ContextType::DataFrame => {
            // Convert DataFrame to LazyFrame and apply filter
            let df = unsafe { &*(handle.handle as *const DataFrame) };
            let lazy_frame = df.clone().lazy().filter(filter_expr);
            FfiResult::success_lazy(lazy_frame)
        }
        ContextType::LazyFrame => {
            // Chain filter operation on existing LazyFrame
            let lazy_frame = unsafe { &*(handle.handle as *const LazyFrame) };
            let new_lazy_frame = lazy_frame.clone().filter(filter_expr);
            FfiResult::success_lazy(new_lazy_frame)
        }
        ContextType::LazyGroupBy => {
            // Invalid operation - cannot filter grouped data without aggregation
            FfiResult::error(
                ERROR_POLARS_OPERATION,
                "Cannot call filter() on grouped data. Call agg() first to resolve grouping.",
            )
        }
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

/// Testing helper - adds a null row to DataFrame
/// Collect operation - materializes LazyFrames into DataFrames
/// If already a DataFrame, returns it as-is
pub fn dispatch_collect(handle: PolarsHandle) -> FfiResult {
    if handle.handle == 0 {
        return FfiResult::error(ERROR_NULL_HANDLE, "Handle cannot be null");
    }

    // Get context type and perform operation based on current context
    let context_type = match handle.get_context_type() {
        Some(ct) => ct,
        None => return FfiResult::error(ERROR_POLARS_OPERATION, "Invalid context type"),
    };

    match context_type {
        ContextType::DataFrame => {
            // Already materialized - return as-is
            let df = unsafe { &*(handle.handle as *const DataFrame) };
            FfiResult::success(df.clone())
        }
        ContextType::LazyFrame => {
            // Materialize LazyFrame into DataFrame
            let lazy_frame = unsafe { &*(handle.handle as *const LazyFrame) };
            match lazy_frame.clone().collect() {
                Ok(df) => FfiResult::success(df),
                Err(e) => FfiResult::error(ERROR_POLARS_OPERATION, &e.to_string()),
            }
        }
        ContextType::LazyGroupBy => {
            // Invalid operation - cannot collect grouped data without aggregation
            FfiResult::error(
                ERROR_POLARS_OPERATION,
                "Cannot collect grouped data. Call agg() first to resolve grouping.",
            )
        }
    }
}

pub fn dispatch_add_null_row(handle: PolarsHandle) -> FfiResult {
    if handle.handle == 0 {
        return FfiResult::error(ERROR_NULL_HANDLE, "Handle cannot be null");
    }

    // This operation only works on DataFrames, not LazyFrames
    let context_type = match handle.get_context_type() {
        Some(ct) => ct,
        None => return FfiResult::error(ERROR_POLARS_OPERATION, "Invalid context type"),
    };

    match context_type {
        ContextType::DataFrame => {
            let df = unsafe { &*(handle.handle as *const DataFrame) };

            // Create a single row with nulls for each column
            let null_series: Result<Vec<Series>, PolarsError> = df
                .get_columns()
                .iter()
                .map(|col| {
                    // Use full_null for all types - it's the standard Polars way
                    Ok(Series::full_null(col.name().clone(), 1, col.dtype()))
                })
                .collect();

            let null_series = match null_series {
                Ok(series) => series,
                Err(e) => return FfiResult::error(ERROR_POLARS_OPERATION, &e.to_string()),
            };

            // Convert Series to Columns
            let null_columns: Vec<Column> = null_series.into_iter().map(|s| s.into()).collect();

            let null_df = match DataFrame::new(null_columns) {
                Ok(df) => df,
                Err(e) => return FfiResult::error(ERROR_POLARS_OPERATION, &e.to_string()),
            };

            // Concatenate the original DataFrame with the null row
            match df.clone().vstack(&null_df) {
                Ok(result_df) => FfiResult::success(result_df),
                Err(e) => FfiResult::error(ERROR_POLARS_OPERATION, &e.to_string()),
            }
        }
        ContextType::LazyFrame | ContextType::LazyGroupBy => {
            // Invalid operation - add_null_row only works on materialized DataFrames
            FfiResult::error(
                ERROR_POLARS_OPERATION,
                "Cannot add null row to lazy frame. Call collect() first.",
            )
        }
    }
}
