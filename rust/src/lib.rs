use polars::prelude::*;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::ptr;

/// Error codes for better error handling
pub const ERROR_NULL_HANDLE: c_int = 1;
pub const ERROR_NULL_ARGS: c_int = 2;
pub const ERROR_INVALID_UTF8: c_int = 3;
pub const ERROR_POLARS_OPERATION: c_int = 4;
pub const ERROR_EMPTY_OPERATION_STACK: c_int = 5;

/// Helper function to convert RawStr array to Vec<String>
unsafe fn raw_str_array_to_vec(
    ptr: *const RawStr,
    count: c_int,
) -> std::result::Result<Vec<String>, &'static str> {
    if ptr.is_null() || count <= 0 {
        return Err("RawStr array cannot be null or empty");
    }

    let slice = std::slice::from_raw_parts(ptr, count as usize);
    let mut result = Vec::with_capacity(count as usize);

    for raw_str in slice {
        let rust_str = raw_str.as_str().map_err(|_| "Invalid UTF-8 in string")?;
        result.push(rust_str.to_owned());
    }

    Ok(result)
}

/// Raw string representation for zero-copy Go memory access
#[repr(C)]
pub struct RawStr {
    pub data: *const c_char,
    pub len: usize,
}

impl RawStr {
    /// Convert RawStr to Rust &str (unsafe - caller must ensure validity)
    pub unsafe fn as_str(&self) -> std::result::Result<&str, std::str::Utf8Error> {
        if self.data.is_null() || self.len == 0 {
            return Ok("");
        }
        let slice = std::slice::from_raw_parts(self.data as *const u8, self.len);
        std::str::from_utf8(slice)
    }
}

/// Operation-specific argument structs
#[repr(C)]
pub struct SelectArgs {
    pub columns: *const RawStr, // Array of RawStr
    pub column_count: c_int,
}

/// Centralized literal abstraction - handles all value types
#[repr(C)]
pub struct Literal {
    pub value_type: c_int, // 0=int, 1=float, 2=string, 3=bool
    pub int_value: i64,
    pub float_value: f64,
    pub string_value: RawStr,
    pub bool_value: bool,
}

#[repr(C)]
pub struct GroupByArgs {
    pub columns: *const RawStr, // Array of RawStr
    pub column_count: c_int,
}

#[repr(C)]
pub struct ReadCsvArgs {
    pub path: RawStr, // RawStr instead of c_char*
}

#[repr(C)]
pub struct CountArgs {
    // No arguments needed for count
}

// Expression operation - one step in building an expression
#[repr(C)]
pub struct ExprOp {
    pub func_ptr: usize,
    pub args: usize,
}

// Arguments for expression operations
#[repr(C)]
pub struct ColumnArgs {
    pub name: RawStr,
}

#[repr(C)]
pub struct LiteralArgs {
    pub literal: Literal,
}

// Filter with expression arguments
#[repr(C)]
pub struct FilterExprArgs {
    pub expr_ops: *const ExprOp,
    pub expr_count: usize,
}

/// Generic operation structure with function pointer and args
#[repr(C)]
pub struct Operation {
    pub func_ptr: usize, // Function pointer as usize
    pub args: usize,     // Pointer to operation-specific args as usize
}

/// Operation stack passed from Go
#[repr(C)]
pub struct OperationStack {
    pub operations: *const Operation,
    pub count: usize,
}

/// Result struct returned to Go
#[repr(C)]
pub struct Result {
    pub handle: usize, // Raw pointer as opaque handle or data payload
    pub error_code: c_int,
    pub error_message: *mut c_char,
    pub error_frame: usize, // Frame pointer - which operation failed
}

impl Result {
    fn success(df: DataFrame) -> Self {
        let boxed = Box::new(df);
        Self {
            handle: Box::into_raw(boxed) as usize,
            error_code: 0,
            error_message: ptr::null_mut(),
            error_frame: 0,
        }
    }

    fn error(code: c_int, message: &str) -> Self {
        Self::error_at_frame(code, message, 0)
    }

    fn error_at_frame(code: c_int, message: &str, frame: usize) -> Self {
        let c_message =
            CString::new(message).unwrap_or_else(|_| CString::new("Invalid UTF-8").unwrap());
        Self {
            handle: 0,
            error_code: code,
            error_message: c_message.into_raw(),
            error_frame: frame,
        }
    }
}

/// Dispatch function for creating new empty DataFrame
#[no_mangle]
pub extern "C" fn dispatch_new_empty(_handle: usize, _args: usize) -> Result {
    let df = DataFrame::empty();
    Result::success(df)
}

/// Dispatch function for reading CSV
#[no_mangle]
pub extern "C" fn dispatch_read_csv(_handle: usize, args: usize) -> Result {
    if args == 0 {
        return Result::error(ERROR_NULL_ARGS, "ReadCSV args cannot be null");
    }

    let args = unsafe { &*(args as *const ReadCsvArgs) };

    // Convert RawStr to &str using zero-copy approach
    let path_str = match unsafe { args.path.as_str() } {
        Ok(s) => s,
        Err(_) => return Result::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in path"),
    };

    // Use LazyCsvReader for lazy evaluation and optimization
    // Polars will copy the path internally, so Go memory is safe to release after this
    match LazyCsvReader::new(path_str).finish() {
        Ok(lazy_frame) => match lazy_frame.collect() {
            Ok(df) => Result::success(df),
            Err(e) => Result::error(ERROR_POLARS_OPERATION, &e.to_string()),
        },
        Err(e) => Result::error(ERROR_POLARS_OPERATION, &e.to_string()),
    }
}

/// Dispatch function for select operation
#[no_mangle]
pub extern "C" fn dispatch_select(handle: usize, args: usize) -> Result {
    if handle == 0 {
        return Result::error(ERROR_NULL_HANDLE, "DataFrame handle cannot be null");
    }
    if args == 0 {
        return Result::error(ERROR_NULL_ARGS, "Select args cannot be null");
    }

    let df = unsafe { &*(handle as *const DataFrame) };
    let args = unsafe { &*(args as *const SelectArgs) };

    // Convert RawStr array to Vec<String> using helper
    let columns = match unsafe { raw_str_array_to_vec(args.columns, args.column_count) } {
        Ok(cols) => cols,
        Err(msg) => return Result::error(ERROR_NULL_ARGS, msg),
    };

    match df.clone().select(columns.iter().map(|s| s.as_str())) {
        Ok(new_df) => Result::success(new_df),
        Err(e) => Result::error(ERROR_POLARS_OPERATION, &e.to_string()),
    }
}

impl Literal {
    /// Convert Literal to Polars Expr
    pub fn to_expr(&self) -> std::result::Result<Expr, &'static str> {
        match self.value_type {
            0 => Ok(lit(self.int_value)),
            1 => Ok(lit(self.float_value)),
            2 => {
                let string_val = unsafe { self.string_value.as_str() }
                    .map_err(|_| "Invalid UTF-8 in string value")?;
                Ok(lit(string_val))
            }
            3 => Ok(lit(self.bool_value)),
            _ => Err("Invalid value type"),
        }
    }
}

/// Dispatch function for group by operation
#[no_mangle]
pub extern "C" fn dispatch_group_by(handle: usize, args: usize) -> Result {
    if handle == 0 {
        return Result::error(1, "DataFrame handle cannot be null");
    }
    if args == 0 {
        return Result::error(2, "GroupBy args cannot be null");
    }

    let df = unsafe { &*(handle as *const DataFrame) };
    let args = unsafe { &*(args as *const GroupByArgs) };

    // Convert RawStr array to Vec<String> using helper
    let columns = match unsafe { raw_str_array_to_vec(args.columns, args.column_count) } {
        Ok(cols) => cols,
        Err(msg) => return Result::error(ERROR_NULL_ARGS, msg),
    };

    // Simple group by implementation - just group and count
    let group_cols: Vec<_> = columns.iter().map(|s| col(s)).collect();
    match df
        .clone()
        .lazy()
        .group_by(group_cols)
        .agg([len().alias("count")])
        .collect()
    {
        Ok(new_df) => Result::success(new_df),
        Err(e) => Result::error(ERROR_POLARS_OPERATION, &e.to_string()),
    }
}

/// Dispatch function for count operation
#[no_mangle]
pub extern "C" fn dispatch_count(handle: usize, _args: usize) -> Result {
    if handle == 0 {
        return Result::error(ERROR_NULL_HANDLE, "DataFrame handle cannot be null");
    }

    let df = unsafe { &*(handle as *const DataFrame) };

    // Count rows in the DataFrame
    match df.clone().lazy().select([len().alias("count")]).collect() {
        Ok(new_df) => Result::success(new_df),
        Err(e) => Result::error(ERROR_POLARS_OPERATION, &e.to_string()),
    }
}

/// Release a DataFrame handle
#[no_mangle]
pub extern "C" fn release(handle: usize) -> c_int {
    if handle == 0 {
        return 1; // Error: null handle
    }

    unsafe {
        let _ = Box::from_raw(handle as *mut DataFrame);
    }
    0 // Success
}

/// Execute operations using function pointers - the main entry point for batched operations
#[no_mangle]
pub extern "C" fn execute_operations(
    handle: usize,
    operations_ptr: *const Operation,
    count: usize,
) -> Result {
    if operations_ptr.is_null() || count == 0 {
        return Result::error(1, "Empty operation stack");
    }

    let operations = unsafe { std::slice::from_raw_parts(operations_ptr, count) };

    let mut current_handle = handle;

    for (frame_idx, op) in operations.iter().enumerate() {
        // Cast function pointer and call it
        type DispatchFn = extern "C" fn(usize, usize) -> Result;
        let dispatch_fn = unsafe { std::mem::transmute::<usize, DispatchFn>(op.func_ptr) };

        let result = dispatch_fn(current_handle, op.args);

        if result.error_code != 0 {
            return Result::error_at_frame(
                result.error_code,
                &unsafe { CStr::from_ptr(result.error_message) }.to_string_lossy(),
                frame_idx,
            );
        }

        // Update handle for next operation - each operation returns a new DataFrame
        current_handle = result.handle;
    }
    // Return the final handle as a successful result
    Result {
        handle: current_handle,
        error_code: 0,
        error_message: ptr::null_mut(),
        error_frame: 0,
    }
}

/// Free error message memory (called from Go)
#[no_mangle]
pub extern "C" fn free_error(error_message: *mut c_char) {
    if !error_message.is_null() {
        unsafe {
            let _ = CString::from_raw(error_message);
        }
    }
}

/// No-op function for measuring pure CGO overhead
#[no_mangle]
pub extern "C" fn noop() -> c_int {
    0
}

/// Expression stack machine functions
#[no_mangle]
pub extern "C" fn expr_column(stack: *mut Vec<Expr>, args: usize) -> c_int {
    if stack.is_null() || args == 0 {
        return -1;
    }

    let stack = unsafe { &mut *stack };
    let args = unsafe { &*(args as *const ColumnArgs) };

    let name = match unsafe { args.name.as_str() } {
        Ok(s) => s,
        Err(_) => return -1, // Invalid UTF-8
    };

    stack.push(col(name));
    0 // Success
}

#[no_mangle]
pub extern "C" fn expr_literal(stack: *mut Vec<Expr>, args: usize) -> c_int {
    if stack.is_null() || args == 0 {
        return -1;
    }

    let stack = unsafe { &mut *stack };
    let args = unsafe { &*(args as *const LiteralArgs) };

    let expr = match args.literal.to_expr() {
        Ok(e) => e,
        Err(_) => return -1, // Invalid literal
    };

    stack.push(expr);
    0 // Success
}

#[no_mangle]
pub extern "C" fn expr_gt(stack: *mut Vec<Expr>, _args: usize) -> c_int {
    if stack.is_null() {
        return -1;
    }

    let stack = unsafe { &mut *stack };

    if stack.len() < 2 {
        return -1; // Not enough operands
    }

    let right = stack.pop().unwrap();
    let left = stack.pop().unwrap();
    stack.push(left.gt(right));
    0 // Success
}

#[no_mangle]
pub extern "C" fn expr_lt(stack: *mut Vec<Expr>, _args: usize) -> c_int {
    if stack.is_null() {
        return -1;
    }

    let stack = unsafe { &mut *stack };

    if stack.len() < 2 {
        return -1; // Not enough operands
    }

    let right = stack.pop().unwrap();
    let left = stack.pop().unwrap();
    stack.push(left.lt(right));
    0 // Success
}

#[no_mangle]
pub extern "C" fn expr_eq(stack: *mut Vec<Expr>, _args: usize) -> c_int {
    if stack.is_null() {
        return -1;
    }

    let stack = unsafe { &mut *stack };

    if stack.len() < 2 {
        return -1; // Not enough operands
    }

    let right = stack.pop().unwrap();
    let left = stack.pop().unwrap();
    stack.push(left.eq(right));
    0 // Success
}

/// Execute a sequence of expression operations to build a single Expr
pub fn execute_expr_ops(ops: &[ExprOp]) -> std::result::Result<Expr, &'static str> {
    let mut stack = Vec::new();

    for op in ops {
        let func: extern "C" fn(*mut Vec<Expr>, usize) -> c_int =
            unsafe { std::mem::transmute(op.func_ptr) };

        let result = func(&mut stack, op.args);
        if result != 0 {
            return Err("Expression operation failed");
        }
    }

    if stack.len() != 1 {
        return Err("Invalid expression - stack should have exactly one element");
    }

    Ok(stack.pop().unwrap())
}

/// Convert DataFrame to CSV string
#[no_mangle]
pub extern "C" fn dataframe_to_csv(handle: usize) -> *mut c_char {
    if handle == 0 {
        return ptr::null_mut();
    }

    let df = unsafe { &*(handle as *const DataFrame) };

    // Use Polars' CSV writer to convert to string
    let mut buf = Vec::new();
    let mut df_clone = df.clone(); // Need mutable reference
    match CsvWriter::new(&mut buf).finish(&mut df_clone) {
        Ok(_) => {
            // Convert Vec<u8> to C string
            match String::from_utf8(buf) {
                Ok(csv_string) => match CString::new(csv_string) {
                    Ok(c_string) => c_string.into_raw(),
                    Err(_) => ptr::null_mut(),
                },
                Err(_) => ptr::null_mut(),
            }
        }
        Err(_) => ptr::null_mut(),
    }
}

/// Get DataFrame display string (similar to Polars' native display)
#[no_mangle]
pub extern "C" fn dataframe_to_string(handle: usize) -> *mut c_char {
    if handle == 0 {
        return ptr::null_mut();
    }

    let df = unsafe { &*(handle as *const DataFrame) };

    // Use Polars' Display trait to get formatted output
    let display_string = format!("{}", df);

    match CString::new(display_string) {
        Ok(c_string) => c_string.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

/// Dispatch function for filter with expression
#[no_mangle]
pub extern "C" fn dispatch_filter_expr(handle: usize, args: usize) -> Result {
    if handle == 0 {
        return Result::error(ERROR_NULL_HANDLE, "DataFrame handle cannot be null");
    }
    if args == 0 {
        return Result::error(ERROR_NULL_ARGS, "Filter expression args cannot be null");
    }

    let df = unsafe { &*(handle as *const DataFrame) };
    let args = unsafe { &*(args as *const FilterExprArgs) };

    // Convert ExprOp array to slice
    if args.expr_ops.is_null() || args.expr_count == 0 {
        return Result::error(
            ERROR_NULL_ARGS,
            "Expression operations cannot be null or empty",
        );
    }

    let expr_ops = unsafe { std::slice::from_raw_parts(args.expr_ops, args.expr_count) };

    // Execute expression operations to build the filter expression
    let filter_expr = match execute_expr_ops(expr_ops) {
        Ok(expr) => expr,
        Err(msg) => return Result::error(ERROR_POLARS_OPERATION, msg),
    };

    // Apply the filter to the DataFrame
    match df.clone().lazy().filter(filter_expr).collect() {
        Ok(new_df) => Result::success(new_df),
        Err(e) => Result::error(ERROR_POLARS_OPERATION, &e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_new_dataframe() {
        let result = dispatch_new_empty(0, 0); // handle and args not used for new_empty
        assert_eq!(result.error_code, 0);
        assert!(result.handle > 0);

        // Clean up
        assert_eq!(release(result.handle), 0);
    }

    #[test]
    fn test_read_csv() {
        // Create a proper temporary file
        let mut temp_file = std::env::temp_dir();
        temp_file.push("test_turbo_polars.csv");

        let csv_content = "name,age,salary\nAlice,25,50000\nBob,30,60000\n";
        fs::write(&temp_file, csv_content).unwrap();

        // Test reading CSV using new dispatch function
        let path_str = temp_file.to_str().unwrap();
        let args = ReadCsvArgs {
            path: RawStr {
                data: path_str.as_ptr() as *const c_char,
                len: path_str.len(),
            },
        };
        let result = dispatch_read_csv(0, &args as *const ReadCsvArgs as usize);

        assert_eq!(result.error_code, 0);
        assert!(result.handle > 0);

        // Clean up
        assert_eq!(release(result.handle), 0);
        fs::remove_file(&temp_file).unwrap();
    }

    #[test]
    fn test_invalid_handle() {
        assert_eq!(release(0), 1); // null handle should fail
    }

    #[test]
    fn test_execute_operations_with_select() {
        // Create a proper temporary file
        let mut temp_file = std::env::temp_dir();
        temp_file.push("test_turbo_polars_select.csv");

        let csv_content = "name,age,salary\nAlice,25,50000\nBob,30,60000\n";
        fs::write(&temp_file, csv_content).unwrap();

        // Create operations: read_csv then select
        let path_str = temp_file.to_str().unwrap();
        let read_args = ReadCsvArgs {
            path: RawStr {
                data: path_str.as_ptr() as *const c_char,
                len: path_str.len(),
            },
        };

        let col_name = "name";
        let col_raw_str = RawStr {
            data: col_name.as_ptr() as *const c_char,
            len: col_name.len(),
        };
        let col_ptrs = [col_raw_str];
        let select_args = SelectArgs {
            columns: col_ptrs.as_ptr(),
            column_count: 1,
        };

        let operations = [
            Operation {
                func_ptr: dispatch_read_csv as usize,
                args: &read_args as *const ReadCsvArgs as usize,
            },
            Operation {
                func_ptr: dispatch_select as usize,
                args: &select_args as *const SelectArgs as usize,
            },
        ];

        // Execute the operation chain
        let result = execute_operations(0, operations.as_ptr(), operations.len());

        assert_eq!(result.error_code, 0);
        assert!(result.handle > 0);

        // Clean up
        assert_eq!(release(result.handle), 0);
        fs::remove_file(&temp_file).unwrap();
    }

    #[test]
    fn test_expr_stack_machine() {
        // Test building Col("age").Gt(Lit(25))

        // Create column args
        let col_name = "age";
        let col_raw_str = RawStr {
            data: col_name.as_ptr() as *const c_char,
            len: col_name.len(),
        };
        let col_args = ColumnArgs { name: col_raw_str };

        // Create literal args
        let literal = Literal {
            value_type: 0, // int
            int_value: 25,
            float_value: 0.0,
            string_value: RawStr {
                data: std::ptr::null(),
                len: 0,
            },
            bool_value: false,
        };
        let lit_args = LiteralArgs { literal };

        // Create expression operations: Col("age"), Lit(25), Gt
        let expr_ops = [
            ExprOp {
                func_ptr: expr_column as usize,
                args: &col_args as *const ColumnArgs as usize,
            },
            ExprOp {
                func_ptr: expr_literal as usize,
                args: &lit_args as *const LiteralArgs as usize,
            },
            ExprOp {
                func_ptr: expr_gt as usize,
                args: 0, // No args for binary operations
            },
        ];

        // Execute the expression operations
        let result = execute_expr_ops(&expr_ops);
        assert!(result.is_ok(), "Expression execution should succeed");

        let _expr = result.unwrap();
        // We can't easily test the actual expression content, but we can verify it was created
        // In a real scenario, this would be used with a DataFrame
    }
}
