use polars::prelude::*;
use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::ptr;

// Module declarations
mod dataframe;
mod execution;
mod expr;
mod opcodes;

// Re-export public items
pub use dataframe::*;
pub use execution::{execute_expr_ops, execute_operations, ExecutionContext};
pub use expr::*;
pub use opcodes::*;

// Error codes
pub const ERROR_NULL_HANDLE: c_int = 1;
pub const ERROR_NULL_ARGS: c_int = 2;
pub const ERROR_INVALID_UTF8: c_int = 3;
pub const ERROR_POLARS_OPERATION: c_int = 4;

/// Zero-copy string representation for FFI
#[repr(C)]
pub struct RawStr {
    pub data: *const c_char, // Pointer to UTF-8 data
    pub len: usize,          // Length in bytes
}

impl RawStr {
    /// Convert RawStr to &str (zero-copy)
    /// # Safety
    /// The caller must ensure that:
    /// - `data` points to valid UTF-8 bytes
    /// - The data remains valid for the lifetime of the returned &str
    /// - `len` accurately represents the byte length
    pub unsafe fn as_str(&self) -> std::result::Result<&str, std::str::Utf8Error> {
        if self.data.is_null() || self.len == 0 {
            return Ok("");
        }
        let slice = std::slice::from_raw_parts(self.data as *const u8, self.len);
        std::str::from_utf8(slice)
    }
}

/// FFI result struct for DataFrame operations
#[repr(C)]
pub struct FfiResult {
    pub handle: usize,              // New DataFrame handle (0 if error)
    pub error_code: c_int,          // 0 = success, non-zero = error
    pub error_message: *mut c_char, // Error message (null if success)
    pub error_frame: usize,         // Frame index where error occurred
}

impl FfiResult {
    /// Create a successful result with a new DataFrame
    pub fn success(df: DataFrame) -> Self {
        let boxed_df = Box::new(df);
        let handle = Box::into_raw(boxed_df) as usize;
        Self {
            handle,
            error_code: 0,
            error_message: ptr::null_mut(),
            error_frame: 0,
        }
    }

    /// Create a successful result with a new LazyFrame
    pub fn success_lazy(lazy_frame: LazyFrame) -> Self {
        let boxed_lazy = Box::new(lazy_frame);
        let handle = Box::into_raw(boxed_lazy) as usize;
        Self {
            handle,
            error_code: 0,
            error_message: ptr::null_mut(),
            error_frame: 0,
        }
    }

    /// Create a successful result with a specific handle (for expression operations)
    pub fn success_with_handle(handle: usize) -> Self {
        Self {
            handle,
            error_code: 0,
            error_message: ptr::null_mut(),
            error_frame: 0,
        }
    }

    /// Create a successful result with no handle (for expression operations that don't return handles)
    pub fn success_no_handle() -> Self {
        Self {
            handle: 0,
            error_code: 0,
            error_message: ptr::null_mut(),
            error_frame: 0,
        }
    }

    /// Create an error result
    pub fn error(code: c_int, message: &str) -> Self {
        let c_message = match CString::new(message) {
            Ok(s) => s.into_raw(),
            Err(_) => ptr::null_mut(),
        };

        Self {
            handle: 0,
            error_code: code,
            error_message: c_message,
            error_frame: 0,
        }
    }
}

/// Helper function to convert RawStr array to Vec<String>
pub unsafe fn raw_str_array_to_vec(
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

/// Helper function to create RawStr from Go string data
/// This is used by Go code to create RawStr instances
#[no_mangle]
pub extern "C" fn make_raw_str(data: *const c_char, len: usize) -> RawStr {
    RawStr { data, len }
}
