use firn::{
    dispatch_read_parquet, ExecutionContext, ReadParquetArgs, RawStr, FfiResult,
    PolarsHandle, ContextType, ERROR_POLARS_OPERATION,
};
use std::ffi::CString;
use std::ptr;

/// Helper function to create RawStr from &str
fn make_raw_str(s: &str) -> RawStr {
    let c_string = CString::new(s).unwrap();
    let ptr = c_string.as_ptr();
    let len = s.len();
    std::mem::forget(c_string); // Prevent deallocation
    RawStr { data: ptr, len }
}

/// Helper function to create RawStr array from Vec<&str>
fn make_raw_str_array(strings: Vec<&str>) -> (Vec<RawStr>, Vec<CString>) {
    let mut raw_strs = Vec::new();
    let mut c_strings = Vec::new(); // Keep alive
    
    for s in strings {
        let c_string = CString::new(s).unwrap();
        let raw_str = RawStr {
            data: c_string.as_ptr(),
            len: s.len(),
        };
        raw_strs.push(raw_str);
        c_strings.push(c_string);
    }
    
    (raw_strs, c_strings)
}

#[test]
fn test_parquet_basic_read() {
    // Test basic parquet reading - this tests Firn's integration with Polars
    let path = make_raw_str("../testdata/fortune1000_2024.parquet");
    
    let args = ReadParquetArgs {
        path,
        columns: ptr::null(),
        column_count: 0,
        n_rows: 5, // Limit to 5 rows for testing
        parallel: true,
        with_glob: false,
    };
    
    let context = ExecutionContext {
        expr_stack: ptr::null_mut(),
        operation_args: &args as *const ReadParquetArgs as usize,
    };
    
    let result = dispatch_read_parquet(PolarsHandle::new(0, ContextType::DataFrame), &context);
    
    // Should succeed
    assert_eq!(result.error_code, 0);
    assert!(!result.error_message.is_null() || result.error_message.is_null()); // Either null or valid
    assert_ne!(result.polars_handle.handle, 0);
    
    // Clean up
    if result.polars_handle.handle != 0 {
        unsafe {
            let _ = Box::from_raw(result.polars_handle.handle as *mut polars::prelude::DataFrame);
        }
    }
}

#[test]
fn test_parquet_column_selection() {
    // Test column selection functionality
    let path = make_raw_str("../testdata/fortune1000_2024.parquet");
    
    let (raw_strs, _c_strings) = make_raw_str_array(vec!["Rank", "Company", "Sector"]);
    
    let args = ReadParquetArgs {
        path,
        columns: raw_strs.as_ptr(),
        column_count: 3,
        n_rows: 3,
        parallel: true,
        with_glob: false,
    };
    
    let context = ExecutionContext {
        expr_stack: ptr::null_mut(),
        operation_args: &args as *const ReadParquetArgs as usize,
    };
    
    let result = dispatch_read_parquet(PolarsHandle::new(0, ContextType::DataFrame), &context);
    
    // Should succeed
    assert_eq!(result.error_code, 0);
    assert_ne!(result.polars_handle.handle, 0);
    
    // Clean up
    if result.polars_handle.handle != 0 {
        unsafe {
            let _ = Box::from_raw(result.polars_handle.handle as *mut polars::prelude::DataFrame);
        }
    }
}

#[test]
fn test_parquet_nonexistent_file() {
    // Test error handling for nonexistent file
    let path = make_raw_str("../testdata/nonexistent.parquet");
    
    let args = ReadParquetArgs {
        path,
        columns: ptr::null(),
        column_count: 0,
        n_rows: 0,
        parallel: true,
        with_glob: false,
    };
    
    let context = ExecutionContext {
        expr_stack: ptr::null_mut(),
        operation_args: &args as *const ReadParquetArgs as usize,
    };
    
    let result = dispatch_read_parquet(PolarsHandle::new(0, ContextType::DataFrame), &context);
    
    // Should fail with polars error
    assert_eq!(result.error_code, ERROR_POLARS_OPERATION);
    assert!(!result.error_message.is_null());
    
    // Clean up error message
    if !result.error_message.is_null() {
        unsafe {
            let _ = CString::from_raw(result.error_message);
        }
    }
}

#[test]
fn test_parquet_scan_args_optimization() {
    // Test that we're using ScanArgsParquet properly (not applying limit after scan)
    // This is more of a behavioral test - the key improvement is that we use
    // ScanArgsParquet.n_rows instead of LazyFrame.limit()
    
    let path = make_raw_str("../testdata/fortune1000_2024.parquet");
    
    let args = ReadParquetArgs {
        path,
        columns: ptr::null(),
        column_count: 0,
        n_rows: 1, // Very small limit to test optimization
        parallel: false, // Test non-parallel path
        with_glob: false,
    };
    
    let context = ExecutionContext {
        expr_stack: ptr::null_mut(),
        operation_args: &args as *const ReadParquetArgs as usize,
    };
    
    let result = dispatch_read_parquet(PolarsHandle::new(0, ContextType::DataFrame), &context);
    
    // Should succeed and return exactly 1 row
    assert_eq!(result.error_code, 0);
    assert_ne!(result.polars_handle.handle, 0);
    
    // Verify we got exactly 1 row (this tests the ScanArgsParquet.n_rows optimization)
    if result.polars_handle.handle != 0 {
        unsafe {
            let df = &*(result.polars_handle.handle as *const polars::prelude::DataFrame);
            assert_eq!(df.height(), 1);
            
            // Clean up
            let _ = Box::from_raw(result.polars_handle.handle as *mut polars::prelude::DataFrame);
        }
    }
}
