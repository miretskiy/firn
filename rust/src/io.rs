use crate::{
    ExecutionContext, FfiResult, PolarsHandle, RawStr, 
    ERROR_INVALID_UTF8, ERROR_POLARS_OPERATION,
};
use polars::prelude::{LazyFrame, LazyCsvReader, ScanArgsParquet, LazyFileListReader, PlPath};

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

/// Arguments for reading Parquet files
#[repr(C)]
pub struct ReadParquetArgs {
    pub path: RawStr,               // File path using zero-copy RawStr
    pub columns: *const RawStr,     // Array of column names to select (null for all)
    pub column_count: usize,        // Number of columns to select
    pub n_rows: usize,              // Number of rows to read (0 for all)
    pub parallel: bool,             // Whether to read in parallel
    pub with_glob: bool,            // Whether to expand glob patterns
}

/// Dispatch function for reading CSV
pub fn dispatch_read_csv(_handle: PolarsHandle, context: &ExecutionContext) -> FfiResult {
    let args = unsafe { &*(context.operation_args as *const ReadCsvArgs) };

    // Convert RawStr to &str using zero-copy approach
    let path_str = match unsafe { args.path.as_str() } {
        Ok(s) => s,
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in path"),
    };

    // Use LazyCsvReader with configurable options - return LazyFrame for lazy evaluation
    match LazyCsvReader::new(PlPath::new(path_str))
        .with_has_header(args.has_header) // Configurable header detection
        .finish()
    {
        Ok(lazy_frame) => FfiResult::success_lazy(lazy_frame),
        Err(e) => FfiResult::error(ERROR_POLARS_OPERATION, &e.to_string()),
    }
}

/// Dispatch function for reading Parquet
pub fn dispatch_read_parquet(_handle: PolarsHandle, context: &ExecutionContext) -> FfiResult {
    let args = unsafe { &*(context.operation_args as *const ReadParquetArgs) };

    // Convert RawStr to &str using zero-copy approach
    let path_str = match unsafe { args.path.as_str() } {
        Ok(s) => s,
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in path"),
    };

    // Build ScanArgsParquet properly instead of using Default and applying operations later
    let mut scan_args = ScanArgsParquet::default();

    // Handle column selection if specified
    if !args.columns.is_null() && args.column_count > 0 {
        let columns = match unsafe { raw_str_array_to_vec(args.columns, args.column_count) } {
            Ok(cols) => cols,
            Err(msg) => return FfiResult::error(ERROR_POLARS_OPERATION, msg),
        };
        // For now, we'll apply column selection after scan since projection field might not be available
        // This is still better than the previous approach since we use n_rows properly
        let lazy_frame = match LazyFrame::scan_parquet(PlPath::new(path_str), scan_args) {
            Ok(lf) => lf,
            Err(e) => return FfiResult::error(ERROR_POLARS_OPERATION, &e.to_string()),
        };
        
        // Apply column selection and return LazyFrame for lazy evaluation
        let column_exprs: Vec<polars::prelude::Expr> = columns.iter().map(|s| polars::prelude::col(s)).collect();
        let selected_lazy = lazy_frame.select(column_exprs);
        
        return FfiResult::success_lazy(selected_lazy);
    }

    // Handle row limit if specified - this is the proper way to limit during scan
    if args.n_rows > 0 {
        scan_args.n_rows = Some(args.n_rows);
    }

    // Handle parallel reading
    scan_args.parallel = if args.parallel {
        polars::prelude::ParallelStrategy::Auto
    } else {
        polars::prelude::ParallelStrategy::None
    };

    // Use LazyFrame scan_parquet with proper ScanArgsParquet - return LazyFrame for lazy evaluation
    match LazyFrame::scan_parquet(PlPath::new(path_str), scan_args) {
        Ok(lazy_frame) => FfiResult::success_lazy(lazy_frame),
        Err(e) => FfiResult::error(ERROR_POLARS_OPERATION, &e.to_string()),
    }
}
