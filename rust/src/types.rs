use crate::{FfiResult, RawStr, ERROR_POLARS_OPERATION};
use polars::prelude::*;

/// Column value for FromMemory operation
#[repr(C)]
pub struct ColumnValue {
    pub value_type: u8, // 0=int64, 1=float64, 2=string, 3=bool, 4=null
    pub int_value: i64,
    pub float_value: f64,
    pub string_value: RawStr,
    pub bool_value: bool,
}

/// Column data for FromMemory operation
#[repr(C)]
pub struct ColumnData {
    pub name: RawStr,
    pub values: *const ColumnValue,
    pub len: usize,
}

/// Arguments for FromMemory operation
#[repr(C)]
pub struct FromMemoryArgs {
    pub columns: *const ColumnData,
    pub column_count: usize,
}

/// Arguments for column reference operations
#[repr(C)]
pub struct ColumnArgs {
    pub name: RawStr, // Column name
}

/// Arguments for literal operations
#[repr(C)]
pub struct LiteralArgs {
    pub literal: Literal, // The literal value
}

/// Arguments for alias operations
#[repr(C)]
pub struct AliasArgs {
    pub name: RawStr, // Column alias name
}

/// Arguments for string operations that take a pattern/string parameter
#[repr(C)]
pub struct StringArgs {
    pub pattern: RawStr, // Pattern/string for operations like contains, starts_with, ends_with
}

/// Arguments for string slice operations
#[repr(C)]
pub struct SliceArgs {
    pub start: i64,   // Start offset (can be negative)
    pub length: i64,  // Length; if negative, slice to end
}

/// Arguments for string replace operations
#[repr(C)]
pub struct ReplaceArgs {
    pub pattern: RawStr,     // Pattern to search
    pub replacement: RawStr, // Replacement string
    pub literal: bool,       // If true, treat pattern as literal
    pub n: i64,              // Max replacements; negative = unlimited
}

/// Arguments for string split operations
#[repr(C)]
pub struct SplitArgs {
    pub delimiter: RawStr, // Delimiter to split on
    pub n: i64,            // Max splits; negative = unlimited
}

/// Arguments for head/tail operations
#[repr(C)]
pub struct HeadTailArgs {
    pub n: i64, // Number of characters
}

/// Arguments for pad operations
#[repr(C)]
pub struct PadArgs {
    pub length: i64,    // Target length
    pub fill_char: u8,  // Fill character (single byte)
}

/// Arguments for aggregation operations that need ddof (std, var)
#[repr(C)]
pub struct AggregationArgs {
    pub ddof: u8, // Delta degrees of freedom (0=population, 1=sample)
}

/// Arguments for count operations
#[repr(C)]
pub struct CountArgs {
    pub include_nulls: bool, // Whether to include null values in count
}

/// Arguments for cast operations
#[repr(C)]
pub struct CastArgs {
    pub dtype: u32,          // Target data type (bit-packed encoding)
    pub strict: bool,        // If true, raise error on invalid cast; if false, produce null
    pub wrap_numerical: bool, // If true, wrap overflowing numeric values instead of marking invalid
}

/// Centralized literal abstraction - C-compatible struct for various literal values
#[repr(C)]
pub struct Literal {
    pub value_type: u8, // 0=int, 1=float, 2=string, 3=bool
    pub int_value: i64,
    pub float_value: f64,
    pub string_value: RawStr,
    pub bool_value: bool,
}

impl Literal {
    /// Convert Literal to Polars Expr
    pub fn to_expr(&self) -> std::result::Result<Expr, &'static str> {
        match self.value_type {
            0 => Ok(lit(self.int_value)),   // int
            1 => Ok(lit(self.float_value)), // float
            2 => {
                // string
                match unsafe { self.string_value.as_str() } {
                    Ok(s) => Ok(lit(s)),
                    Err(_) => Err("Invalid UTF-8 in string literal"),
                }
            }
            3 => Ok(lit(self.bool_value)), // bool
            _ => Err("Invalid literal type"),
        }
    }
}

/// Decode bit-packed data type from u32 to Polars DataType
pub fn decode_data_type(encoded: u32) -> Result<DataType, FfiResult> {
    // Extract type family (high 16 bits) and variant (low 16 bits)
    let family = (encoded >> 16) & 0xFFFF;
    let variant = encoded & 0xFFFF;
    
    match family {
        0x0000 => {
            // Integer family
            match variant {
                0x0001 => Ok(DataType::Int8),
                0x0002 => Ok(DataType::Int16),
                0x0003 => Ok(DataType::Int32),
                0x0004 => Ok(DataType::Int64),
                0x0005 => Ok(DataType::UInt8),
                0x0006 => Ok(DataType::UInt16),
                0x0007 => Ok(DataType::UInt32),
                0x0008 => Ok(DataType::UInt64),
                _ => Err(FfiResult::error(
                    ERROR_POLARS_OPERATION,
                    &format!("Unknown integer type variant: {}", variant),
                )),
            }
        }
        0x0001 => {
            // Float family
            match variant {
                0x0001 => Ok(DataType::Float32),
                0x0002 => Ok(DataType::Float64),
                _ => Err(FfiResult::error(
                    ERROR_POLARS_OPERATION,
                    &format!("Unknown float type variant: {}", variant),
                )),
            }
        }
        0x0002 => {
            // String family
            match variant {
                0x0001 => Ok(DataType::String),
                _ => Err(FfiResult::error(
                    ERROR_POLARS_OPERATION,
                    &format!("Unknown string type variant: {}", variant),
                )),
            }
        }
        0x0003 => {
            // Temporal family
            match variant {
                0x0001 => Ok(DataType::Date),
                0x0002 => Ok(DataType::Time),
                0x0003 => Ok(DataType::Datetime(TimeUnit::Nanoseconds, None)),
                0x0004 => Ok(DataType::Datetime(TimeUnit::Microseconds, None)),
                0x0005 => Ok(DataType::Datetime(TimeUnit::Milliseconds, None)),
                // Note: Polars doesn't have TimeUnit::Seconds, using Milliseconds as fallback
                0x0006 => Ok(DataType::Datetime(TimeUnit::Milliseconds, None)),
                _ => Err(FfiResult::error(
                    ERROR_POLARS_OPERATION,
                    &format!("Unknown temporal type variant: {}", variant),
                )),
            }
        }
        0x0004 => {
            // Boolean family
            match variant {
                0x0001 => Ok(DataType::Boolean),
                _ => Err(FfiResult::error(
                    ERROR_POLARS_OPERATION,
                    &format!("Unknown boolean type variant: {}", variant),
                )),
            }
        }
        _ => Err(FfiResult::error(
            ERROR_POLARS_OPERATION,
            &format!("Unknown data type family: {}", family),
        )),
    }
}
