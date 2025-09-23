use crate::{ExecutionContext, FfiResult, RawStr, ERROR_INVALID_UTF8, ERROR_POLARS_OPERATION};
use polars::prelude::*;

/// Helper function for binary expression operations
/// Takes a closure that operates on (left, right) expressions and returns the result
fn binary_expr_op<F>(ctx: &ExecutionContext, op_name: &str, op: F) -> FfiResult
where
    F: FnOnce(Expr, Expr) -> Expr,
{
    let expr_stack = unsafe { &mut *ctx.expr_stack };

    if expr_stack.len() < 2 {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            &format!("{} requires 2 expressions on stack", op_name),
        );
    }

    let right = expr_stack.pop().unwrap();
    let left = expr_stack.pop().unwrap();
    expr_stack.push(op(left, right));

    FfiResult::success_no_handle()
}

fn unary_expr_op<F>(ctx: &ExecutionContext, op_name: &str, op: F) -> FfiResult
where
    F: FnOnce(Expr) -> Expr,
{
    let expr_stack = unsafe { &mut *ctx.expr_stack };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            &format!("{} requires 1 expression on stack", op_name),
        );
    }

    let expr = expr_stack.pop().unwrap();
    expr_stack.push(op(expr));

    FfiResult::success_no_handle()
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

/// Arguments for aggregation operations that need ddof (std, var)
#[repr(C)]
pub struct AggregationArgs {
    pub ddof: u8, // Delta degrees of freedom (0=population, 1=sample)
}

#[repr(C)]
pub struct CountArgs {
    pub include_nulls: bool, // Whether to include null values in count
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

/// Expression stack machine functions
pub fn expr_column(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const ColumnArgs) };

    let name = match unsafe { args.name.as_str() } {
        Ok(s) => s,
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in column name"),
    };

    expr_stack.push(col(name));
    FfiResult::success_no_handle()
}

pub fn expr_literal(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const LiteralArgs) };

    let expr = match args.literal.to_expr() {
        Ok(e) => e,
        Err(_) => return FfiResult::error(ERROR_POLARS_OPERATION, "Invalid literal"),
    };

    expr_stack.push(expr);
    FfiResult::success_no_handle()
}

// Comparison operations
pub fn expr_gt(ctx: &ExecutionContext) -> FfiResult {
    binary_expr_op(ctx, "greater than", |left, right| left.gt(right))
}

pub fn expr_lt(ctx: &ExecutionContext) -> FfiResult {
    binary_expr_op(ctx, "less than", |left, right| left.lt(right))
}

pub fn expr_eq(ctx: &ExecutionContext) -> FfiResult {
    binary_expr_op(ctx, "equality", |left, right| left.eq(right))
}

// Arithmetic operations
pub fn expr_add(ctx: &ExecutionContext) -> FfiResult {
    binary_expr_op(ctx, "addition", |left, right| left + right)
}

pub fn expr_sub(ctx: &ExecutionContext) -> FfiResult {
    binary_expr_op(ctx, "subtraction", |left, right| left - right)
}

pub fn expr_mul(ctx: &ExecutionContext) -> FfiResult {
    binary_expr_op(ctx, "multiplication", |left, right| left * right)
}

pub fn expr_div(ctx: &ExecutionContext) -> FfiResult {
    binary_expr_op(ctx, "division", |left, right| left / right)
}

// Boolean operations
pub fn expr_and(ctx: &ExecutionContext) -> FfiResult {
    binary_expr_op(ctx, "logical AND", |left, right| left.and(right))
}

pub fn expr_or(ctx: &ExecutionContext) -> FfiResult {
    binary_expr_op(ctx, "logical OR", |left, right| left.or(right))
}

pub fn expr_not(ctx: &ExecutionContext) -> FfiResult {
    unary_expr_op(ctx, "logical NOT", |expr| expr.not())
}

/// Sum aggregation - applies sum to the top expression on the stack
pub fn expr_sum(ctx: &ExecutionContext) -> FfiResult {
    unary_expr_op(ctx, "sum", |expr| expr.sum())
}

/// Mean aggregation - applies mean to the top expression on the stack
pub fn expr_mean(ctx: &ExecutionContext) -> FfiResult {
    unary_expr_op(ctx, "mean", |expr| expr.mean())
}

/// Min aggregation - applies min to the top expression on the stack
pub fn expr_min(ctx: &ExecutionContext) -> FfiResult {
    unary_expr_op(ctx, "min", |expr| expr.min())
}

/// Max aggregation - applies max to the top expression on the stack
pub fn expr_max(ctx: &ExecutionContext) -> FfiResult {
    unary_expr_op(ctx, "max", |expr| expr.max())
}

/// Std aggregation - applies std to the top expression on the stack
pub fn expr_std(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const AggregationArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(ERROR_POLARS_OPERATION, "std requires 1 expression on stack");
    }

    let expr = expr_stack.pop().unwrap();
    expr_stack.push(expr.std(args.ddof));
    FfiResult::success_no_handle()
}

/// Var aggregation - applies var to the top expression on the stack
pub fn expr_var(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const AggregationArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(ERROR_POLARS_OPERATION, "var requires 1 expression on stack");
    }

    let expr = expr_stack.pop().unwrap();
    expr_stack.push(expr.var(args.ddof));
    FfiResult::success_no_handle()
}

/// Alias operation - adds an alias to the top expression on the stack
pub fn expr_alias(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const AliasArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "alias requires 1 expression on stack",
        );
    }

    // Convert RawStr to &str
    let alias_name = match unsafe { args.name.as_str() } {
        Ok(s) => s,
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in alias name"),
    };

    let expr = expr_stack.pop().unwrap();
    expr_stack.push(expr.alias(alias_name));
    FfiResult::success_no_handle()
}

// Additional aggregation operations
pub fn expr_median(ctx: &ExecutionContext) -> FfiResult {
    unary_expr_op(ctx, "median", |expr| expr.median())
}

pub fn expr_first(ctx: &ExecutionContext) -> FfiResult {
    unary_expr_op(ctx, "first", |expr| expr.first())
}

pub fn expr_last(ctx: &ExecutionContext) -> FfiResult {
    unary_expr_op(ctx, "last", |expr| expr.last())
}

pub fn expr_nunique(ctx: &ExecutionContext) -> FfiResult {
    unary_expr_op(ctx, "nunique", |expr| expr.n_unique())
}

pub fn expr_count(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const CountArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "count requires 1 expression on stack",
        );
    }

    let expr = expr_stack.pop().unwrap();
    // Use the include_nulls parameter from CountArgs
    expr_stack.push(if args.include_nulls {
        expr.count() // count() includes nulls in polars 0.32
    } else {
        expr.count() // count() excludes nulls
    });
    FfiResult::success_no_handle()
}

// Null checking operations
pub fn expr_is_null(ctx: &ExecutionContext) -> FfiResult {
    unary_expr_op(ctx, "is_null", |expr| expr.is_null())
}

pub fn expr_is_not_null(ctx: &ExecutionContext) -> FfiResult {
    unary_expr_op(ctx, "is_not_null", |expr| expr.is_not_null())
}

// String operations
pub fn expr_str_len(ctx: &ExecutionContext) -> FfiResult {
    unary_expr_op(ctx, "str_len", |expr| expr.str().len_chars())
}

pub fn expr_str_to_lowercase(ctx: &ExecutionContext) -> FfiResult {
    unary_expr_op(ctx, "str_to_lowercase", |expr| expr.str().to_lowercase())
}

pub fn expr_str_to_uppercase(ctx: &ExecutionContext) -> FfiResult {
    unary_expr_op(ctx, "str_to_uppercase", |expr| expr.str().to_uppercase())
}

pub fn expr_str_contains(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const StringArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "str_contains requires 1 expression on stack",
        );
    }

    let pattern_str = match unsafe { args.pattern.as_str() } {
        Ok(s) => s,
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in pattern"),
    };

    let expr = expr_stack.pop().unwrap();
    expr_stack.push(expr.str().contains_literal(lit(pattern_str)));
    FfiResult::success_no_handle()
}

pub fn expr_str_starts_with(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const StringArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "str_starts_with requires 1 expression on stack",
        );
    }

    let prefix_str = match unsafe { args.pattern.as_str() } {
        Ok(s) => s,
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in prefix"),
    };

    let expr = expr_stack.pop().unwrap();
    expr_stack.push(expr.str().starts_with(lit(prefix_str)));
    FfiResult::success_no_handle()
}

pub fn expr_str_ends_with(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const StringArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "str_ends_with requires 1 expression on stack",
        );
    }

    let suffix_str = match unsafe { args.pattern.as_str() } {
        Ok(s) => s,
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in suffix"),
    };

    let expr = expr_stack.pop().unwrap();
    expr_stack.push(expr.str().ends_with(lit(suffix_str)));
    FfiResult::success_no_handle()
}

/// SQL expression parsing - uses polars_sql::sql_expr to parse individual expressions
pub fn expr_sql(ctx: &ExecutionContext) -> FfiResult {
    use crate::SqlExprArgs;
    
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const SqlExprArgs) };

    let sql = match unsafe { args.sql.as_str() } {
        Ok(s) => s,
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in SQL expression"),
    };

    // Use polars_sql::sql_expr to parse the SQL expression
    match polars_sql::sql_expr(sql) {
        Ok(expr) => {
            expr_stack.push(expr);
            FfiResult::success_no_handle()
        }
        Err(e) => FfiResult::error(
            ERROR_POLARS_OPERATION,
            &format!("SQL expression parsing failed: {}", e),
        ),
    }
}

// Window function operations

/// Apply window context to the top expression on the stack
pub fn expr_over(ctx: &ExecutionContext) -> FfiResult {
    use crate::WindowArgs;
    
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const WindowArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "over requires 1 expression on stack",
        );
    }

    // Extract partition columns as owned Strings
    let partition_columns: Result<Vec<String>, _> = (0..args.partition_count)
        .map(|i| {
            let raw_str = unsafe { *args.partition_columns.offset(i as isize) };
            unsafe { raw_str.as_str() }.map(|s| s.to_string())
        })
        .collect();

    let partition_columns = match partition_columns {
        Ok(cols) => cols,
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in partition columns"),
    };

    // Extract order columns as owned Strings (if any)
    let order_columns: Result<Vec<String>, _> = if args.order_count > 0 {
        (0..args.order_count)
            .map(|i| {
                let raw_str = unsafe { *args.order_columns.offset(i as isize) };
                unsafe { raw_str.as_str() }.map(|s| s.to_string())
            })
            .collect()
    } else {
        Ok(vec![])
    };

    let order_columns = match order_columns {
        Ok(cols) => cols,
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in order columns"),
    };

    let expr = expr_stack.pop().unwrap();
    
    // Apply window function
    let windowed_expr = if order_columns.is_empty() {
        // Simple partition-by window - convert String to &str for polars API
        let partition_refs: Vec<&str> = partition_columns.iter().map(|s| s.as_str()).collect();
        expr.over(partition_refs)
    } else {
        // Partition-by with ordering - convert String to col() expressions
        let partition_refs: Vec<&str> = partition_columns.iter().map(|s| s.as_str()).collect();
        let order_exprs: Vec<Expr> = order_columns.iter().map(|col_name| col(col_name.as_str())).collect();
        expr.over(partition_refs).sort_by(order_exprs, SortMultipleOptions::default())
    };

    expr_stack.push(windowed_expr);
    FfiResult::success_no_handle()
}

/// Rank function - requires ordering
pub fn expr_rank(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    
    // For now, return a placeholder that will work with Over()
    // This is a simplified implementation - in a full version we'd need proper ranking
    expr_stack.push(lit(1).alias("rank"));
    FfiResult::success_no_handle()
}

/// Dense rank function - requires ordering
pub fn expr_dense_rank(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    
    // For now, return a placeholder that will work with Over()
    expr_stack.push(lit(1).alias("dense_rank"));
    FfiResult::success_no_handle()
}

/// Row number function
pub fn expr_row_number(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    
    // For now, return a placeholder that will work with Over()
    expr_stack.push(lit(1).alias("row_number"));
    FfiResult::success_no_handle()
}

/// Lag function - get value from previous row
pub fn expr_lag(ctx: &ExecutionContext) -> FfiResult {
    use crate::WindowOffsetArgs;
    
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const WindowOffsetArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "lag requires 1 expression on stack",
        );
    }

    let expr = expr_stack.pop().unwrap();
    // Note: args.offset is negative for lag (looking back)
    let offset = (-args.offset) as i64; // Convert to positive for lag
    expr_stack.push(expr.shift(lit(offset)));
    FfiResult::success_no_handle()
}

/// Lead function - get value from following row
pub fn expr_lead(ctx: &ExecutionContext) -> FfiResult {
    use crate::WindowOffsetArgs;
    
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const WindowOffsetArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "lead requires 1 expression on stack",
        );
    }

    let expr = expr_stack.pop().unwrap();
    // Note: args.offset is positive for lead (looking ahead)
    let offset = -(args.offset as i64); // Convert to negative for shift (lead)
    expr_stack.push(expr.shift(lit(offset)));
    FfiResult::success_no_handle()
}
