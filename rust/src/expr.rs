use crate::{
    ExecutionContext, FfiResult, RawStr, ERROR_INVALID_UTF8, ERROR_NULL_ARGS,
    ERROR_POLARS_OPERATION,
};
use polars::prelude::*;

/// Helper function for binary expression operations
/// Takes a closure that operates on (left, right) expressions and returns the result
fn binary_expr_op<F>(handle: usize, context: usize, op_name: &str, op: F) -> FfiResult
where
    F: FnOnce(Expr, Expr) -> Expr,
{
    let expr_stack = match extract_context_no_args(context, 2, op_name) {
        Ok(stack) => stack,
        Err(error) => return error,
    };

    let right = expr_stack.pop().unwrap();
    let left = expr_stack.pop().unwrap();
    expr_stack.push(op(left, right));

    FfiResult::success_with_handle(handle)
}

fn unary_expr_op<F>(handle: usize, context: usize, op_name: &str, op: F) -> FfiResult
where
    F: FnOnce(Expr) -> Expr,
{
    let expr_stack = match extract_context_no_args(context, 1, op_name) {
        Ok(stack) => stack,
        Err(error) => return error,
    };

    let expr = expr_stack.pop().unwrap();
    expr_stack.push(op(expr));

    FfiResult::success_with_handle(handle)
}

/// Helper function for operations that need args extraction
/// Returns (expr_stack, args) ready to use
pub fn extract_context_with_args<'a, T>(
    context: usize,
    min_stack_size: usize,
    op_name: &str,
) -> Result<(&'a mut Vec<Expr>, &'a T), FfiResult> {
    if context == 0 {
        return Err(FfiResult::error(
            ERROR_NULL_ARGS,
            "ExecutionContext cannot be null",
        ));
    }

    let ctx = unsafe { &*(context as *const ExecutionContext) };
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const T) };

    if expr_stack.len() < min_stack_size {
        return Err(FfiResult::error(
            ERROR_POLARS_OPERATION,
            &format!(
                "Not enough operands for {}: need {}, have {}",
                op_name,
                min_stack_size,
                expr_stack.len()
            ),
        ));
    }

    Ok((expr_stack, args))
}

/// Helper function for operations that don't need args (expects args to be 0)
/// Returns expr_stack ready to use
fn extract_context_no_args<'a>(
    context: usize,
    min_stack_size: usize,
    op_name: &str,
) -> Result<&'a mut Vec<Expr>, FfiResult> {
    if context == 0 {
        return Err(FfiResult::error(
            ERROR_NULL_ARGS,
            "ExecutionContext cannot be null",
        ));
    }

    let ctx = unsafe { &*(context as *const ExecutionContext) };

    if ctx.operation_args != 0 {
        return Err(FfiResult::error(
            ERROR_POLARS_OPERATION,
            "Expected no args but args were provided",
        ));
    }

    let expr_stack = unsafe { &mut *ctx.expr_stack };

    if expr_stack.len() < min_stack_size {
        return Err(FfiResult::error(
            ERROR_POLARS_OPERATION,
            &format!(
                "Not enough operands for {}: need {}, have {}",
                op_name,
                min_stack_size,
                expr_stack.len()
            ),
        ));
    }

    Ok(expr_stack)
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
#[no_mangle]
pub extern "C" fn expr_column(handle: usize, context: usize) -> FfiResult {
    if context == 0 {
        return FfiResult::error(ERROR_NULL_ARGS, "ExecutionContext cannot be null");
    }

    let ctx = unsafe { &*(context as *const ExecutionContext) };
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const ColumnArgs) };

    let name = match unsafe { args.name.as_str() } {
        Ok(s) => s,
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in column name"),
    };

    expr_stack.push(col(name));
    FfiResult::success_with_handle(handle)
}

#[no_mangle]
pub extern "C" fn expr_literal(handle: usize, context: usize) -> FfiResult {
    if context == 0 {
        return FfiResult::error(ERROR_NULL_ARGS, "ExecutionContext cannot be null");
    }

    let ctx = unsafe { &*(context as *const ExecutionContext) };
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const LiteralArgs) };

    let expr = match args.literal.to_expr() {
        Ok(e) => e,
        Err(_) => return FfiResult::error(ERROR_POLARS_OPERATION, "Invalid literal"),
    };

    expr_stack.push(expr);
    FfiResult::success_with_handle(handle)
}

// Comparison operations
#[no_mangle]
pub extern "C" fn expr_gt(handle: usize, context: usize) -> FfiResult {
    binary_expr_op(handle, context, "greater than", |left, right| {
        left.gt(right)
    })
}

#[no_mangle]
pub extern "C" fn expr_lt(handle: usize, context: usize) -> FfiResult {
    binary_expr_op(handle, context, "less than", |left, right| left.lt(right))
}

#[no_mangle]
pub extern "C" fn expr_eq(handle: usize, context: usize) -> FfiResult {
    binary_expr_op(handle, context, "equality", |left, right| left.eq(right))
}

// Arithmetic operations
#[no_mangle]
pub extern "C" fn expr_add(handle: usize, context: usize) -> FfiResult {
    binary_expr_op(handle, context, "addition", |left, right| left + right)
}

#[no_mangle]
pub extern "C" fn expr_sub(handle: usize, context: usize) -> FfiResult {
    binary_expr_op(handle, context, "subtraction", |left, right| left - right)
}

#[no_mangle]
pub extern "C" fn expr_mul(handle: usize, context: usize) -> FfiResult {
    binary_expr_op(handle, context, "multiplication", |left, right| {
        left * right
    })
}

#[no_mangle]
pub extern "C" fn expr_div(handle: usize, context: usize) -> FfiResult {
    binary_expr_op(handle, context, "division", |left, right| left / right)
}

// Boolean operations
#[no_mangle]
pub extern "C" fn expr_and(handle: usize, context: usize) -> FfiResult {
    binary_expr_op(handle, context, "logical AND", |left, right| {
        left.and(right)
    })
}

#[no_mangle]
pub extern "C" fn expr_or(handle: usize, context: usize) -> FfiResult {
    binary_expr_op(handle, context, "logical OR", |left, right| left.or(right))
}

#[no_mangle]
pub extern "C" fn expr_not(handle: usize, context: usize) -> FfiResult {
    unary_expr_op(handle, context, "logical NOT", |expr| expr.not())
}

/// Sum aggregation - applies sum to the top expression on the stack
#[no_mangle]
pub extern "C" fn expr_sum(handle: usize, context: usize) -> FfiResult {
    unary_expr_op(handle, context, "sum", |expr| expr.sum())
}

/// Mean aggregation - applies mean to the top expression on the stack
#[no_mangle]
pub extern "C" fn expr_mean(handle: usize, context: usize) -> FfiResult {
    unary_expr_op(handle, context, "mean", |expr| expr.mean())
}

/// Min aggregation - applies min to the top expression on the stack
#[no_mangle]
pub extern "C" fn expr_min(handle: usize, context: usize) -> FfiResult {
    unary_expr_op(handle, context, "min", |expr| expr.min())
}

/// Max aggregation - applies max to the top expression on the stack
#[no_mangle]
pub extern "C" fn expr_max(handle: usize, context: usize) -> FfiResult {
    unary_expr_op(handle, context, "max", |expr| expr.max())
}

/// Std aggregation - applies std to the top expression on the stack
#[no_mangle]
pub extern "C" fn expr_std(handle: usize, context: usize) -> FfiResult {
    let (expr_stack, args) = match extract_context_with_args::<AggregationArgs>(context, 1, "std") {
        Ok(result) => result,
        Err(error) => return error,
    };

    let expr = expr_stack.pop().unwrap();
    expr_stack.push(expr.std(args.ddof));
    FfiResult::success_with_handle(handle)
}

/// Var aggregation - applies var to the top expression on the stack
#[no_mangle]
pub extern "C" fn expr_var(handle: usize, context: usize) -> FfiResult {
    let (expr_stack, args) = match extract_context_with_args::<AggregationArgs>(context, 1, "var") {
        Ok(result) => result,
        Err(error) => return error,
    };

    let expr = expr_stack.pop().unwrap();
    expr_stack.push(expr.var(args.ddof));
    FfiResult::success_with_handle(handle)
}

/// Alias operation - adds an alias to the top expression on the stack
#[no_mangle]
pub extern "C" fn expr_alias(handle: usize, context: usize) -> FfiResult {
    let (expr_stack, args) = match extract_context_with_args::<AliasArgs>(context, 1, "alias") {
        Ok(result) => result,
        Err(error) => return error,
    };

    // Convert RawStr to &str
    let alias_name = match unsafe { args.name.as_str() } {
        Ok(s) => s,
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in alias name"),
    };

    let expr = expr_stack.pop().unwrap();
    expr_stack.push(expr.alias(alias_name));
    FfiResult::success_with_handle(handle)
}

// Additional aggregation operations
#[no_mangle]
pub extern "C" fn expr_median(handle: usize, context: usize) -> FfiResult {
    unary_expr_op(handle, context, "median", |expr| expr.median())
}

#[no_mangle]
pub extern "C" fn expr_first(handle: usize, context: usize) -> FfiResult {
    unary_expr_op(handle, context, "first", |expr| expr.first())
}

#[no_mangle]
pub extern "C" fn expr_last(handle: usize, context: usize) -> FfiResult {
    unary_expr_op(handle, context, "last", |expr| expr.last())
}

#[no_mangle]
pub extern "C" fn expr_nunique(handle: usize, context: usize) -> FfiResult {
    unary_expr_op(handle, context, "nunique", |expr| expr.n_unique())
}

#[no_mangle]
pub extern "C" fn expr_count(handle: usize, context: usize) -> FfiResult {
    let (expr_stack, args) = match extract_context_with_args::<CountArgs>(context, 1, "count") {
        Ok(result) => result,
        Err(error) => return error,
    };

    let expr = expr_stack.pop().unwrap();
    // Use the include_nulls parameter from CountArgs
    expr_stack.push(if args.include_nulls {
        expr.len() // len() includes nulls
    } else {
        expr.count() // count() excludes nulls
    });
    FfiResult::success_with_handle(handle)
}

// Null checking operations
#[no_mangle]
pub extern "C" fn expr_is_null(handle: usize, context: usize) -> FfiResult {
    unary_expr_op(handle, context, "is_null", |expr| expr.is_null())
}

#[no_mangle]
pub extern "C" fn expr_is_not_null(handle: usize, context: usize) -> FfiResult {
    unary_expr_op(handle, context, "is_not_null", |expr| expr.is_not_null())
}

// String operations
#[no_mangle]
pub extern "C" fn expr_str_len(handle: usize, context: usize) -> FfiResult {
    unary_expr_op(handle, context, "str_len", |expr| expr.str().len_chars())
}

#[no_mangle]
pub extern "C" fn expr_str_to_lowercase(handle: usize, context: usize) -> FfiResult {
    unary_expr_op(handle, context, "str_to_lowercase", |expr| {
        expr.str().to_lowercase()
    })
}

#[no_mangle]
pub extern "C" fn expr_str_to_uppercase(handle: usize, context: usize) -> FfiResult {
    unary_expr_op(handle, context, "str_to_uppercase", |expr| {
        expr.str().to_uppercase()
    })
}

#[no_mangle]
pub extern "C" fn expr_str_contains(handle: usize, context: usize) -> FfiResult {
    let (expr_stack, args) =
        match extract_context_with_args::<StringArgs>(context, 1, "str_contains") {
            Ok(result) => result,
            Err(error) => return error,
        };

    let pattern_str = match unsafe { args.pattern.as_str() } {
        Ok(s) => s,
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in pattern"),
    };

    let expr = expr_stack.pop().unwrap();
    expr_stack.push(expr.str().contains_literal(lit(pattern_str)));
    FfiResult::success_with_handle(handle)
}

#[no_mangle]
pub extern "C" fn expr_str_starts_with(handle: usize, context: usize) -> FfiResult {
    let (expr_stack, args) =
        match extract_context_with_args::<StringArgs>(context, 1, "str_starts_with") {
            Ok(result) => result,
            Err(error) => return error,
        };

    let prefix_str = match unsafe { args.pattern.as_str() } {
        Ok(s) => s,
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in prefix"),
    };

    let expr = expr_stack.pop().unwrap();
    expr_stack.push(expr.str().starts_with(lit(prefix_str)));
    FfiResult::success_with_handle(handle)
}

#[no_mangle]
pub extern "C" fn expr_str_ends_with(handle: usize, context: usize) -> FfiResult {
    let (expr_stack, args) =
        match extract_context_with_args::<StringArgs>(context, 1, "str_ends_with") {
            Ok(result) => result,
            Err(error) => return error,
        };

    let suffix_str = match unsafe { args.pattern.as_str() } {
        Ok(s) => s,
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in suffix"),
    };

    let expr = expr_stack.pop().unwrap();
    expr_stack.push(expr.str().ends_with(lit(suffix_str)));
    FfiResult::success_with_handle(handle)
}
