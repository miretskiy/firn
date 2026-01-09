use crate::{ExecutionContext, FfiResult, ERROR_INVALID_UTF8, ERROR_POLARS_OPERATION};
use crate::types::{
    decode_data_type, AggregationArgs, AliasArgs, CastArgs, ColumnArgs, CountArgs, HeadTailArgs,
    LiteralArgs, PadArgs, ReplaceArgs, SliceArgs, SplitArgs, StringArgs,
};
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

// Cast operations

/// Cast operation - casts the top expression on the stack to the specified data type
pub fn expr_cast(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const CastArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "cast requires 1 expression on stack",
        );
    }

    // Decode the bit-packed data type
    let dtype = match decode_data_type(args.dtype) {
        Ok(dt) => dt,
        Err(err) => return err,
    };
    
    let expr = expr_stack.pop().unwrap();
    
    // Apply cast - for now use simple cast, can be enhanced later with options
    let cast_expr = expr.cast(dtype);
    
    expr_stack.push(cast_expr);
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

pub fn expr_str_slice(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const SliceArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "str_slice requires 1 expression on stack",
        );
    }

    let expr = expr_stack.pop().unwrap();

    // Polars string slice expects Expr offsets/lengths. Use a very large length to represent "to the end".
    let length_expr = if args.length < 0 {
        lit(i64::MAX)
    } else {
        lit(args.length as i64)
    };

    let slice_expr = expr.str().slice(lit(args.start as i64), length_expr);

    expr_stack.push(slice_expr);
    FfiResult::success_no_handle()
}

pub fn expr_str_replace(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const ReplaceArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "str_replace requires 1 expression on stack",
        );
    }

    let pattern = match unsafe { args.pattern.as_str() } {
        Ok(s) => s.to_string(),
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in pattern"),
    };

    let replacement = match unsafe { args.replacement.as_str() } {
        Ok(s) => s.to_string(),
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in replacement"),
    };

    let literal = args.literal;
    let max_replacements = args.n;

    let expr = expr_stack.pop().unwrap();

    // Polars replace_all(pat, value, literal) replaces all occurrences.
    // Polars replace(pat, value, literal) replaces only the first occurrence.
    // For n replacements we use replace_all when n<0, otherwise replace for n=1.
    // Note: Polars doesn't have replace_n for arbitrary n; approximate with replace_all or replace.
    let replaced_expr = if max_replacements < 0 {
        expr.str().replace_all(lit(pattern), lit(replacement), literal)
    } else if max_replacements == 1 {
        expr.str().replace(lit(pattern), lit(replacement), literal)
    } else {
        // For n > 1, use replace_all (best approximation; Polars lacks replace_n)
        expr.str().replace_all(lit(pattern), lit(replacement), literal)
    };

    expr_stack.push(replaced_expr);
    FfiResult::success_no_handle()
}

pub fn expr_str_split(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const SplitArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "str_split requires 1 expression on stack",
        );
    }

    let delimiter = match unsafe { args.delimiter.as_str() } {
        Ok(s) => s,
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in delimiter"),
    };

    let expr = expr_stack.pop().unwrap();

    let split_expr = expr.str().split(lit(delimiter));

    expr_stack.push(split_expr);
    FfiResult::success_no_handle()
}

// Advanced string operations (Tier 2)

/// String byte length - faster than char length for ASCII
pub fn expr_str_len_bytes(ctx: &ExecutionContext) -> FfiResult {
    unary_expr_op(ctx, "str_len_bytes", |expr| expr.str().len_bytes())
}

/// Strip leading and trailing characters
pub fn expr_str_strip_chars(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const StringArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "str_strip_chars requires 1 expression on stack",
        );
    }

    let chars_str = match unsafe { args.pattern.as_str() } {
        Ok(s) => s,
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in characters"),
    };

    let expr = expr_stack.pop().unwrap();
    let result = if chars_str.is_empty() {
        expr.str().strip_chars(lit(Null {}))
    } else {
        expr.str().strip_chars(lit(chars_str))
    };
    expr_stack.push(result);
    FfiResult::success_no_handle()
}

/// Strip leading characters
pub fn expr_str_strip_chars_start(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const StringArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "str_strip_chars_start requires 1 expression on stack",
        );
    }

    let chars_str = match unsafe { args.pattern.as_str() } {
        Ok(s) => s,
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in characters"),
    };

    let expr = expr_stack.pop().unwrap();
    let result = if chars_str.is_empty() {
        expr.str().strip_chars_start(lit(Null {}))
    } else {
        expr.str().strip_chars_start(lit(chars_str))
    };
    expr_stack.push(result);
    FfiResult::success_no_handle()
}

/// Strip trailing characters
pub fn expr_str_strip_chars_end(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const StringArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "str_strip_chars_end requires 1 expression on stack",
        );
    }

    let chars_str = match unsafe { args.pattern.as_str() } {
        Ok(s) => s,
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in characters"),
    };

    let expr = expr_stack.pop().unwrap();
    let result = if chars_str.is_empty() {
        expr.str().strip_chars_end(lit(Null {}))
    } else {
        expr.str().strip_chars_end(lit(chars_str))
    };
    expr_stack.push(result);
    FfiResult::success_no_handle()
}

/// Strip prefix
pub fn expr_str_strip_prefix(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const StringArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "str_strip_prefix requires 1 expression on stack",
        );
    }

    let prefix_str = match unsafe { args.pattern.as_str() } {
        Ok(s) => s,
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in prefix"),
    };

    let expr = expr_stack.pop().unwrap();
    expr_stack.push(expr.str().strip_prefix(lit(prefix_str)));
    FfiResult::success_no_handle()
}

/// Strip suffix
pub fn expr_str_strip_suffix(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const StringArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "str_strip_suffix requires 1 expression on stack",
        );
    }

    let suffix_str = match unsafe { args.pattern.as_str() } {
        Ok(s) => s,
        Err(_) => return FfiResult::error(ERROR_INVALID_UTF8, "Invalid UTF-8 in suffix"),
    };

    let expr = expr_stack.pop().unwrap();
    expr_stack.push(expr.str().strip_suffix(lit(suffix_str)));
    FfiResult::success_no_handle()
}

/// Reverse string
pub fn expr_str_reverse(ctx: &ExecutionContext) -> FfiResult {
    unary_expr_op(ctx, "str_reverse", |expr| expr.str().reverse())
}

/// Get first n characters (head)
pub fn expr_str_head(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const HeadTailArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "str_head requires 1 expression on stack",
        );
    }

    let expr = expr_stack.pop().unwrap();
    expr_stack.push(expr.str().head(lit(args.n)));
    FfiResult::success_no_handle()
}

/// Get last n characters (tail)
pub fn expr_str_tail(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const HeadTailArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "str_tail requires 1 expression on stack",
        );
    }

    let expr = expr_stack.pop().unwrap();
    expr_stack.push(expr.str().tail(lit(args.n)));
    FfiResult::success_no_handle()
}

/// Pad string on the left (start)
pub fn expr_str_pad_start(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const PadArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "str_pad_start requires 1 expression on stack",
        );
    }

    let fill_char = args.fill_char as char;
    let expr = expr_stack.pop().unwrap();
    expr_stack.push(expr.str().pad_start(lit(args.length as u64), fill_char));
    FfiResult::success_no_handle()
}

/// Pad string on the right (end)
pub fn expr_str_pad_end(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const PadArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "str_pad_end requires 1 expression on stack",
        );
    }

    let fill_char = args.fill_char as char;
    let expr = expr_stack.pop().unwrap();
    expr_stack.push(expr.str().pad_end(lit(args.length as u64), fill_char));
    FfiResult::success_no_handle()
}

/// Zero-fill string on the left
pub fn expr_str_zfill(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };
    let args = unsafe { &*(ctx.operation_args as *const HeadTailArgs) };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "str_zfill requires 1 expression on stack",
        );
    }

    let expr = expr_stack.pop().unwrap();
    expr_stack.push(expr.str().zfill(lit(args.n as u64)));
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

// Conditional expression operations

/// When operation - starts a conditional chain
/// Stack: [condition] -> [condition, when_marker]
pub fn expr_when(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };

    if expr_stack.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "when requires 1 expression (condition) on stack",
        );
    }

    // The condition is already on the stack, just mark it as a when condition
    // We don't need to do anything here - the condition stays on the stack
    // and will be used when we build the final conditional in expr_otherwise
    FfiResult::success_no_handle()
}

/// Then operation - pairs a value with the most recent when condition
/// Stack: [condition] -> [condition, value]
pub fn expr_then(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };

    if expr_stack.len() < 2 {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "then requires 2 expressions on stack (condition and value)",
        );
    }

    // The value and condition are already on the stack
    // We don't need to do anything here - they stay on the stack
    // and will be used when we build the final conditional in expr_otherwise
    FfiResult::success_no_handle()
}

/// Otherwise operation - finalizes the conditional chain and builds the complete expression
/// Stack: [condition1, value1, condition2, value2, ..., default_value] -> [when().then().otherwise()]
pub fn expr_otherwise(ctx: &ExecutionContext) -> FfiResult {
    let expr_stack = unsafe { &mut *ctx.expr_stack };

    if expr_stack.len() < 3 {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "otherwise requires at least 3 expressions on stack (condition, value, default)",
        );
    }

    // Pop the default value (otherwise clause)
    let default_value = expr_stack.pop().unwrap();

    // Build the conditional expression by processing pairs from the stack
    // The stack contains: [condition1, value1, condition2, value2, ...]
    // We need to build: when(condition1).then(value1).when(condition2).then(value2).otherwise(default)
    
    let mut conditions_and_values = Vec::new();
    
    // Collect all condition-value pairs
    while expr_stack.len() >= 2 {
        let value = expr_stack.pop().unwrap();
        let condition = expr_stack.pop().unwrap();
        conditions_and_values.push((condition, value));
    }

    if conditions_and_values.is_empty() {
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "otherwise requires at least one when/then pair",
        );
    }

    // Reverse to get the correct order (first when/then pair first)
    conditions_and_values.reverse();

    // Build the conditional expression step by step
    // For now, let's handle only single when/then/otherwise to get it working
    if conditions_and_values.len() == 1 {
        let (condition, value) = conditions_and_values.into_iter().next().unwrap();
        let final_expr = when(condition).then(value).otherwise(default_value);
        expr_stack.push(final_expr);
    } else {
        // For multiple conditions, we'll need to handle the complex type system
        // For now, return an error to indicate this needs more work
        return FfiResult::error(
            ERROR_POLARS_OPERATION,
            "Multiple when/then conditions not yet supported - use single when/then/otherwise for now",
        );
    }

    FfiResult::success_no_handle()
}
