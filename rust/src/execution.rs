use crate::{ContextType, FfiResult, OpCode, Operation, PolarsHandle, ERROR_POLARS_OPERATION};
use polars::prelude::*;

/// ExecutionContext holds the expression stack and operation arguments
/// This provides a unified interface for all dispatch functions
#[repr(C)]
pub struct ExecutionContext {
    pub expr_stack: *mut Vec<Expr>, // Raw pointer to Vec<Expr>
    pub operation_args: usize,      // Pointer to operation-specific args
}

/// Execute a sequence of expression operations to build a single Expr
pub fn execute_expr_ops(ops: &[Operation]) -> std::result::Result<Expr, &'static str> {
    let mut stack = Vec::new();

    for op in ops {

        let opcode = op.get_opcode().ok_or("Invalid opcode")?;

        // Verify this is an expression operation
        if !opcode.is_expression_op() {
            return Err("Non-expression operation in expression context");
        }

        // Create ExecutionContext for this operation
        let context = ExecutionContext {
            expr_stack: &mut stack as *mut Vec<Expr>,
            operation_args: op.args,
        };

        // Dispatch based on opcode
        let result = dispatch_expression_operation(opcode, &context);

        if result.error_code != 0 {
            return Err("Expression operation failed");
        }
    }

    if stack.len() != 1 {
        return Err("Invalid expression - stack should have exactly one element");
    }

    Ok(stack.pop().unwrap())
}

/// Dispatch expression operations based on opcode
fn dispatch_expression_operation(opcode: OpCode, ctx: &ExecutionContext) -> FfiResult {
    use crate::expr::*;

    match opcode {
        OpCode::ExprColumn => expr_column(ctx),
        OpCode::ExprLiteral => expr_literal(ctx),
        OpCode::ExprAdd => expr_add(ctx),
        OpCode::ExprSub => expr_sub(ctx),
        OpCode::ExprMul => expr_mul(ctx),
        OpCode::ExprDiv => expr_div(ctx),
        OpCode::ExprGt => expr_gt(ctx),
        OpCode::ExprLt => expr_lt(ctx),
        OpCode::ExprEq => expr_eq(ctx),
        OpCode::ExprAnd => expr_and(ctx),
        OpCode::ExprOr => expr_or(ctx),
        OpCode::ExprNot => expr_not(ctx),
        OpCode::ExprSum => expr_sum(ctx),
        OpCode::ExprMean => expr_mean(ctx),
        OpCode::ExprMin => expr_min(ctx),
        OpCode::ExprMax => expr_max(ctx),
        OpCode::ExprStd => expr_std(ctx),
        OpCode::ExprVar => expr_var(ctx),
        OpCode::ExprMedian => expr_median(ctx),
        OpCode::ExprFirst => expr_first(ctx),
        OpCode::ExprLast => expr_last(ctx),
        OpCode::ExprNUnique => expr_nunique(ctx),
        OpCode::ExprCount => expr_count(ctx),
        OpCode::ExprCountNulls => expr_count(ctx), // Same function, different args
        OpCode::ExprIsNull => expr_is_null(ctx),
        OpCode::ExprIsNotNull => expr_is_not_null(ctx),
        OpCode::ExprAlias => expr_alias(ctx),
        OpCode::ExprStrLen => expr_str_len(ctx),
        OpCode::ExprStrContains => expr_str_contains(ctx),
        OpCode::ExprStrStartsWith => expr_str_starts_with(ctx),
        OpCode::ExprStrEndsWith => expr_str_ends_with(ctx),
        OpCode::ExprStrToLowercase => expr_str_to_lowercase(ctx),
        OpCode::ExprStrToUppercase => expr_str_to_uppercase(ctx),
        OpCode::ExprSql => expr_sql(ctx),
        OpCode::ExprStrSlice => expr_str_slice(ctx),
        OpCode::ExprStrReplace => expr_str_replace(ctx),
        OpCode::ExprStrSplit => expr_str_split(ctx),
        // Advanced string operations (Tier 2)
        OpCode::ExprStrLenBytes => expr_str_len_bytes(ctx),
        OpCode::ExprStrStripChars => expr_str_strip_chars(ctx),
        OpCode::ExprStrStripStart => expr_str_strip_chars_start(ctx),
        OpCode::ExprStrStripEnd => expr_str_strip_chars_end(ctx),
        OpCode::ExprStrStripPrefix => expr_str_strip_prefix(ctx),
        OpCode::ExprStrStripSuffix => expr_str_strip_suffix(ctx),
        OpCode::ExprStrReverse => expr_str_reverse(ctx),
        OpCode::ExprStrHead => expr_str_head(ctx),
        OpCode::ExprStrTail => expr_str_tail(ctx),
        OpCode::ExprStrPadStart => expr_str_pad_start(ctx),
        OpCode::ExprStrPadEnd => expr_str_pad_end(ctx),
        OpCode::ExprStrZfill => expr_str_zfill(ctx),
        // Window function operations
        OpCode::ExprOver => expr_over(ctx),
        OpCode::ExprRank => expr_rank(ctx),
        OpCode::ExprDenseRank => expr_dense_rank(ctx),
        OpCode::ExprRowNumber => expr_row_number(ctx),
        OpCode::ExprLag => expr_lag(ctx),
        OpCode::ExprLead => expr_lead(ctx),
        // Conditional expressions
        OpCode::ExprWhen => expr_when(ctx),
        OpCode::ExprThen => expr_then(ctx),
        OpCode::ExprOtherwise => expr_otherwise(ctx),
        // Cast operations
        OpCode::ExprCast => expr_cast(ctx),
        _ => FfiResult::error(ERROR_POLARS_OPERATION, "Unsupported expression operation"),
    }
}

/// Dispatch DataFrame operations based on opcode and context
fn dispatch_dataframe_operation(
    opcode: OpCode,
    handle: PolarsHandle,
    context: &ExecutionContext,
) -> (FfiResult, ContextType) {
    use crate::dataframe::*;
    use crate::io::*;

    match opcode {
        OpCode::NewEmpty => (dispatch_new_empty(), ContextType::DataFrame),
        OpCode::ReadCsv => (dispatch_read_csv(handle, context), ContextType::LazyFrame),
        OpCode::ReadParquet => (dispatch_read_parquet(handle, context), ContextType::LazyFrame),
        OpCode::Select => (dispatch_select(handle, context), ContextType::LazyFrame),
        OpCode::SelectExpr => (
            dispatch_select_expr(handle, context),
            ContextType::LazyFrame,
        ),
        OpCode::Count => (dispatch_count(handle), ContextType::LazyFrame),
        OpCode::Concat => (dispatch_concat(handle, context), ContextType::DataFrame),
        OpCode::WithColumn => (
            dispatch_with_column(handle, context),
            ContextType::LazyFrame,
        ),
        OpCode::FilterExpr => (
            dispatch_filter_expr(handle, context),
            ContextType::LazyFrame,
        ),
        OpCode::GroupBy => (dispatch_group_by(handle, context), ContextType::LazyGroupBy),
        OpCode::Agg => (dispatch_agg(handle, context), ContextType::LazyFrame),
        OpCode::Sort => {
            // Sort preserves the input context type (DataFrame->DataFrame, LazyFrame->LazyFrame)
            let input_context = handle.get_context_type().unwrap_or(ContextType::DataFrame);
            (dispatch_sort(handle, context), input_context)
        }
        OpCode::Limit => {
            // Limit preserves the input context type (DataFrame->DataFrame, LazyFrame->LazyFrame)
            let input_context = handle.get_context_type().unwrap_or(ContextType::DataFrame);
            (dispatch_limit(handle, context), input_context)
        }
        OpCode::AddNullRow => (dispatch_add_null_row(handle), ContextType::DataFrame),
        OpCode::Collect => (dispatch_collect(handle), ContextType::DataFrame),
        OpCode::Query => (dispatch_query(handle, context), ContextType::LazyFrame),
        OpCode::Join => {
            // Join preserves the input context type (DataFrame->DataFrame, LazyFrame->LazyFrame)
            let input_context = handle.get_context_type().unwrap_or(ContextType::DataFrame);
            (dispatch_join(handle, context), input_context)
        }
        OpCode::FromMemory => (dispatch_from_memory(context), ContextType::DataFrame),
        _ => (
            FfiResult::error(ERROR_POLARS_OPERATION, "Unsupported DataFrame operation"),
            handle.get_context_type().unwrap_or(ContextType::DataFrame),
        ),
    }
}

/// Main execution function - processes a chain of operations with context tracking
#[no_mangle]
pub extern "C" fn execute_operations(
    polars_handle: PolarsHandle,
    operations_ptr: *const Operation,
    count: usize,
) -> FfiResult {
    if operations_ptr.is_null() || count == 0 {
        return FfiResult::error(ERROR_POLARS_OPERATION, "Operations cannot be null or empty");
    }

    let operations = unsafe { std::slice::from_raw_parts(operations_ptr, count) };
    let mut current_handle = polars_handle.handle;
    let mut current_context_type = polars_handle
        .get_context_type()
        .unwrap_or(ContextType::DataFrame); // Use the actual context from the handle
    let mut expr_stack = Vec::new(); // Expression stack for building expressions

    for (frame_idx, op) in operations.iter().enumerate() {

        let opcode = match op.get_opcode() {
            Some(opcode) => opcode,
            None => {
                return FfiResult::error(
                    ERROR_POLARS_OPERATION,
                    &format!("Invalid opcode: {}", op.opcode),
                )
            }
        };

        // Create ExecutionContext for this operation
        let ctx = ExecutionContext {
            expr_stack: &mut expr_stack as *mut Vec<Expr>,
            operation_args: op.args,
        };

        // Dispatch based on operation type
        let (result, new_context_type) = if opcode.is_dataframe_op() {
            dispatch_dataframe_operation(
                opcode,
                PolarsHandle::new(current_handle, current_context_type),
                &ctx,
            )
        } else if opcode.is_expression_op() {
            // Expression operations don't change context, just build expressions
            (
                dispatch_expression_operation(opcode, &ctx),
                current_context_type,
            )
        } else {
            (
                FfiResult::error(ERROR_POLARS_OPERATION, "Unknown operation type"),
                current_context_type,
            )
        };

        if result.error_code != 0 {
            // Return error with frame information
            return FfiResult {
                polars_handle: PolarsHandle::new(0, ContextType::DataFrame), // Error case
                error_code: result.error_code,
                error_message: result.error_message,
                error_frame: frame_idx,
            };
        }

        // Only update handle for DataFrame operations, not expression operations
        if opcode.is_dataframe_op() {
            current_handle = result.polars_handle.handle;
        }
        current_context_type = new_context_type;
    }

    FfiResult::success_with_handle(current_handle, current_context_type)
}
