use crate::{FfiResult, ERROR_POLARS_OPERATION};
use polars::prelude::*;

/// ExecutionContext holds the expression stack and operation arguments
/// This provides a unified interface for all dispatch functions
#[repr(C)]
pub struct ExecutionContext {
    pub expr_stack: *mut Vec<Expr>, // Raw pointer to Vec<Expr>
    pub operation_args: usize,      // Pointer to operation-specific args
}

/// Operation struct for expression operations (used by execute_expr_ops)
#[repr(C)]
pub struct ExprOp {
    pub func_ptr: usize, // Function pointer to expression operation
    pub args: usize,     // Arguments for the operation
}

/// Operation struct for the main operation chain
#[repr(C)]
pub struct Operation {
    pub func_ptr: usize, // Function pointer to dispatch function
    pub args: usize,     // Arguments for the operation
}

/// Execute a sequence of expression operations to build a single Expr
pub fn execute_expr_ops(ops: &[ExprOp]) -> std::result::Result<Expr, &'static str> {
    let mut stack = Vec::new();

    for op in ops {
        // Create ExecutionContext for this operation
        let context = ExecutionContext {
            expr_stack: &mut stack as *mut Vec<Expr>,
            operation_args: op.args,
        };

        // Call the operation with new signature
        type DispatchFn = extern "C" fn(usize, usize) -> FfiResult;
        let dispatch_fn = unsafe { std::mem::transmute::<usize, DispatchFn>(op.func_ptr) };
        let result = dispatch_fn(0, &context as *const ExecutionContext as usize); // handle=0 for expression ops

        if result.error_code != 0 {
            return Err("Expression operation failed");
        }
    }

    if stack.len() != 1 {
        return Err("Invalid expression - stack should have exactly one element");
    }

    Ok(stack.pop().unwrap())
}

/// Main execution function - processes a chain of operations
#[no_mangle]
pub extern "C" fn execute_operations(
    handle: usize,
    operations_ptr: *const Operation,
    count: usize,
) -> FfiResult {
    if operations_ptr.is_null() || count == 0 {
        return FfiResult::error(ERROR_POLARS_OPERATION, "Operations cannot be null or empty");
    }

    let operations = unsafe { std::slice::from_raw_parts(operations_ptr, count) };
    let mut current_handle = handle;
    let mut expr_stack = Vec::new(); // Expression stack for building expressions

    for (frame_idx, op) in operations.iter().enumerate() {
        // Create ExecutionContext for this operation
        let context = ExecutionContext {
            expr_stack: &mut expr_stack as *mut Vec<Expr>,
            operation_args: op.args,
        };

        // Call the operation with uniform interface
        type DispatchFn = extern "C" fn(usize, usize) -> FfiResult;
        let dispatch_fn = unsafe { std::mem::transmute::<usize, DispatchFn>(op.func_ptr) };
        let result = dispatch_fn(current_handle, &context as *const ExecutionContext as usize);

        if result.error_code != 0 {
            // Return error with frame information
            return FfiResult {
                handle: 0,
                error_code: result.error_code,
                error_message: result.error_message,
                error_frame: frame_idx,
            };
        }

        current_handle = result.handle;
    }

    FfiResult::success_with_handle(current_handle)
}
