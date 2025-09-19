package polars

/*
#include "turbo_polars.h"
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// Expression node - contains a sequence of operations to build an expression
// Follows Rust-like move semantics: expressions are consumed by operations unless explicitly cloned
type ExprNode struct {
	scratch [4]Operation // Stack-allocated buffer for 1-4 operations (covers 95% of cases)
	ops     []Operation  // Points to scratch[:0] initially, heap slice when >4 operations
}

// Helper methods for ExprNode
// Note: Using pointer semantics for fluent chaining API

// consumed returns true if the expression has been consumed (move semantics)
func (e *ExprNode) consumed() bool {
	return len(e.ops) == 0
}

// consume clears the expression operations (move semantics)
func (e *ExprNode) consume() {
	e.ops = e.ops[:0] // Reuse slice capacity
}

// Expression builders
func Col(name string) *ExprNode {
	expr := &ExprNode{}
	expr.ops = expr.scratch[:0] // Initialize ops to point to scratch buffer
	expr.ops = append(expr.ops, Operation{
		funcPtr: uintptr(unsafe.Pointer(C.expr_column)),
		args: func() unsafe.Pointer {
			return unsafe.Pointer(&C.ColumnArgs{
				name: makeRawStr(name), // name captured by closure, stays alive
			})
		},
	})
	return expr
}

func Lit(value interface{}) *ExprNode {
	expr := &ExprNode{}
	expr.ops = expr.scratch[:0] // Initialize ops to point to scratch buffer
	expr.ops = append(expr.ops, Operation{
		funcPtr: uintptr(unsafe.Pointer(C.expr_literal)),
		args: func() unsafe.Pointer {
			// Closure captures value, keeping it alive
			switch v := value.(type) {
			case int:
				return unsafe.Pointer(&C.LiteralArgs{
					literal: C.Literal{
						value_type: 0,
						int_value:  C.longlong(v),
					},
				})
			case int64:
				return unsafe.Pointer(&C.LiteralArgs{
					literal: C.Literal{
						value_type: 0,
						int_value:  C.longlong(v),
					},
				})
			case float64:
				return unsafe.Pointer(&C.LiteralArgs{
					literal: C.Literal{
						value_type:  1,
						float_value: C.double(v),
					},
				})
			case string:
				return unsafe.Pointer(&C.LiteralArgs{
					literal: C.Literal{
						value_type:   2,
						string_value: makeRawStr(v), // v captured by closure
					},
				})
			case bool:
				return unsafe.Pointer(&C.LiteralArgs{
					literal: C.Literal{
						value_type: 3,
						bool_value: C._Bool(v),
					},
				})
			default:
				panic(fmt.Sprintf("unsupported literal type: %T", value))
			}
		},
	})
	return expr
}

// Binary operations - "poor man's move semantics" with slice reuse
func (left *ExprNode) Gt(right *ExprNode) *ExprNode {
	// Reuse left's slice, append right operations and GT operation
	left.ops = append(left.ops, right.ops...)
	left.ops = append(left.ops, Operation{
		funcPtr: uintptr(unsafe.Pointer(C.expr_gt)),
		args:    func() unsafe.Pointer { return nil }, // GT takes no args - operates on expression stack
	})

	// Clear right operand to prevent reuse (move semantics)
	right.consume()

	return left // Return the modified left node
}

func (left *ExprNode) Lt(right *ExprNode) *ExprNode {
	// Reuse left's slice, append right operations and LT operation
	left.ops = append(left.ops, right.ops...)
	left.ops = append(left.ops, Operation{
		funcPtr: uintptr(unsafe.Pointer(C.expr_lt)),
		args:    func() unsafe.Pointer { return nil }, // LT takes no args - operates on expression stack
	})

	// Clear right operand to prevent reuse (move semantics)
	right.consume()

	return left // Return the modified left node
}

func (left *ExprNode) Eq(right *ExprNode) *ExprNode {
	// Reuse left's slice, append right operations and EQ operation
	left.ops = append(left.ops, right.ops...)
	left.ops = append(left.ops, Operation{
		funcPtr: uintptr(unsafe.Pointer(C.expr_eq)),
		args:    func() unsafe.Pointer { return nil }, // EQ takes no args - operates on expression stack
	})

	// Clear right operand to prevent reuse (move semantics)
	right.consume()

	return left // Return the modified left node
}
