package polars

/*
#include "turbo_polars.h"
*/
import "C"
import (
	"fmt"
	"iter"
	"unsafe"
)

// ExprNode contains a lazy sequence of operations to build an expression
type ExprNode struct {
	ops iter.Seq[Operation] // Lazy iterator over operations - no allocation until consumed
}

// Helper functions for iterator composition
func combine(iterators ...iter.Seq[Operation]) iter.Seq[Operation] {
	return func(yield func(Operation) bool) {
		for _, it := range iterators {
			if it == nil {
				continue
			}
			for op := range it {
				if !yield(op) {
					return
				}
			}
		}
	}
}

// single creates an iterator that yields a single operation
func single(op Operation) iter.Seq[Operation] {
	return func(yield func(Operation) bool) {
		yield(op)
	}
}

// Helper methods for ExprNode
// Note: Using pointer semantics for fluent chaining API

// consumed returns true if the expression has been consumed (move semantics)
func (e *ExprNode) consumed() bool {
	return e.ops == nil
}

// consume clears the expression operations (move semantics)
func (e *ExprNode) consume() {
	e.ops = nil
}

// consumeOps returns the operations and clears them (move semantics)
func (e *ExprNode) consumeOps() iter.Seq[Operation] {
	ops := e.ops
	e.ops = nil
	return ops
}

// countOps returns the number of operations in the expression (for testing)
func (e *ExprNode) countOps() int {
	if e.ops == nil {
		return 0
	}
	count := 0
	for range e.ops {
		count++
	}
	return count
}

// Expression builders
func Col(name string) *ExprNode {
	return &ExprNode{
		ops: func(yield func(Operation) bool) {
			yield(Operation{
				funcPtr: C.expr_column,
				args: func() unsafe.Pointer {
					return unsafe.Pointer(&C.ColumnArgs{
						name: makeRawStr(name), // name captured by closure, stays alive
					})
				},
			})
		},
	}
}

func Lit(value interface{}) *ExprNode {
	return &ExprNode{
		ops: func(yield func(Operation) bool) {
			yield(Operation{
				funcPtr: C.expr_literal,
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
		},
	}
}

func noArgs() unsafe.Pointer { return nil }

func binOp(left, right *ExprNode, op unsafe.Pointer) *ExprNode {
	// Combine left, right using op.
	left.ops = combine(
		left.ops,
		right.consumeOps(),
		single(Operation{
			funcPtr: op,
			args:    noArgs, // op takes no args - operates on expression stack
		}))
	return left
}

// Binary operations - iterator chaining with move semantics
func (left *ExprNode) Gt(right *ExprNode) *ExprNode {
	return binOp(left, right, C.expr_gt)
}

func (left *ExprNode) Lt(right *ExprNode) *ExprNode {
	return binOp(left, right, C.expr_lt)
}

func (left *ExprNode) Eq(right *ExprNode) *ExprNode {
	return binOp(left, right, C.expr_eq)
}

// Arithmetic operations
func (left *ExprNode) Add(right *ExprNode) *ExprNode {
	return binOp(left, right, C.expr_add)
}

func (left *ExprNode) Sub(right *ExprNode) *ExprNode {
	return binOp(left, right, C.expr_sub)
}

func (left *ExprNode) Mul(right *ExprNode) *ExprNode {
	return binOp(left, right, C.expr_mul)
}

func (left *ExprNode) Div(right *ExprNode) *ExprNode {
	return binOp(left, right, C.expr_div)
}

// Boolean operations
func (left *ExprNode) And(right *ExprNode) *ExprNode {
	return binOp(left, right, C.expr_and)
}

func (left *ExprNode) Or(right *ExprNode) *ExprNode {
	return binOp(left, right, C.expr_or)
}

func (expr *ExprNode) Not() *ExprNode {
	// NOT is a unary operation - just add it to the current expression
	expr.ops = combine(
		expr.ops,
		single(Operation{
			funcPtr: C.expr_not,
			args:    noArgs, // NOT takes no args - operates on expression stack
		}),
	)

	return expr // Return the modified expression
}

// Alias adds an alias to the expression for naming computed columns
func (expr *ExprNode) Alias(name string) *ExprNode {
	// Add alias operation to the expression
	expr.ops = combine(
		expr.ops,
		single(Operation{
			funcPtr: unsafe.Pointer(C.expr_alias),
			args: func() unsafe.Pointer {
				return unsafe.Pointer(&C.AliasArgs{
					name: makeRawStr(name),
				})
			},
		}),
	)
	return expr
}
