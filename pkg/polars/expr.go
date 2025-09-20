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
	return expr.unaryOp(C.expr_not)
}

// Sum applies sum aggregation to the expression
func (expr *ExprNode) Sum() *ExprNode {
	return expr.unaryOp(C.expr_sum)
}

// Mean applies mean aggregation to the expression
func (expr *ExprNode) Mean() *ExprNode {
	return expr.unaryOp(C.expr_mean)
}

// Min applies min aggregation to the expression
func (expr *ExprNode) Min() *ExprNode {
	return expr.unaryOp(C.expr_min)
}

// Max applies max aggregation to the expression
func (expr *ExprNode) Max() *ExprNode {
	return expr.unaryOp(C.expr_max)
}

// Median applies median aggregation to the expression
func (expr *ExprNode) Median() *ExprNode {
	return expr.unaryOp(C.expr_median)
}

// First gets the first value of the expression
func (expr *ExprNode) First() *ExprNode {
	return expr.unaryOp(C.expr_first)
}

// Last gets the last value of the expression
func (expr *ExprNode) Last() *ExprNode {
	return expr.unaryOp(C.expr_last)
}

// NUnique counts unique values in the expression
func (expr *ExprNode) NUnique() *ExprNode {
	return expr.unaryOp(C.expr_nunique)
}

// IsNull checks if values are null
func (expr *ExprNode) IsNull() *ExprNode {
	return expr.unaryOp(C.expr_is_null)
}

// IsNotNull checks if values are not null
func (expr *ExprNode) IsNotNull() *ExprNode {
	return expr.unaryOp(C.expr_is_not_null)
}

// Count counts non-null values (excludes nulls)
func (expr *ExprNode) Count() *ExprNode {
	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			funcPtr: C.expr_count,
			args: func() unsafe.Pointer {
				return unsafe.Pointer(&C.CountArgs{
					include_nulls: C.bool(false),
				})
			},
		})),
	}
}

// CountWithNulls counts all values including nulls
func (expr *ExprNode) CountWithNulls() *ExprNode {
	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			funcPtr: C.expr_count,
			args: func() unsafe.Pointer {
				return unsafe.Pointer(&C.CountArgs{
					include_nulls: C.bool(true),
				})
			},
		})),
	}
}

// unaryOp is a helper for simple unary operations (no parameters)
func (expr *ExprNode) unaryOp(funcPtr unsafe.Pointer) *ExprNode {
	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			funcPtr: funcPtr,
			args:    noArgs,
		})),
	}
}

// unaryOpWithStringArgs is a helper for unary operations that take StringArgs
func (expr *ExprNode) unaryOpWithStringArgs(funcPtr unsafe.Pointer, pattern string) *ExprNode {
	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			funcPtr: funcPtr,
			args: func() unsafe.Pointer {
				return unsafe.Pointer(&C.StringArgs{
					pattern: makeRawStr(pattern),
				})
			},
		})),
	}
}

// unaryOpWithAliasArgs is a helper for unary operations that take AliasArgs
func (expr *ExprNode) unaryOpWithAliasArgs(funcPtr unsafe.Pointer, name string) *ExprNode {
	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			funcPtr: funcPtr,
			args: func() unsafe.Pointer {
				return unsafe.Pointer(&C.AliasArgs{
					name: makeRawStr(name),
				})
			},
		})),
	}
}

// ddofAggregation is a helper for std/var operations that take ddof parameter
func (expr *ExprNode) ddofAggregation(funcPtr unsafe.Pointer, opName string, ddof ...uint8) *ExprNode {
	if len(ddof) > 1 {
		return &ExprNode{ops: combine(expr.ops, single(Operation{err: fmt.Errorf("%s() accepts at most one ddof parameter", opName)}))}
	}

	ddofValue := uint8(0) // Default to population
	if len(ddof) == 1 {
		ddofValue = ddof[0]
		if ddofValue != 0 && ddofValue != 1 {
			return &ExprNode{ops: combine(expr.ops, single(Operation{err: fmt.Errorf("ddof must be 0 (population) or 1 (sample)")}))}
		}
	}

	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			funcPtr: funcPtr,
			args: func() unsafe.Pointer {
				return unsafe.Pointer(&C.AggregationArgs{ddof: C.uchar(ddofValue)})
			},
		})),
	}
}

// Std applies standard deviation aggregation to the expression
// ddof=0: population std (default), ddof=1: sample std (unbiased)
// Usage: Col("age").Std() or Col("age").Std(0) or Col("age").Std(1)
func (expr *ExprNode) Std(ddof ...uint8) *ExprNode {
	return expr.ddofAggregation(C.expr_std, "Std", ddof...)
}

// Var applies variance aggregation to the expression
// ddof=0: population variance (default), ddof=1: sample variance (unbiased)
// Usage: Col("age").Var() or Col("age").Var(0) or Col("age").Var(1)
func (expr *ExprNode) Var(ddof ...uint8) *ExprNode {
	return expr.ddofAggregation(C.expr_var, "Var", ddof...)
}

// Alias adds an alias to the expression for naming computed columns
func (expr *ExprNode) Alias(name string) *ExprNode {
	return expr.unaryOpWithAliasArgs(C.expr_alias, name)
}

// String operations

// StrLen returns the length of each string as the number of characters
func (expr *ExprNode) StrLen() *ExprNode {
	return expr.unaryOp(C.expr_str_len)
}

// StrToLowercase converts all characters to lowercase
func (expr *ExprNode) StrToLowercase() *ExprNode {
	return expr.unaryOp(C.expr_str_to_lowercase)
}

// StrToUppercase converts all characters to uppercase
func (expr *ExprNode) StrToUppercase() *ExprNode {
	return expr.unaryOp(C.expr_str_to_uppercase)
}

// StrContains checks if string values contain a literal substring
func (expr *ExprNode) StrContains(pattern string) *ExprNode {
	return expr.unaryOpWithStringArgs(C.expr_str_contains, pattern)
}

// StrStartsWith checks if string values start with a prefix
func (expr *ExprNode) StrStartsWith(prefix string) *ExprNode {
	return expr.unaryOpWithStringArgs(C.expr_str_starts_with, prefix)
}

// StrEndsWith checks if string values end with a suffix
func (expr *ExprNode) StrEndsWith(suffix string) *ExprNode {
	return expr.unaryOpWithStringArgs(C.expr_str_ends_with, suffix)
}
