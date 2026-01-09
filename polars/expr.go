package polars

/*
#include "firn.h"
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

// ConditionalNode represents a conditional expression being built (When/Then/Otherwise)
type ConditionalNode struct {
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
				opcode: OpExprColumn,
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
				opcode: OpExprLiteral,
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

// SqlExpr creates an ExprNode from a SQL expression string
// Supports SQL expressions like "salary * 1.1", "(a + b) / c", "salary * 1.1 AS bonus_salary"
// For supported SQL functions, see: https://docs.pola.rs/api/python/dev/reference/sql/functions/index.html
func SqlExpr(sql string) *ExprNode {
	return &ExprNode{
		ops: single(Operation{
			opcode: OpExprSql,
			args: func() unsafe.Pointer {
				return unsafe.Pointer(&C.SqlExprArgs{
					sql: makeRawStr(sql), // sql captured by closure, stays alive
				})
			},
		}),
	}
}

// toExprNodes converts a variadic list of any type to ExprNodes
// Strings are automatically converted to SqlExpr, ExprNodes are used as-is
func toExprNodes(args ...any) []*ExprNode {
	exprs := make([]*ExprNode, len(args))
	for i, arg := range args {
		switch v := arg.(type) {
		case string:
			exprs[i] = SqlExpr(v)
		case *ExprNode:
			exprs[i] = v
		default:
			// Create an error expression for unsupported types
			exprs[i] = &ExprNode{
				ops: single(errOpf("unsupported argument type: %T (expected string or *ExprNode)", arg)),
			}
		}
	}
	return exprs
}

func noArgs() unsafe.Pointer { return nil }

func binOp(left, right *ExprNode, opcode uint32) *ExprNode {
	// Combine left, right using opcode.
	left.ops = combine(
		left.ops,
		right.consumeOps(),
		single(Operation{
			opcode: opcode,
			args:   noArgs, // op takes no args - operates on expression stack
		}))
	return left
}

// Binary operations - iterator chaining with move semantics
func (left *ExprNode) Gt(right *ExprNode) *ExprNode {
	return binOp(left, right, OpExprGt)
}

func (left *ExprNode) Lt(right *ExprNode) *ExprNode {
	return binOp(left, right, OpExprLt)
}

func (left *ExprNode) Eq(right *ExprNode) *ExprNode {
	return binOp(left, right, OpExprEq)
}

// Arithmetic operations
func (left *ExprNode) Add(right *ExprNode) *ExprNode {
	return binOp(left, right, OpExprAdd)
}

func (left *ExprNode) Sub(right *ExprNode) *ExprNode {
	return binOp(left, right, OpExprSub)
}

func (left *ExprNode) Mul(right *ExprNode) *ExprNode {
	return binOp(left, right, OpExprMul)
}

func (left *ExprNode) Div(right *ExprNode) *ExprNode {
	return binOp(left, right, OpExprDiv)
}

// Boolean operations
func (left *ExprNode) And(right *ExprNode) *ExprNode {
	return binOp(left, right, OpExprAnd)
}

func (left *ExprNode) Or(right *ExprNode) *ExprNode {
	return binOp(left, right, OpExprOr)
}

func (expr *ExprNode) Not() *ExprNode {
	return expr.unaryOp(OpExprNot)
}

// Sum applies sum aggregation to the expression
func (expr *ExprNode) Sum() *ExprNode {
	return expr.unaryOp(OpExprSum)
}

// Mean applies mean aggregation to the expression
func (expr *ExprNode) Mean() *ExprNode {
	return expr.unaryOp(OpExprMean)
}

// Min applies min aggregation to the expression
func (expr *ExprNode) Min() *ExprNode {
	return expr.unaryOp(OpExprMin)
}

// Max applies max aggregation to the expression
func (expr *ExprNode) Max() *ExprNode {
	return expr.unaryOp(OpExprMax)
}

// Median applies median aggregation to the expression
func (expr *ExprNode) Median() *ExprNode {
	return expr.unaryOp(OpExprMedian)
}

// First gets the first value of the expression
func (expr *ExprNode) First() *ExprNode {
	return expr.unaryOp(OpExprFirst)
}

// Last gets the last value of the expression
func (expr *ExprNode) Last() *ExprNode {
	return expr.unaryOp(OpExprLast)
}

// NUnique counts unique values in the expression
func (expr *ExprNode) NUnique() *ExprNode {
	return expr.unaryOp(OpExprNUnique)
}

// IsNull checks if values are null
func (expr *ExprNode) IsNull() *ExprNode {
	return expr.unaryOp(OpExprIsNull)
}

// IsNotNull checks if values are not null
func (expr *ExprNode) IsNotNull() *ExprNode {
	return expr.unaryOp(OpExprIsNotNull)
}

// Count counts non-null values (excludes nulls)
func (expr *ExprNode) Count() *ExprNode {
	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			opcode: OpExprCount,
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
			opcode: OpExprCountNulls,
			args: func() unsafe.Pointer {
				return unsafe.Pointer(&C.CountArgs{
					include_nulls: C.bool(true),
				})
			},
		})),
	}
}

// unaryOp is a helper for simple unary operations (no parameters)
func (expr *ExprNode) unaryOp(opcode uint32) *ExprNode {
	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			opcode: opcode,
			args:   noArgs,
		})),
	}
}

// unaryOpWithStringArgs is a helper for unary operations that take StringArgs
func (expr *ExprNode) unaryOpWithStringArgs(opcode uint32, pattern string) *ExprNode {
	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			opcode: opcode,
			args: func() unsafe.Pointer {
				return unsafe.Pointer(&C.StringArgs{
					pattern: makeRawStr(pattern),
				})
			},
		})),
	}
}

// unaryOpWithAliasArgs is a helper for unary operations that take AliasArgs
func (expr *ExprNode) unaryOpWithAliasArgs(opcode uint32, name string) *ExprNode {
	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			opcode: opcode,
			args: func() unsafe.Pointer {
				return unsafe.Pointer(&C.AliasArgs{
					name: makeRawStr(name),
				})
			},
		})),
	}
}

// ddofAggregation is a helper for std/var operations that take ddof parameter
func (expr *ExprNode) ddofAggregation(opcode uint32, opName string, ddof ...uint8) *ExprNode {
	if len(ddof) > 1 {
		return &ExprNode{ops: combine(expr.ops, single(errOpf("%s() accepts at most one ddof parameter", opName)))}
	}

	ddofValue := uint8(0) // Default to population
	if len(ddof) == 1 {
		ddofValue = ddof[0]
		if ddofValue != 0 && ddofValue != 1 {
			return &ExprNode{ops: combine(expr.ops, single(errOp("ddof must be 0 (population) or 1 (sample)")))}
		}
	}

	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			opcode: opcode,
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
	return expr.ddofAggregation(OpExprStd, "Std", ddof...)
}

// Var applies variance aggregation to the expression
// ddof=0: population variance (default), ddof=1: sample variance (unbiased)
// Usage: Col("age").Var() or Col("age").Var(0) or Col("age").Var(1)
func (expr *ExprNode) Var(ddof ...uint8) *ExprNode {
	return expr.ddofAggregation(OpExprVar, "Var", ddof...)
}

// Alias adds an alias to the expression for naming computed columns
func (expr *ExprNode) Alias(name string) *ExprNode {
	return expr.unaryOpWithAliasArgs(OpExprAlias, name)
}

// String operations

// StrLen returns the length of each string as the number of characters
func (expr *ExprNode) StrLen() *ExprNode {
	return expr.unaryOp(OpExprStrLen)
}

// StrToLowercase converts all characters to lowercase
func (expr *ExprNode) StrToLowercase() *ExprNode {
	return expr.unaryOp(OpExprStrToLowercase)
}

// StrToUppercase converts all characters to uppercase
func (expr *ExprNode) StrToUppercase() *ExprNode {
	return expr.unaryOp(OpExprStrToUppercase)
}

// StrContains checks if string values contain a literal substring
func (expr *ExprNode) StrContains(pattern string) *ExprNode {
	return expr.unaryOpWithStringArgs(OpExprStrContains, pattern)
}

// StrStartsWith checks if string values start with a prefix
func (expr *ExprNode) StrStartsWith(prefix string) *ExprNode {
	return expr.unaryOpWithStringArgs(OpExprStrStartsWith, prefix)
}

// StrEndsWith checks if string values end with a suffix
func (expr *ExprNode) StrEndsWith(suffix string) *ExprNode {
	return expr.unaryOpWithStringArgs(OpExprStrEndsWith, suffix)
}

// StrSlice slices each string by start/length; negative length means to end
func (expr *ExprNode) StrSlice(start, length int64) *ExprNode {
	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			opcode: OpExprStrSlice,
			args: func() unsafe.Pointer {
				return unsafe.Pointer(&C.SliceArgs{
					start:  C.int64_t(start),
					length: C.int64_t(length),
				})
			},
		})),
	}
}

// StrReplace replaces occurrences of pattern with replacement (literal match by default)
// n < 0 replaces all occurrences; literal controls whether pattern is treated as literal
func (expr *ExprNode) StrReplace(pattern, replacement string, literal bool, n ...int64) *ExprNode {
	maxRepl := int64(-1)
	if len(n) > 0 {
		maxRepl = n[0]
	}

	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			opcode: OpExprStrReplace,
			args: func() unsafe.Pointer {
				return unsafe.Pointer(&C.ReplaceArgs{
					pattern:     makeRawStr(pattern),
					replacement: makeRawStr(replacement),
					literal:     C.bool(literal),
					n:           C.int64_t(maxRepl),
				})
			},
		})),
	}
}

// StrSplit splits strings by delimiter; n < 0 means unlimited splits
func (expr *ExprNode) StrSplit(delimiter string, n ...int64) *ExprNode {
	maxSplits := int64(-1)
	if len(n) > 0 {
		maxSplits = n[0]
	}

	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			opcode: OpExprStrSplit,
			args: func() unsafe.Pointer {
				return unsafe.Pointer(&C.SplitArgs{
					delimiter: makeRawStr(delimiter),
					n:         C.int64_t(maxSplits),
				})
			},
		})),
	}
}

// StrLenBytes returns the byte length of each string (faster than StrLen for ASCII)
func (expr *ExprNode) StrLenBytes() *ExprNode {
	return expr.unaryOp(OpExprStrLenBytes)
}

// StrStripChars removes leading and trailing characters
// If chars is empty, removes all whitespace
func (expr *ExprNode) StrStripChars(chars ...string) *ExprNode {
	pattern := ""
	if len(chars) > 0 {
		pattern = chars[0]
	}
	return expr.unaryOpWithStringArgs(OpExprStrStripChars, pattern)
}

// StrStripCharsStart removes leading characters (left strip)
// If chars is empty, removes leading whitespace
func (expr *ExprNode) StrStripCharsStart(chars ...string) *ExprNode {
	pattern := ""
	if len(chars) > 0 {
		pattern = chars[0]
	}
	return expr.unaryOpWithStringArgs(OpExprStrStripStart, pattern)
}

// StrStripCharsEnd removes trailing characters (right strip)
// If chars is empty, removes trailing whitespace
func (expr *ExprNode) StrStripCharsEnd(chars ...string) *ExprNode {
	pattern := ""
	if len(chars) > 0 {
		pattern = chars[0]
	}
	return expr.unaryOpWithStringArgs(OpExprStrStripEnd, pattern)
}

// StrStripPrefix removes exact prefix from strings
func (expr *ExprNode) StrStripPrefix(prefix string) *ExprNode {
	return expr.unaryOpWithStringArgs(OpExprStrStripPrefix, prefix)
}

// StrStripSuffix removes exact suffix from strings
func (expr *ExprNode) StrStripSuffix(suffix string) *ExprNode {
	return expr.unaryOpWithStringArgs(OpExprStrStripSuffix, suffix)
}

// StrReverse reverses each string
func (expr *ExprNode) StrReverse() *ExprNode {
	return expr.unaryOp(OpExprStrReverse)
}

// StrHead returns the first n characters of each string
// If n is negative, returns all characters except the last |n|
func (expr *ExprNode) StrHead(n int64) *ExprNode {
	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			opcode: OpExprStrHead,
			args: func() unsafe.Pointer {
				return unsafe.Pointer(&C.HeadTailArgs{
					n: C.int64_t(n),
				})
			},
		})),
	}
}

// StrTail returns the last n characters of each string
// If n is negative, returns all characters except the first |n|
func (expr *ExprNode) StrTail(n int64) *ExprNode {
	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			opcode: OpExprStrTail,
			args: func() unsafe.Pointer {
				return unsafe.Pointer(&C.HeadTailArgs{
					n: C.int64_t(n),
				})
			},
		})),
	}
}

// StrPadStart pads strings on the left to reach target length
func (expr *ExprNode) StrPadStart(length int64, fillChar ...byte) *ExprNode {
	fill := byte(' ')
	if len(fillChar) > 0 {
		fill = fillChar[0]
	}
	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			opcode: OpExprStrPadStart,
			args: func() unsafe.Pointer {
				return unsafe.Pointer(&C.PadArgs{
					length:    C.int64_t(length),
					fill_char: C.char(fill),
				})
			},
		})),
	}
}

// StrPadEnd pads strings on the right to reach target length
func (expr *ExprNode) StrPadEnd(length int64, fillChar ...byte) *ExprNode {
	fill := byte(' ')
	if len(fillChar) > 0 {
		fill = fillChar[0]
	}
	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			opcode: OpExprStrPadEnd,
			args: func() unsafe.Pointer {
				return unsafe.Pointer(&C.PadArgs{
					length:    C.int64_t(length),
					fill_char: C.char(fill),
				})
			},
		})),
	}
}

// StrZfill pads strings with zeros on the left to reach target length
func (expr *ExprNode) StrZfill(length int64) *ExprNode {
	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			opcode: OpExprStrZfill,
			args: func() unsafe.Pointer {
				return unsafe.Pointer(&C.HeadTailArgs{
					n: C.int64_t(length),
				})
			},
		})),
	}
}

// Window Functions

// Over applies a window context to the expression with partition columns
// Usage: Col("salary").Sum().Over("department") or Col("salary").Sum().Over("department", "region")
func (expr *ExprNode) Over(partitionColumns ...string) *ExprNode {
	if len(partitionColumns) == 0 {
		return &ExprNode{ops: combine(expr.ops, single(errOp("Over() requires at least one partition column")))}
	}

	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			opcode: OpExprOver,
			args: func() unsafe.Pointer {
				// Closure captures partitionColumns, keeping them alive
				rawColumns := make([]C.RawStr, len(partitionColumns))
				for i, col := range partitionColumns {
					rawColumns[i] = makeRawStr(col)
				}

				return unsafe.Pointer(&C.WindowArgs{
					partition_columns: &rawColumns[0],
					partition_count:   C.int(len(partitionColumns)),
					order_columns:     nil, // No ordering for basic Over()
					order_count:       0,
				})
			},
		})),
	}
}

// OverOrdered applies a window context with both partition and order columns
// Usage: Col("salary").Rank().OverOrdered([]string{"department"}, []string{"salary"})
func (expr *ExprNode) OverOrdered(partitionColumns []string, orderColumns []string) *ExprNode {
	if len(partitionColumns) == 0 {
		return &ExprNode{ops: combine(expr.ops, single(errOp("OverOrdered() requires at least one partition column")))}
	}
	if len(orderColumns) == 0 {
		return &ExprNode{ops: combine(expr.ops, single(errOp("OverOrdered() requires at least one order column")))}
	}

	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			opcode: OpExprOver,
			args: func() unsafe.Pointer {
				// Closure captures both column arrays, keeping them alive
				rawPartitionColumns := make([]C.RawStr, len(partitionColumns))
				for i, col := range partitionColumns {
					rawPartitionColumns[i] = makeRawStr(col)
				}

				rawOrderColumns := make([]C.RawStr, len(orderColumns))
				for i, col := range orderColumns {
					rawOrderColumns[i] = makeRawStr(col)
				}

				return unsafe.Pointer(&C.WindowArgs{
					partition_columns: &rawPartitionColumns[0],
					partition_count:   C.int(len(partitionColumns)),
					order_columns:     &rawOrderColumns[0],
					order_count:       C.int(len(orderColumns)),
				})
			},
		})),
	}
}

// Ranking Functions

// Rank returns the rank of each row within its partition
// Requires ordering - use with OverOrdered()
func Rank() *ExprNode {
	return &ExprNode{
		ops: single(Operation{
			opcode: OpExprRank,
			args:   noArgs,
		}),
	}
}

// DenseRank returns the dense rank of each row within its partition
// Requires ordering - use with OverOrdered()
func DenseRank() *ExprNode {
	return &ExprNode{
		ops: single(Operation{
			opcode: OpExprDenseRank,
			args:   noArgs,
		}),
	}
}

// RowNumber returns the row number within each partition
func RowNumber() *ExprNode {
	return &ExprNode{
		ops: single(Operation{
			opcode: OpExprRowNumber,
			args:   noArgs,
		}),
	}
}

// Offset Functions

// Lag returns the value from a previous row within the partition
// offset: number of rows to look back (positive integer)
// Requires ordering - use with OverOrdered()
func (expr *ExprNode) Lag(offset int) *ExprNode {
	if offset <= 0 {
		return &ExprNode{ops: combine(expr.ops, single(errOp("Lag() offset must be positive")))}
	}

	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			opcode: OpExprLag,
			args: func() unsafe.Pointer {
				return unsafe.Pointer(&C.WindowOffsetArgs{
					offset: C.int(-offset), // Negative for looking back
				})
			},
		})),
	}
}

// Lead returns the value from a following row within the partition
// offset: number of rows to look ahead (positive integer)
// Requires ordering - use with OverOrdered()
func (expr *ExprNode) Lead(offset int) *ExprNode {
	if offset <= 0 {
		return &ExprNode{ops: combine(expr.ops, single(errOp("Lead() offset must be positive")))}
	}

	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			opcode: OpExprLead,
			args: func() unsafe.Pointer {
				return unsafe.Pointer(&C.WindowOffsetArgs{
					offset: C.int(offset), // Positive for looking ahead
				})
			},
		})),
	}
}

// Conditional Expressions (When/Then/Otherwise)

// When starts a conditional expression with a condition
// Usage: When(Col("age").Gt(Lit(30))).Then(Lit("senior")).Otherwise(Lit("junior"))
func When(condition *ExprNode) *ConditionalNode {
	return &ConditionalNode{
		ops: combine(
			condition.consumeOps(),
			single(Operation{
				opcode: OpExprWhen,
				args:   noArgs,
			}),
		),
	}
}

// Then pairs a value with the most recent When condition
func (c *ConditionalNode) Then(value *ExprNode) *ConditionalNode {
	return &ConditionalNode{
		ops: combine(
			c.ops,
			value.consumeOps(),
			single(Operation{
				opcode: OpExprThen,
				args:   noArgs,
			}),
		),
	}
}

// When adds another condition to the chain (chained conditionals)
// Usage: When(...).Then(...).When(...).Then(...).Otherwise(...)
func (c *ConditionalNode) When(condition *ExprNode) *ConditionalNode {
	return &ConditionalNode{
		ops: combine(
			c.ops,
			condition.consumeOps(),
			single(Operation{
				opcode: OpExprWhen,
				args:   noArgs,
			}),
		),
	}
}

// Otherwise provides the default value and returns the final ExprNode
func (c *ConditionalNode) Otherwise(value *ExprNode) *ExprNode {
	return &ExprNode{
		ops: combine(
			c.ops,
			value.consumeOps(),
			single(Operation{
				opcode: OpExprOtherwise,
				args:   noArgs,
			}),
		),
	}
}

// Cast Operations

// Cast casts the expression to the specified data type with default options (strict=true, wrap_numerical=false)
// Usage: Col("age").Cast(polars.Int64), Col("price").Cast(polars.Float32)
func (expr *ExprNode) Cast(dtype DataType) *ExprNode {
	return expr.CastWithOptions(dtype, true, false)
}

// CastStrict casts the expression with configurable strict mode
// strict=true: raise error on invalid cast, strict=false: produce null values on invalid cast
func (expr *ExprNode) CastStrict(dtype DataType, strict bool) *ExprNode {
	return expr.CastWithOptions(dtype, strict, false)
}

// CastWithOptions casts the expression with full control over casting behavior
// dtype: target data type
// strict: if true, raise error on invalid cast; if false, produce null values
// wrap_numerical: if true, numeric casts wrap overflowing values instead of marking as invalid
func (expr *ExprNode) CastWithOptions(dtype DataType, strict, wrap_numerical bool) *ExprNode {
	return &ExprNode{
		ops: combine(expr.ops, single(Operation{
			opcode: OpExprCast,
			args: func() unsafe.Pointer {
				return unsafe.Pointer(&C.CastArgs{
					dtype:          C.uint(dtype),
					strict:         C.bool(strict),
					wrap_numerical: C.bool(wrap_numerical),
				})
			},
		})),
	}
}
