package polars

/*
#include "turbo_polars.h"
*/
import "C"

// OpCode constants matching Rust OpCode enum
// IMPORTANT: When adding/changing opcodes in rust/src/opcodes.rs,
// update these constants to match the Rust enum values exactly!
const (
	// DataFrame operations
	OpNewEmpty   = 1
	OpReadCsv    = 2
	OpSelect     = 3
	OpSelectExpr = 4
	OpCount      = 5
	OpConcat     = 6
	OpWithColumn = 7
	OpFilterExpr = 8
	OpGroupBy    = 9
	OpAddNullRow = 10
	OpCollect    = 11
	OpAgg        = 12
	OpSort       = 13
	OpLimit      = 14

	// Expression operations (stack-based)
	OpExprColumn         = 100
	OpExprLiteral        = 101
	OpExprAdd            = 102
	OpExprSub            = 103
	OpExprMul            = 104
	OpExprDiv            = 105
	OpExprGt             = 106
	OpExprLt             = 107
	OpExprEq             = 108
	OpExprAnd            = 109
	OpExprOr             = 110
	OpExprNot            = 111
	OpExprSum            = 112
	OpExprMean           = 113
	OpExprMin            = 114
	OpExprMax            = 115
	OpExprStd            = 116
	OpExprVar            = 117
	OpExprMedian         = 118
	OpExprFirst          = 119
	OpExprLast           = 120
	OpExprNUnique        = 121
	OpExprCountExpr      = 122
	OpExprCountWithNulls = 123
	OpExprIsNull         = 124
	OpExprIsNotNull      = 125
	OpExprAlias          = 126
	OpExprStrLen         = 127
	OpExprStrContains    = 128
	OpExprStrStartsWith  = 129
	OpExprStrEndsWith    = 130
	OpExprStrToLowercase = 131
	OpExprStrToUppercase = 132

	// Error operation for fluent API error handling
	OpError = 999
)

// Context type constants matching Rust ContextType enum
const (
	ContextTypeDataFrame   = 1
	ContextTypeLazyFrame   = 2
	ContextTypeLazyGroupBy = 3
)

// Note: Sort direction and nulls ordering constants are defined directly 
// in sort.go using C.SORT_DIRECTION_* and C.NULLS_ORDERING_* constants
