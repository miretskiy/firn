package polars

/*
#include "firn.h"
*/
import "C"

// OpCode constants matching Rust OpCode enum
// IMPORTANT: When adding/changing opcodes in rust/src/opcodes.rs,
// update these constants to match the Rust enum values exactly!
const (
	// DataFrame operations
	OpNewEmpty    = 1
	OpReadCsv     = 2
	OpReadParquet = 3
	OpSelect      = 4
	OpSelectExpr  = 5
	OpCount       = 6
	OpConcat      = 7
	OpWithColumn  = 8
	OpFilterExpr  = 9
	OpGroupBy     = 10
	OpAddNullRow  = 11
	OpCollect     = 12
	OpAgg         = 13
	OpSort        = 14
	OpLimit       = 15
	OpQuery       = 16
	OpJoin        = 17
	OpFromMemory  = 18

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
	OpExprCount          = 122
	OpExprCountNulls     = 123
	OpExprIsNull         = 124
	OpExprIsNotNull      = 125
	OpExprAlias          = 126
	OpExprStrLen         = 127
	OpExprStrContains    = 128
	OpExprStrStartsWith  = 129
	OpExprStrEndsWith    = 130
	OpExprStrToLowercase = 131
	OpExprStrToUppercase = 132
	OpExprSql            = 133
	OpExprStrSlice       = 134
	OpExprStrReplace     = 135
	OpExprStrSplit       = 136

	// Advanced string operations (Tier 2)
	OpExprStrLenBytes    = 137
	OpExprStrStripChars  = 138
	OpExprStrStripStart  = 139
	OpExprStrStripEnd    = 140
	OpExprStrStripPrefix = 170
	OpExprStrStripSuffix = 171
	OpExprStrReverse     = 173
	OpExprStrHead        = 174
	OpExprStrTail        = 175
	OpExprStrPadStart    = 176
	OpExprStrPadEnd      = 177
	OpExprStrZfill       = 178

	// Window function operations
	OpExprOver      = 180 // Applies window context to previous expression
	OpExprRank      = 181 // Rank() function
	OpExprDenseRank = 182 // DenseRank() function
	OpExprRowNumber = 183 // RowNumber() function
	OpExprLag       = 184 // Lag(n) function
	OpExprLead      = 185 // Lead(n) function

	// Conditional expressions (When/Then/Otherwise)
	OpExprWhen      = 190 // Start conditional chain
	OpExprThen      = 191 // Pair with most recent When
	OpExprOtherwise = 192 // Finalize conditional chain

	// Cast operations
	OpExprCast = 200 // Cast expression to different data type

	// Error operation for fluent API error handling
	OpError = 999
)

// Note: Sort direction and nulls ordering constants are defined directly
// in sort.go using C.SORT_DIRECTION_* and C.NULLS_ORDERING_* constants
