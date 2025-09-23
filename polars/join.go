package polars

/*
#include "firn.h"
*/
import "C"
import (
	"unsafe"
)

// JoinType represents the type of join operation
// Using C constants to keep in sync with Rust definitions
type JoinType = C.JoinType

const (
	JoinTypeInner = C.JoinTypeInner
	JoinTypeLeft  = C.JoinTypeLeft
	JoinTypeRight = C.JoinTypeRight
	JoinTypeOuter = C.JoinTypeOuter // Maps to Polars' Full join
	JoinTypeCross = C.JoinTypeCross
)

// JoinSpec represents the specification for a join operation
type JoinSpec struct {
	leftOn    []string
	rightOn   []string
	joinType  JoinType
	suffix    string
	coalesce  bool
}

// On creates a JoinSpec for joining on the same column names in both DataFrames
// This is the most common case, equivalent to SQL's ON clause
func On(columns ...string) JoinSpec {
	return JoinSpec{
		leftOn:   columns,
		rightOn:  columns, // Same columns on both sides
		joinType: JoinTypeInner, // Default to inner join
		suffix:   "",
		coalesce: false,
	}
}

// LeftOn creates a JoinSpec builder for specifying different left and right columns
func LeftOn(columns ...string) JoinSpecBuilder {
	return JoinSpecBuilder{
		spec: JoinSpec{
			leftOn:   columns,
			joinType: JoinTypeInner, // Default to inner join
			suffix:   "",
			coalesce: false,
		},
	}
}

// JoinSpecBuilder allows building complex join specifications
type JoinSpecBuilder struct {
	spec JoinSpec
}

// RightOn specifies the right-side columns for the join
func (b JoinSpecBuilder) RightOn(columns ...string) JoinSpec {
	b.spec.rightOn = columns
	return b.spec
}

// WithType sets the join type
func (spec JoinSpec) WithType(joinType JoinType) JoinSpec {
	spec.joinType = joinType
	return spec
}

// WithSuffix sets the suffix for duplicate column names
func (spec JoinSpec) WithSuffix(suffix string) JoinSpec {
	spec.suffix = suffix
	return spec
}

// WithCoalesce enables coalescing of join columns
func (spec JoinSpec) WithCoalesce(coalesce bool) JoinSpec {
	spec.coalesce = coalesce
	return spec
}

// Join performs a join operation with another DataFrame
func (df *DataFrame) Join(other *DataFrame, spec JoinSpec) *DataFrame {
	// Validate inputs
	if other == nil {
		return df.appendErrOp("Join: other DataFrame cannot be nil")
	}
	
	if len(spec.leftOn) == 0 || len(spec.rightOn) == 0 {
		return df.appendErrOp("Join: join columns cannot be empty")
	}
	
	if len(spec.leftOn) != len(spec.rightOn) {
		return df.appendErrOpf("Join: left columns (%d) and right columns (%d) must have same count", 
			len(spec.leftOn), len(spec.rightOn))
	}

	// We need the other DataFrame to be executed to get its handle
	if other.handle.handle == 0 {
		return df.appendErrOp("Join: other DataFrame must be executed first (call Collect())")
	}

	op := Operation{
		opcode: OpJoin,
		args: func() unsafe.Pointer {
			// Convert left column names to RawStr array
			leftRawStrs := make([]C.RawStr, len(spec.leftOn))
			for i, col := range spec.leftOn {
				leftRawStrs[i] = makeRawStr(col)
			}

			// Convert right column names to RawStr array  
			rightRawStrs := make([]C.RawStr, len(spec.rightOn))
			for i, col := range spec.rightOn {
				rightRawStrs[i] = makeRawStr(col)
			}

			return unsafe.Pointer(&C.JoinArgs{
				other_handle:  C.uintptr_t(other.handle.handle),
				left_on:      (*C.RawStr)(unsafe.Pointer(&leftRawStrs[0])),
				right_on:     (*C.RawStr)(unsafe.Pointer(&rightRawStrs[0])),
				column_count: C.uintptr_t(len(spec.leftOn)),
				how:          C.JoinType(spec.joinType),
				suffix:       makeRawStr(spec.suffix),
				coalesce:     C.bool(spec.coalesce),
			})
		},
	}

	df.operations = append(df.operations, op)
	return df
}

// Convenience methods for common join types

// InnerJoin performs an inner join on the specified columns
func (df *DataFrame) InnerJoin(other *DataFrame, columns ...string) *DataFrame {
	return df.Join(other, On(columns...).WithType(JoinTypeInner))
}

// LeftJoin performs a left join on the specified columns
func (df *DataFrame) LeftJoin(other *DataFrame, columns ...string) *DataFrame {
	return df.Join(other, On(columns...).WithType(JoinTypeLeft))
}

// RightJoin performs a right join on the specified columns
func (df *DataFrame) RightJoin(other *DataFrame, columns ...string) *DataFrame {
	return df.Join(other, On(columns...).WithType(JoinTypeRight))
}

// OuterJoin performs an outer (full) join on the specified columns
func (df *DataFrame) OuterJoin(other *DataFrame, columns ...string) *DataFrame {
	return df.Join(other, On(columns...).WithType(JoinTypeOuter))
}

// CrossJoin performs a cross join (Cartesian product)
func (df *DataFrame) CrossJoin(other *DataFrame) *DataFrame {
	// Validate inputs
	if other == nil {
		return df.appendErrOp("CrossJoin: other DataFrame cannot be nil")
	}

	// We need the other DataFrame to be executed to get its handle
	if other.handle.handle == 0 {
		return df.appendErrOp("CrossJoin: other DataFrame must be executed first (call Collect())")
	}

	op := Operation{
		opcode: OpJoin,
		args: func() unsafe.Pointer {
			// Cross join doesn't use join columns, so pass empty arrays
			return unsafe.Pointer(&C.JoinArgs{
				other_handle:  C.uintptr_t(other.handle.handle),
				left_on:      nil, // No join columns for cross join
				right_on:     nil, // No join columns for cross join
				column_count: C.uintptr_t(0), // No columns
				how:          C.JoinType(JoinTypeCross),
				suffix:       makeRawStr(""),
				coalesce:     C.bool(false),
			})
		},
	}

	df.operations = append(df.operations, op)
	return df
}
