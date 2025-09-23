package polars

/*
#include "firn.h"
*/
import "C"

// SortField represents a column to sort by with direction and nulls ordering
type SortField struct {
	Column        string
	Direction     SortDirection
	NullsOrdering NullsOrdering
}

// SortDirection represents the sort order for a column
type SortDirection int

const (
	Ascending  SortDirection = C.SORT_DIRECTION_ASCENDING
	Descending SortDirection = C.SORT_DIRECTION_DESCENDING
)

// NullsOrdering represents how null values should be ordered
type NullsOrdering int

const (
	NullsFirst NullsOrdering = C.NULLS_ORDERING_FIRST
	NullsLast  NullsOrdering = C.NULLS_ORDERING_LAST
)

// Asc creates a SortField for ascending order with nulls last (default)
func Asc(column string) SortField {
	return SortField{
		Column:        column,
		Direction:     Ascending,
		NullsOrdering: NullsLast,
	}
}

// Desc creates a SortField for descending order with nulls last (default)
func Desc(column string) SortField {
	return SortField{
		Column:        column,
		Direction:     Descending,
		NullsOrdering: NullsLast,
	}
}

// AscNullsFirst creates a SortField for ascending order with nulls first
func AscNullsFirst(column string) SortField {
	return SortField{
		Column:        column,
		Direction:     Ascending,
		NullsOrdering: NullsFirst,
	}
}

// DescNullsFirst creates a SortField for descending order with nulls first
func DescNullsFirst(column string) SortField {
	return SortField{
		Column:        column,
		Direction:     Descending,
		NullsOrdering: NullsFirst,
	}
}

// String returns a string representation of the sort direction
func (d SortDirection) String() string {
	switch d {
	case Ascending:
		return "ASC"
	case Descending:
		return "DESC"
	default:
		return "UNKNOWN"
	}
}

// String returns a string representation of the sort field
func (sf SortField) String() string {
	return sf.Column + " " + sf.Direction.String()
}
