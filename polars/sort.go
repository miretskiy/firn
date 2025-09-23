package polars

/*
#include "firn.h"
*/
import "C"
import "unsafe"

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

// convertSortFields converts Go SortField slice to C SortField array
func convertSortFields(fields []SortField) ([]C.SortField, func()) {
	if len(fields) == 0 {
		return nil, func() {}
	}

	cFields := make([]C.SortField, len(fields))
	var cleanup []func()

	for i, field := range fields {
		// Convert column name to RawStr
		columnData := unsafe.StringData(field.Column)
		cFields[i] = C.SortField{
			column: C.RawStr{
				data: (*C.char)(unsafe.Pointer(columnData)),
				len:  C.size_t(len(field.Column)),
			},
			direction:      C.SortDirection(field.Direction),
			nulls_ordering: C.NullsOrdering(field.NullsOrdering),
		}
	}

	return cFields, func() {
		// Cleanup function - in this case, no explicit cleanup needed
		// since we're using unsafe.StringData which doesn't allocate
		for _, cleanupFn := range cleanup {
			cleanupFn()
		}
	}
}
