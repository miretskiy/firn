package polars

/*
#include "firn.h"
*/
import "C"
import "unsafe"

// FromRecords creates a DataFrame from a slice of maps (records) - pure memory, no temp files
// Each map represents a row with column names as keys
// Supported types: int64, float64, string, bool, nil (for nulls)
//
// Example:
//
//	records := []map[string]any{
//	    {"name": "Alice", "age": int64(30), "salary": 50000.0},
//	    {"name": "Bob", "age": int64(25), "salary": 45000.0},
//	}
//	df := polars.FromRecords(records)
//	result, err := df.Collect()
func FromRecords(records []map[string]any) *DataFrame {
	if len(records) == 0 {
		return NewDataFrame().appendErrOp("FromRecords: records cannot be empty")
	}

	// Extract column names from the first record
	var columnNames []string
	for key := range records[0] {
		columnNames = append(columnNames, key)
	}

	if len(columnNames) == 0 {
		return NewDataFrame().appendErrOp("FromRecords: first record is empty")
	}

	// Convert to column-oriented format
	columns := make(map[string][]any)
	for _, colName := range columnNames {
		columns[colName] = make([]any, len(records))
	}

	for i, record := range records {
		for _, colName := range columnNames {
			columns[colName][i] = record[colName]
		}
	}

	return FromColumns(columns)
}

// FromColumns creates a DataFrame from column-oriented data - pure memory, no temp files
// Each key is a column name, each value is a slice of column values
// Supported types: int64, float64, string, bool, nil (for nulls)
//
// Example:
//
//	columns := map[string][]any{
//	    "name":   []any{"Alice", "Bob", "Charlie"},
//	    "age":    []any{int64(30), int64(25), int64(35)},
//	    "salary": []any{50000.0, 45000.0, 75000.0},
//	}
//	df := polars.FromColumns(columns)
//	result, err := df.Collect()
func FromColumns(columns map[string][]any) *DataFrame {
	if len(columns) == 0 {
		return NewDataFrame().appendErrOp("FromColumns: columns cannot be empty")
	}

	// Verify all columns have the same length
	var rowCount int
	var columnNames []string
	for colName, values := range columns {
		columnNames = append(columnNames, colName)
		if rowCount == 0 {
			rowCount = len(values)
		} else if len(values) != rowCount {
			return NewDataFrame().appendErrOpf(
				"FromColumns: all columns must have same length (got %d and %d)",
				rowCount, len(values))
		}
	}

	op := Operation{
		opcode: OpFromMemory,
		args: func() unsafe.Pointer {
			// Build C structures - must be captured in closure to keep alive
			cColumns := make([]C.ColumnData, len(columnNames))
			// Keep all values alive for the duration of FFI call
			allValues := make([][]C.ColumnValue, len(columnNames))

			for i, colName := range columnNames {
				values := columns[colName]
				cValues := make([]C.ColumnValue, len(values))

				for j, value := range values {
					cValues[j] = makeColumnValue(value)
				}

				allValues[i] = cValues

				cColumns[i] = C.ColumnData{
					name:   makeRawStr(colName),
					values: &cValues[0],
					len:    C.size_t(len(values)),
				}
			}

			return unsafe.Pointer(&C.FromMemoryArgs{
				columns:      &cColumns[0],
				column_count: C.size_t(len(columnNames)),
			})
		},
	}

	return &DataFrame{
		handle:     C.PolarsHandle{handle: C.uintptr_t(0), context_type: C.uint32_t(0)},
		operations: []Operation{op},
	}
}

// FromSQL creates a DataFrame using SQL VALUES syntax (for small datasets)
// Example:
//
//	df := polars.FromSQL(`
//	    SELECT * FROM (VALUES
//	        ('Alice', 30, 50000),
//	        ('Bob', 25, 45000)
//	    ) AS t(name, age, salary)
//	`)
func FromSQL(sql string) *DataFrame {
	return NewDataFrame().Query(sql)
}

// makeColumnValue converts a Go value to C ColumnValue
func makeColumnValue(value any) C.ColumnValue {
	if value == nil {
		return C.ColumnValue{
			value_type: 4, // null
		}
	}

	switch v := value.(type) {
	case int64:
		return C.ColumnValue{
			value_type: 0,
			int_value:  C.int64_t(v),
		}
	case float64:
		return C.ColumnValue{
			value_type:  1,
			float_value: C.double(v),
		}
	case string:
		return C.ColumnValue{
			value_type:   2,
			string_value: makeRawStr(v),
		}
	case bool:
		return C.ColumnValue{
			value_type: 3,
			bool_value: C.bool(v),
		}
	// Support other integer types
	case int:
		return C.ColumnValue{
			value_type: 0,
			int_value:  C.int64_t(v),
		}
	case int32:
		return C.ColumnValue{
			value_type: 0,
			int_value:  C.int64_t(v),
		}
	case float32:
		return C.ColumnValue{
			value_type:  1,
			float_value: C.double(v),
		}
	default:
		// Unknown type - treat as null
		return C.ColumnValue{
			value_type: 4, // null
		}
	}
}
