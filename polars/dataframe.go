package polars

/*
#include "firn.h"
*/
import "C"
import (
	"errors"
	"fmt"
	"unsafe"
)

// Helper function to create RawStr from Go string (zero-copy)
func makeRawStr(s string) C.RawStr {
	if len(s) == 0 {
		return C.RawStr{data: nil, len: 0}
	}
	return C.RawStr{
		data: (*C.char)(unsafe.Pointer(unsafe.StringData(s))),
		len:  C.size_t(len(s)),
	}
}

// Operation represents a single DataFrame operation with opcode and args
type Operation struct {
	opcode uint32                // OpCode for the operation
	args   func() unsafe.Pointer // Lazy args allocation via closure (keeps references alive naturally)
	err    error                 // Error associated with this operation (if any)
}

// Helper functions for creating error operations

// errOp creates an Operation that represents an error
func errOp(message string) Operation {
	return Operation{
		opcode: 0, // Dummy opcode, error will be caught during execution
		args:   noArgs,
		err:    fmt.Errorf("%s", message),
	}
}

// errOpf creates an Operation with formatted error message
func errOpf(format string, args ...interface{}) Operation {
	return Operation{
		opcode: 0, // Dummy opcode, error will be caught during execution
		args:   noArgs,
		err:    fmt.Errorf(format, args...),
	}
}

// appendErrOp appends an error operation to a DataFrame and returns it
func (df *DataFrame) appendErrOp(message string) *DataFrame {
	df.operations = append(df.operations, errOp(message))
	return df
}

// appendErrOpf appends a formatted error operation to a DataFrame and returns it
func (df *DataFrame) appendErrOpf(format string, args ...interface{}) *DataFrame {
	df.operations = append(df.operations, errOpf(format, args...))
	return df
}

// DataFrame represents a Polars DataFrame with lazy operations
type DataFrame struct {
	handle     C.PolarsHandle // Handle with context type information
	operations []Operation    // Pending operations to execute
}

// Error represents a Polars operation error
type Error struct {
	Code    int
	Message string
	Frame   int
}

func (e *Error) Error() string {
	if e.Frame > 0 {
		return fmt.Sprintf("polars error %d at operation %d: %s", e.Code, e.Frame, e.Message)
	}
	return fmt.Sprintf("polars error %d: %s", e.Code, e.Message)
}

// NewDataFrame creates a new empty DataFrame
func NewDataFrame() *DataFrame {
	op := Operation{
		opcode: OpNewEmpty,
		args:   func() unsafe.Pointer { return unsafe.Pointer(&C.CountArgs{}) }, // Lazy allocation
	}

	return &DataFrame{
		handle:     C.PolarsHandle{handle: C.uintptr_t(0), context_type: C.uint32_t(0)}, // Lazy - no handle yet
		operations: []Operation{op},
	}
}

// ReadCSV creates a DataFrame from a CSV file with default options
// - has_header: true (assumes CSV has header row)
// - with_glob: true (enables glob pattern expansion for paths like "data_*.csv")
func ReadCSV(path string) *DataFrame {
	return ReadCSVWithOptions(path, true, true)
}

// ReadCSVWithOptions creates a DataFrame from a CSV file with configurable options
func ReadCSVWithOptions(path string, hasHeader bool, withGlob bool) *DataFrame {
	op := Operation{
		opcode: OpReadCsv,
		args: func() unsafe.Pointer {
			return unsafe.Pointer(&C.ReadCsvArgs{
				path:       makeRawStr(path), // path captured by closure
				has_header: C.bool(hasHeader),
				with_glob:  C.bool(withGlob),
			})
		},
	}

	return &DataFrame{
		handle:     C.PolarsHandle{handle: C.uintptr_t(0), context_type: C.uint32_t(0)}, // Lazy - no handle yet
		operations: []Operation{op},
	}
}

// ParquetOptions configures Parquet reading options
type ParquetOptions struct {
	Columns  []string // Optional column selection (nil = all columns)
	NRows    int      // Optional row limit (0 = all rows)
	Parallel bool     // Enable parallel reading
	WithGlob bool     // Whether to expand glob patterns
}

// ReadParquet creates a DataFrame from a Parquet file with default options
// - columns: all columns (no selection)
// - n_rows: all rows (no limit)
// - parallel: true (enables parallel reading)
// - with_glob: true (enables glob pattern expansion for paths like "data_*.parquet")
func ReadParquet(path string) *DataFrame {
	return ReadParquetWithOptions(path, ParquetOptions{
		Columns:  nil,
		NRows:    0,
		Parallel: true,
		WithGlob: true,
	})
}

// ReadParquetWithOptions creates a DataFrame from a Parquet file with configurable options
func ReadParquetWithOptions(path string, options ParquetOptions) *DataFrame {
	op := Operation{
		opcode: OpReadParquet,
		args: func() unsafe.Pointer {
			var columnsPtr *C.RawStr
			var columnCount C.size_t

			// Handle column selection if specified
			if len(options.Columns) > 0 {
				// Create RawStr array for columns
				rawStrs := make([]C.RawStr, len(options.Columns))
				for i, col := range options.Columns {
					rawStrs[i] = makeRawStr(col)
				}
				columnsPtr = &rawStrs[0]
				columnCount = C.size_t(len(options.Columns))
			}

			return unsafe.Pointer(&C.ReadParquetArgs{
				path:         makeRawStr(path), // path captured by closure
				columns:      columnsPtr,
				column_count: columnCount,
				n_rows:       C.size_t(options.NRows),
				parallel:     C.bool(options.Parallel),
				with_glob:    C.bool(options.WithGlob),
			})
		},
	}

	return &DataFrame{
		handle:     C.PolarsHandle{handle: C.uintptr_t(0), context_type: C.uint32_t(0)}, // Lazy - no handle yet
		operations: []Operation{op},
	}
}

// Execute materializes the DataFrame by executing the operation stack.
// Returns this DataFrame with updated handle, leaving operations cleared.
// Collect processes all accumulated operations and materializes the result
// This is where lazy operations are executed and the DataFrame is materialized
func (df *DataFrame) Collect() (*DataFrame, error) {
	// Add a Collect operation to the chain
	df.operations = append(df.operations, Operation{
		opcode: OpCollect,
		args:   noArgs,
	})

	return df.execute()
}

func (df *DataFrame) execute() (*DataFrame, error) {
	if len(df.operations) == 0 {
		return nil, errors.New("no operations to execute")
	}

	// Store the old handle for potential cleanup
	oldHandle := df.handle.handle

	// Defer cleanup of operations (always runs)
	defer func() {
		// Clear operations slice but keep capacity for reuse
		df.operations = df.operations[:0]
	}()

	// Convert Go operations to C operations, checking for errors
	cOps := make([]C.Operation, len(df.operations))
	for i, op := range df.operations {
		// Check if this operation has an error
		if op.err != nil {
			return nil, &Error{
				Code:    4, // ERROR_POLARS_OPERATION
				Message: op.err.Error(),
				Frame:   i,
			}
		}

		// Call the args function to get the actual args (lazy allocation)
		var argsPtr unsafe.Pointer
		if op.args != nil {
			argsPtr = op.args() // Direct unsafe.Pointer, no type switch needed!
		}

		cOps[i] = C.Operation{
			opcode: C.uint32_t(op.opcode),
			args:   C.uintptr_t(uintptr(argsPtr)),
		}
	}

	// Single FFI call with the entire operation array
	result := C.execute_operations(
		df.handle, // Pass the full PolarsHandle with context
		&cOps[0],
		C.size_t(len(cOps)),
	)

	if result.error_code != 0 {
		errorMsg := C.GoString(result.error_message)
		C.free_string(result.error_message)
		return nil, &Error{
			Code:    int(result.error_code),
			Message: errorMsg,
			Frame:   int(result.error_frame),
		}
	}

	// Update this DataFrame's handle to the new one
	df.handle = result.polars_handle

	// Release the old handle if it was valid (not 0) and different from new handle
	// This prevents memory leaks from intermediate DataFrames
	if oldHandle != 0 && oldHandle != df.handle.handle {
		releaseResult := C.release_dataframe(C.uintptr_t(oldHandle))
		if releaseResult != 0 {
			// Log the error but don't fail the operation since we got a valid new handle
			// In production, we might want to use a proper logger here
			_ = releaseResult // Ignore the error for now
		}
	}

	// Return this DataFrame (now with updated handle)
	return df, nil
}

// Select adds a select operation to the DataFrame using expressions or column names
// Strings are automatically converted to SQL expressions, ExprNodes are used as-is
// Example: df.Select("name", "salary * 1.1 as bonus", Col("age").Alias("years"))
func (df *DataFrame) Select(args ...any) *DataFrame {
	exprs := toExprNodes(args...)

	// Add all expression operations first
	for _, expr := range exprs {
		for exprOp := range expr.ops {
			df.operations = append(df.operations, exprOp)
		}
		// Consume the expression to prevent reuse
		expr.consume()
	}

	// Add the select_expr operation
	df.operations = append(df.operations, Operation{
		opcode: OpSelectExpr,
		args:   noArgs,
	})

	return df
}

// SelectExpr adds a select operation to the DataFrame using expressions
func (df *DataFrame) SelectExpr(exprs ...*ExprNode) *DataFrame {
	// Add all expression operations first
	for _, expr := range exprs {
		for exprOp := range expr.ops {
			df.operations = append(df.operations, exprOp)
		}
		// Consume the expression to prevent reuse
		expr.consume()
	}

	// Add the select_expr operation
	df.operations = append(df.operations, Operation{
		opcode: OpSelectExpr,
		args:   noArgs,
	})

	return df
}

// Count returns a DataFrame with a single row containing the count of rows
func (df *DataFrame) Count() *DataFrame {
	op := Operation{
		opcode: OpCount,
		args:   func() unsafe.Pointer { return unsafe.Pointer(&C.CountArgs{}) }, // Lazy allocation
	}

	df.operations = append(df.operations, op)
	return df
}

// Height returns the number of rows in the DataFrame as an integer
// This requires the DataFrame to be executed first
func (df *DataFrame) Height() (int, error) {
	if df.handle.handle == 0 {
		return 0, errors.New("DataFrame must be executed before calling Height()")
	}

	height := C.dataframe_height(df.handle.handle)
	return int(height), nil
}

// Concat concatenates multiple executed DataFrames vertically (union)
// All DataFrames must be executed before calling this function
func Concat(dataframes ...*DataFrame) *DataFrame {
	if len(dataframes) == 0 {
		return NewDataFrame() // Return empty DataFrame
	}

	// Create operation that will concatenate the DataFrames
	op := Operation{
		opcode: OpConcat,
		args: func() unsafe.Pointer {
			// Create array of handles
			handles := make([]C.uintptr_t, len(dataframes))
			for i, df := range dataframes {
				if df.handle.handle == 0 {
					// This will cause an error in Rust, which is what we want
					handles[i] = 0
				} else {
					handles[i] = df.handle.handle
				}
			}

			return unsafe.Pointer(&C.ConcatArgs{
				handles: (*C.uintptr_t)(unsafe.Pointer(&handles[0])),
				count:   C.size_t(len(handles)),
			})
		},
	}

	return &DataFrame{
		handle:     C.PolarsHandle{handle: C.uintptr_t(0), context_type: C.uint32_t(0)}, // Lazy - no handle yet
		operations: []Operation{op},
	}
}

// WithColumns adds computed columns to the DataFrame while keeping existing columns
// Strings are automatically converted to SQL expressions, ExprNodes are used as-is
// Example: df.WithColumns("salary * 1.1 as bonus", Col("age").Alias("years"))
func (df *DataFrame) WithColumns(args ...any) *DataFrame {
	exprs := toExprNodes(args...)

	// Add all expression operations first
	for _, expr := range exprs {
		for exprOp := range expr.ops {
			df.operations = append(df.operations, exprOp)
		}
		// Consume the expression to prevent reuse
		expr.consume()
	}

	// Add a single with_column operation (this consumes ALL expressions from the stack)
	df.operations = append(df.operations, Operation{
		opcode: OpWithColumn,
		args:   noArgs,
	})

	return df
}

// Filter applies an expression as a filter to the DataFrame
// Strings are automatically converted to SQL expressions, ExprNodes are used as-is
// Example: df.Filter("age > 30") or df.Filter(Col("age").Gt(Lit(30)))
func (df *DataFrame) Filter(arg any) *DataFrame {
	exprs := toExprNodes(arg)
	if len(exprs) != 1 {
		return df.appendErrOp("Filter() requires exactly one expression")
	}

	expr := exprs[0]
	op := Operation{
		opcode: OpFilterExpr,
		args: func() unsafe.Pointer {
			// Build C operation array directly from iterator (truly lazy!)
			cOps := make([]C.Operation, 0, 4) // Start with capacity 4, grow as needed

			for exprOp := range expr.ops {
				// Call the expression's args function to get the actual args
				var argsPtr unsafe.Pointer
				if exprOp.args != nil {
					argsPtr = exprOp.args() // Direct unsafe.Pointer, no type switch needed!
				}

				cOps = append(cOps, C.Operation{
					opcode: C.uint32_t(exprOp.opcode),
					args:   C.uintptr_t(uintptr(argsPtr)),
				})
			}

			return unsafe.Pointer(&C.FilterExprArgs{
				expr_ops:   &cOps[0],
				expr_count: C.size_t(len(cOps)),
			})
		},
	}

	df.operations = append(df.operations, op)
	return df
}

// NoopCGOCall calls a no-op Rust function to measure pure CGO overhead
func NoopCGOCall() {
	C.noop()
}

// GroupBy groups the DataFrame by the specified expressions or column names
// Strings are automatically converted to SQL expressions, ExprNodes are used as-is
// Returns a DataFrame in LazyGroupBy context that can be used with Agg()
// Example: df.GroupBy("department", "year(hire_date) as hire_year")
func (df *DataFrame) GroupBy(args ...any) *DataFrame {
	if len(args) == 0 {
		return df.appendErrOp("GroupBy() requires at least one expression")
	}

	exprs := toExprNodes(args...)

	// Add all expression operations first
	for _, expr := range exprs {
		for exprOp := range expr.ops {
			df.operations = append(df.operations, exprOp)
		}
		// Consume the expression to prevent reuse
		expr.consume()
	}

	// Add the group_by operation
	df.operations = append(df.operations, Operation{
		opcode: OpGroupBy,
		args:   noArgs,
	})

	return df
}

// Agg applies aggregation expressions to a grouped DataFrame
// Can only be called after GroupBy() - validates context before FFI call
// Strings are automatically converted to SQL expressions, ExprNodes are used as-is
// Example: df.GroupBy("department").Agg("avg(salary) as avg_salary", Col("age").Max().Alias("max_age"))
func (df *DataFrame) Agg(args ...any) *DataFrame {
	if len(args) == 0 {
		return df.appendErrOp("Agg() requires at least one expression")
	}

	exprs := toExprNodes(args...)

	// Add all expression operations first (like WithColumns)
	for _, expr := range exprs {
		for exprOp := range expr.ops {
			df.operations = append(df.operations, exprOp)
		}
		// Consume the expression to prevent reuse
		expr.consume()
	}

	// Add a single agg operation (this consumes ALL expressions from the stack)
	df.operations = append(df.operations, Operation{
		opcode: OpAgg,
		args:   noArgs,
	})

	return df
}

// Sort sorts the DataFrame by the specified columns (ascending order for now)
// columns: column names to sort by
// Sort sorts the DataFrame by the specified columns (simple ascending sort)
func (df *DataFrame) Sort(columns []string) *DataFrame {
	if len(columns) == 0 {
		return df.appendErrOp("Sort() requires at least one column")
	}

	// Convert to SortField array with ascending direction
	fields := make([]SortField, len(columns))
	for i, col := range columns {
		fields[i] = Asc(col)
	}

	return df.SortBy(fields)
}

// SortBy sorts the DataFrame by the specified sort fields
func (df *DataFrame) SortBy(fields []SortField) *DataFrame {
	if len(fields) == 0 {
		return df.appendErrOp("SortBy() requires at least one sort field")
	}

	op := Operation{
		opcode: OpSort,
		args: func() unsafe.Pointer {
			// Convert SortField slice to C array
			cFields := make([]C.SortField, len(fields))
			for i, field := range fields {
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

			return unsafe.Pointer(&C.SortArgs{
				fields:      &cFields[0],
				field_count: C.int(len(fields)),
			})
		},
	}

	df.operations = append(df.operations, op)
	return df
}

// Limit limits the DataFrame to the first n rows
func (df *DataFrame) Limit(n int) *DataFrame {
	if n <= 0 {
		return df.appendErrOp("Limit() requires n > 0")
	}

	op := Operation{
		opcode: OpLimit,
		args: func() unsafe.Pointer {
			return unsafe.Pointer(&C.LimitArgs{
				n: C.size_t(n),
			})
		},
	}

	df.operations = append(df.operations, op)
	return df
}

// addNullRowForTesting is an internal helper for testing null handling
// It adds a single row with null values for all columns
func (df *DataFrame) addNullRowForTesting() *DataFrame {
	df.operations = append(df.operations, Operation{
		opcode: OpAddNullRow,
		args:   noArgs,
	})
	return df
}

// Release manually releases the DataFrame resources
func (df *DataFrame) Release() error {
	if df.handle.handle == 0 {
		return nil // Already released or never executed
	}

	result := C.release_dataframe(df.handle.handle)
	if result != 0 {
		return errors.New("failed to release dataframe")
	}

	df.handle = C.PolarsHandle{} // Mark as released
	return nil
}

// ToCsv converts an executed DataFrame to a CSV string
func (df *DataFrame) ToCsv() (string, error) {
	if df.handle.handle == 0 {
		return "", errors.New("dataframe not executed - call Execute() first")
	}

	csvPtr := C.dataframe_to_csv(df.handle.handle)
	if csvPtr == nil {
		return "", errors.New("failed to convert dataframe to CSV")
	}

	csvString := C.GoString(csvPtr)
	C.free(unsafe.Pointer(csvPtr)) // Free C memory
	return csvString, nil
}

// String implements fmt.Stringer for DataFrame display
func (df *DataFrame) String() string {
	if df.handle.handle == 0 {
		if len(df.operations) == 0 {
			return "DataFrame{empty}"
		}
		return fmt.Sprintf("DataFrame{lazy: %d ops}", len(df.operations))
	}

	displayPtr := C.dataframe_to_string(df.handle.handle)
	if displayPtr == nil {
		return fmt.Sprintf("DataFrame{handle: %d, error: failed to get display}", df.handle.handle)
	}

	displayString := C.GoString(displayPtr)
	C.free(unsafe.Pointer(displayPtr))
	return displayString
}

// Query executes a SQL query on the DataFrame
// The DataFrame is registered as "df" table in the SQL context
// Example: df.Query("SELECT name, salary * 1.1 as new_salary FROM df WHERE age > 25")
func (df *DataFrame) Query(sql string) *DataFrame {
	op := Operation{
		opcode: OpQuery,
		args: func() unsafe.Pointer {
			args := &C.QueryArgs{
				sql: makeRawStr(sql),
			}
			return unsafe.Pointer(args)
		},
	}

	return &DataFrame{
		handle:     df.handle,
		operations: append(df.operations, op),
	}
}
