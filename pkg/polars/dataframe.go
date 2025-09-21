package polars

/*
#cgo LDFLAGS: -L../../lib -lturbo_polars
#include "turbo_polars.h"
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

// Handle represents an opaque handle to a Rust DataFrame
type Handle uintptr

// DataFrame represents a Polars DataFrame with lazy operations
type DataFrame struct {
	handle     Handle      // Opaque handle to Rust DataFrame (0 if lazy)
	operations []Operation // Pending operations to execute
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
		handle:     0, // Lazy - no handle yet
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
		handle:     0, // Lazy - no handle yet
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
	oldHandle := df.handle

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
			error:  0, // No error for this operation
		}
	}

	// Single FFI call with the entire operation array
	result := C.execute_operations(
		C.uintptr_t(df.handle),
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
	df.handle = Handle(result.polars_handle.handle)

	// Release the old handle if it was valid (not 0) and different from new handle
	// This prevents memory leaks from intermediate DataFrames
	if oldHandle != 0 && oldHandle != df.handle {
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

// Select adds a select operation to the DataFrame using column names
func (df *DataFrame) Select(columns ...string) *DataFrame {
	op := Operation{
		opcode: OpSelect,
		args: func() unsafe.Pointer {
			// Closure captures columns, keeping them alive
			rawColumns := make([]C.RawStr, len(columns))
			for i, col := range columns {
				rawColumns[i] = makeRawStr(col)
			}

			return unsafe.Pointer(&C.SelectArgs{
				columns:      &rawColumns[0],
				column_count: C.int(len(columns)),
			})
		},
	}

	df.operations = append(df.operations, op)
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
	if df.handle == 0 {
		return 0, errors.New("DataFrame must be executed before calling Height()")
	}

	height := C.dataframe_height(C.uintptr_t(df.handle))
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
				if df.handle == 0 {
					// This will cause an error in Rust, which is what we want
					handles[i] = 0
				} else {
					handles[i] = C.uintptr_t(df.handle)
				}
			}

			return unsafe.Pointer(&C.ConcatArgs{
				handles: (*C.uintptr_t)(unsafe.Pointer(&handles[0])),
				count:   C.size_t(len(handles)),
			})
		},
	}

	return &DataFrame{
		handle:     0, // Lazy - no handle yet
		operations: []Operation{op},
	}
}

// WithColumns adds computed columns to the DataFrame while keeping existing columns
func (df *DataFrame) WithColumns(exprs ...*ExprNode) *DataFrame {
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
func (df *DataFrame) Filter(expr *ExprNode) *DataFrame {
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
					error:  0, // No error for this operation
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

// GroupBy groups the DataFrame by the specified columns
// This is a complete operation that returns a grouped DataFrame with count
func (df *DataFrame) GroupBy(columns ...string) *DataFrame {
	op := Operation{
		opcode: OpGroupBy,
		args: func() unsafe.Pointer {
			// Closure captures columns, keeping them alive
			rawColumns := make([]C.RawStr, len(columns))
			for i, col := range columns {
				rawColumns[i] = makeRawStr(col)
			}

			return unsafe.Pointer(&C.GroupByArgs{
				columns:      &rawColumns[0],
				column_count: C.int(len(columns)),
			})
		},
	}

	df.operations = append(df.operations, op)
	return df
}

// Removed Agg method - will be reimplemented with proper context handling

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
	if df.handle == 0 {
		return nil // Already released or never executed
	}

	result := C.release_dataframe(C.uintptr_t(df.handle))
	if result != 0 {
		return errors.New("failed to release dataframe")
	}

	df.handle = 0 // Mark as released
	return nil
}

// ToCsv converts an executed DataFrame to a CSV string
func (df *DataFrame) ToCsv() (string, error) {
	if df.handle == 0 {
		return "", errors.New("dataframe not executed - call Execute() first")
	}

	csvPtr := C.dataframe_to_csv(C.uintptr_t(df.handle))
	if csvPtr == nil {
		return "", errors.New("failed to convert dataframe to CSV")
	}

	csvString := C.GoString(csvPtr)
	C.free(unsafe.Pointer(csvPtr)) // Free C memory
	return csvString, nil
}

// String implements fmt.Stringer for DataFrame display
func (df *DataFrame) String() string {
	if df.handle == 0 {
		if len(df.operations) == 0 {
			return "DataFrame{empty}"
		}
		return fmt.Sprintf("DataFrame{lazy: %d ops}", len(df.operations))
	}

	displayPtr := C.dataframe_to_string(C.uintptr_t(df.handle))
	if displayPtr == nil {
		return fmt.Sprintf("DataFrame{handle: %d, error: failed to get display}", df.handle)
	}

	displayString := C.GoString(displayPtr)
	C.free(unsafe.Pointer(displayPtr))
	return displayString
}
