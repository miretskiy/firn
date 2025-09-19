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

// Operation represents a single DataFrame operation with function pointer and args
type Operation struct {
	funcPtr uintptr               // Pointer to dispatch function
	args    func() unsafe.Pointer // Lazy args allocation via closure (keeps references alive naturally)
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
		funcPtr: uintptr(unsafe.Pointer(C.dispatch_new_empty)),
		args:    func() unsafe.Pointer { return unsafe.Pointer(&C.CountArgs{}) }, // Lazy allocation
	}

	return &DataFrame{
		handle:     0, // Lazy - no handle yet
		operations: []Operation{op},
	}
}

// ReadCSV creates a DataFrame from a CSV file
func ReadCSV(path string) *DataFrame {
	op := Operation{
		funcPtr: uintptr(unsafe.Pointer(C.dispatch_read_csv)),
		args: func() unsafe.Pointer {
			return unsafe.Pointer(&C.ReadCsvArgs{
				path: makeRawStr(path), // path captured by closure
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
func (df *DataFrame) Execute() (*DataFrame, error) {
	if len(df.operations) == 0 {
		return nil, errors.New("no operations to execute")
	}

	// Defer cleanup of operations and GC references (even on error)
	defer func(handleToRelease Handle) {
		// Clear all GC references
		// Clear operations slice but keep capacity for reuse
		df.operations = df.operations[:0]

		// Release the old handle if it was valid (not 0)
		// This prevents memory leaks from intermediate DataFrames
		if handleToRelease != 0 {
			releaseResult := C.release(C.uintptr_t(handleToRelease))
			if releaseResult != 0 {
				// Log the error but don't fail the operation since we got a valid new handle
				// In production, we might want to use a proper logger here
				_ = releaseResult // Ignore the error for now
			}
		}
	}(df.handle)

	// Convert Go operations to C operations
	cOps := make([]C.Operation, len(df.operations))
	for i, op := range df.operations {
		// Call the args function to get the actual args (lazy allocation)
		var argsPtr unsafe.Pointer
		if op.args != nil {
			argsPtr = op.args() // Direct unsafe.Pointer, no type switch needed!
		}

		cOps[i] = C.Operation{
			func_ptr: C.uintptr_t(op.funcPtr),
			args:     C.uintptr_t(uintptr(argsPtr)),
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
		C.free_error(result.error_message)
		return nil, &Error{
			Code:    int(result.error_code),
			Message: errorMsg,
			Frame:   int(result.error_frame),
		}
	}

	// Update this DataFrame's handle to the new one
	df.handle = Handle(result.handle)

	// Return this DataFrame (now with updated handle)
	return df, nil
}

// Select adds a select operation to the DataFrame
func (df *DataFrame) Select(columns ...string) *DataFrame {
	op := Operation{
		funcPtr: uintptr(unsafe.Pointer(C.dispatch_select)),
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

// Count returns a DataFrame with a single row containing the count of rows
func (df *DataFrame) Count() *DataFrame {
	op := Operation{
		funcPtr: uintptr(unsafe.Pointer(C.dispatch_count)),
		args:    func() unsafe.Pointer { return unsafe.Pointer(&C.CountArgs{}) }, // Lazy allocation
	}

	df.operations = append(df.operations, op)
	return df
}

// Filter applies an expression as a filter to the DataFrame
func (df *DataFrame) Filter(expr *ExprNode) *DataFrame {
	// Use the expression's operations directly
	exprOps := expr.ops

	op := Operation{
		funcPtr: uintptr(unsafe.Pointer(C.dispatch_filter_expr)),
		args: func() unsafe.Pointer {
			// Build C operation array for the expression (lazy)
			cOps := make([]C.Operation, len(exprOps))
			for i, exprOp := range exprOps {
				// Call the expression's args function to get the actual args
				var argsPtr unsafe.Pointer
				if exprOp.args != nil {
					argsPtr = exprOp.args() // Direct unsafe.Pointer, no type switch needed!
				}

				cOps[i] = C.Operation{
					func_ptr: C.uintptr_t(exprOp.funcPtr),
					args:     C.uintptr_t(uintptr(argsPtr)),
				}
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

// Release manually releases the DataFrame resources
func (df *DataFrame) Release() error {
	if df.handle == 0 {
		return nil // Already released or never executed
	}

	result := C.release(C.uintptr_t(df.handle))
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
