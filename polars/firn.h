#ifndef FIRN_H
#define FIRN_H

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

// Raw string representation for zero-copy Go memory access
typedef struct {
    const char* data;
    size_t len;
} RawStr;

// Operation-specific argument structs
typedef struct {
    RawStr* columns;
    int column_count;
} SelectArgs;


// Removed AggArgs - will be reimplemented with proper context handling

typedef struct {
    RawStr path;
    bool has_header;  // Whether CSV has header row
    bool with_glob;   // Whether to enable glob pattern expansion
} ReadCsvArgs;

typedef struct {
    uintptr_t* handles; // Array of DataFrame handles
    size_t count;       // Number of handles
} ConcatArgs;

typedef struct {
    RawStr name; // Column alias name
} AliasArgs;

typedef struct {
    unsigned char ddof; // Delta degrees of freedom (0=population, 1=sample)
} AggregationArgs;

typedef struct {
    bool include_nulls; // Whether to include null values in count
} CountArgs;

typedef struct {
    RawStr pattern; // Pattern/string for operations like contains, starts_with, ends_with
} StringArgs;

// Sort direction constants (matching Rust SortDirection enum)
#define SORT_DIRECTION_ASCENDING 0
#define SORT_DIRECTION_DESCENDING 1

// Nulls ordering constants (matching Rust NullsOrdering enum)
#define NULLS_ORDERING_FIRST 0
#define NULLS_ORDERING_LAST 1

// Sort direction for individual columns
typedef enum {
    SortDirectionAscending = SORT_DIRECTION_ASCENDING,
    SortDirectionDescending = SORT_DIRECTION_DESCENDING
} SortDirection;

// Nulls ordering options
typedef enum {
    NullsOrderingFirst = NULLS_ORDERING_FIRST,
    NullsOrderingLast = NULLS_ORDERING_LAST
} NullsOrdering;

// A single sort field with column name, direction, and nulls ordering
typedef struct {
    RawStr column;
    SortDirection direction;
    NullsOrdering nulls_ordering;
} SortField;

// Arguments for sort operations with full directionality support
typedef struct {
    SortField* fields;
    int field_count;
} SortArgs;

typedef struct {
    size_t n;            // Number of rows to limit to
} LimitArgs;

typedef struct {
    RawStr sql;
} QueryArgs;

typedef struct {
    RawStr sql;
} SqlExprArgs;

// Window function arguments
typedef struct {
    RawStr* partition_columns;
    int partition_count;
    RawStr* order_columns;     // Optional ordering columns
    int order_count;
} WindowArgs;

typedef struct {
    int offset;  // For Lag/Lead functions (positive for Lead, negative for Lag)
} WindowOffsetArgs;

// Centralized literal abstraction - handles all value types
typedef struct {
    int value_type;       // 0=int, 1=float, 2=string, 3=bool
    long long int_value;
    double float_value;
    RawStr string_value;
    _Bool bool_value;
} Literal;

// Arguments for expression operations
typedef struct {
    RawStr name;
} ColumnArgs;

typedef struct {
    Literal literal;
} LiteralArgs;

// Generic operation structure with opcode and args
typedef struct {
    uint32_t opcode;       // OpCode for the operation
    uintptr_t args;        // Pointer to operation-specific args as uintptr_t
} Operation;

// Filter with expression arguments
typedef struct {
    Operation* expr_ops;  // Note: using Operation instead of ExprOp
    size_t expr_count;
} FilterExprArgs;

// Enhanced handle that tracks both the handle and its type
typedef struct {
    uintptr_t handle;
    uint32_t context_type; // ContextType as u32 for C compatibility
} PolarsHandle;

typedef struct {
    PolarsHandle polars_handle; // Handle with context type
    int error_code;
    char* error_message;
    size_t error_frame;
} FfiResult;

// Core FFI functions - these are the only functions called from Go
FfiResult execute_operations(PolarsHandle handle, const Operation* operations, size_t count);
int release_dataframe(uintptr_t handle);
void free_string(char* error_message);

// DataFrame introspection
size_t dataframe_height(uintptr_t handle);
char* dataframe_to_csv(uintptr_t handle);
char* dataframe_to_string(uintptr_t handle);

// Testing and benchmarking helpers
FfiResult dispatch_add_null_row(uintptr_t handle, uintptr_t args);
int noop();

// Expression operations are now handled internally via opcode dispatch
// All expressions are processed through execute_operations() with opcodes

#endif // FIRN_H
