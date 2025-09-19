#ifndef TURBO_POLARS_H
#define TURBO_POLARS_H

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

typedef struct {
    RawStr* columns;
    int column_count;
} GroupByArgs;

typedef struct {
    RawStr path;
    bool has_header;  // Whether CSV has header row
    bool with_glob;   // Whether to enable glob pattern expansion
} ReadCsvArgs;

typedef struct {
    // No arguments needed for count
} CountArgs;

typedef struct {
    uintptr_t* handles; // Array of DataFrame handles
    size_t count;       // Number of handles
} ConcatArgs;

typedef struct {
    RawStr name; // Column alias name
} AliasArgs;

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

// Generic operation structure with function pointer and args
typedef struct {
    uintptr_t func_ptr;    // Function pointer as uintptr_t
    uintptr_t args;        // Pointer to operation-specific args as uintptr_t
} Operation;

// Filter with expression arguments
typedef struct {
    Operation* expr_ops;  // Note: using Operation instead of ExprOp
    size_t expr_count;
} FilterExprArgs;

typedef struct {
    uintptr_t handle;
    int error_code;
    char* error_message;
    size_t error_frame;
} FfiResult;

// Dispatch functions
FfiResult dispatch_new_empty(uintptr_t handle, uintptr_t args);
FfiResult dispatch_read_csv(uintptr_t handle, uintptr_t args);
FfiResult dispatch_select(uintptr_t handle, uintptr_t args);
FfiResult dispatch_group_by(uintptr_t handle, uintptr_t args);
FfiResult dispatch_count(uintptr_t handle, uintptr_t args);
FfiResult dispatch_concat(uintptr_t handle, uintptr_t args);
FfiResult dispatch_with_column(uintptr_t handle, uintptr_t args);

// DataFrame introspection functions
size_t dataframe_height(uintptr_t handle);
FfiResult dispatch_filter_expr(uintptr_t handle, uintptr_t args);

// Expression dispatch functions
int expr_column(void* stack, uintptr_t args);
int expr_literal(void* stack, uintptr_t args);
int expr_gt(void* stack, uintptr_t args);
int expr_lt(void* stack, uintptr_t args);
int expr_eq(void* stack, uintptr_t args);

// Arithmetic operations
int expr_add(void* stack, uintptr_t args);
int expr_sub(void* stack, uintptr_t args);
int expr_mul(void* stack, uintptr_t args);
int expr_div(void* stack, uintptr_t args);

// Boolean operations
int expr_and(void* stack, uintptr_t args);
int expr_or(void* stack, uintptr_t args);
int expr_not(void* stack, uintptr_t args);

// Expression utility operations
int expr_alias(void* stack, uintptr_t args);

// Main execution function
FfiResult execute_operations(uintptr_t handle, const Operation* operations, size_t count);
int release_dataframe(uintptr_t handle);
void free_string(char* error_message);

// DataFrame introspection
char* dataframe_to_csv(uintptr_t handle);
char* dataframe_to_string(uintptr_t handle);

// Benchmark helper
int noop();

#endif // TURBO_POLARS_H
