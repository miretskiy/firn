#ifndef TURBO_POLARS_H
#define TURBO_POLARS_H

#include <stdlib.h>
#include <stdint.h>

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
} ReadCsvArgs;

typedef struct {
    // No arguments needed for count
} CountArgs;

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
} Result;

// Dispatch functions
Result dispatch_new_empty(uintptr_t handle, uintptr_t args);
Result dispatch_read_csv(uintptr_t handle, uintptr_t args);
Result dispatch_select(uintptr_t handle, uintptr_t args);
Result dispatch_group_by(uintptr_t handle, uintptr_t args);
Result dispatch_count(uintptr_t handle, uintptr_t args);
Result dispatch_filter_expr(uintptr_t handle, uintptr_t args);

// Expression dispatch functions
int expr_column(void* stack, uintptr_t args);
int expr_literal(void* stack, uintptr_t args);
int expr_gt(void* stack, uintptr_t args);
int expr_lt(void* stack, uintptr_t args);
int expr_eq(void* stack, uintptr_t args);

// Main execution function
Result execute_operations(uintptr_t handle, const Operation* operations, size_t count);
int release(uintptr_t handle);
void free_error(char* error_message);

// DataFrame introspection
char* dataframe_to_csv(uintptr_t handle);
char* dataframe_to_string(uintptr_t handle);

// Benchmark helper
int noop();

#endif // TURBO_POLARS_H
