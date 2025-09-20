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
FfiResult dispatch_select_expr(uintptr_t handle, uintptr_t args);
FfiResult dispatch_group_by(uintptr_t handle, uintptr_t args);
FfiResult dispatch_count(uintptr_t handle, uintptr_t args);
FfiResult dispatch_concat(uintptr_t handle, uintptr_t args);
FfiResult dispatch_with_column(uintptr_t handle, uintptr_t args);

// DataFrame introspection functions
size_t dataframe_height(uintptr_t handle);
FfiResult dispatch_filter_expr(uintptr_t handle, uintptr_t args);

// Expression dispatch functions
FfiResult expr_column(uintptr_t handle, uintptr_t context);
FfiResult expr_literal(uintptr_t handle, uintptr_t context);
FfiResult expr_gt(uintptr_t handle, uintptr_t context);
FfiResult expr_lt(uintptr_t handle, uintptr_t context);
FfiResult expr_eq(uintptr_t handle, uintptr_t context);

// Arithmetic operations
FfiResult expr_add(uintptr_t handle, uintptr_t context);
FfiResult expr_sub(uintptr_t handle, uintptr_t context);
FfiResult expr_mul(uintptr_t handle, uintptr_t context);
FfiResult expr_div(uintptr_t handle, uintptr_t context);

// Boolean operations
FfiResult expr_and(uintptr_t handle, uintptr_t context);
FfiResult expr_or(uintptr_t handle, uintptr_t context);
FfiResult expr_not(uintptr_t handle, uintptr_t context);

// Aggregation operations
FfiResult expr_sum(uintptr_t handle, uintptr_t context);
FfiResult expr_mean(uintptr_t handle, uintptr_t context);
FfiResult expr_min(uintptr_t handle, uintptr_t context);
FfiResult expr_max(uintptr_t handle, uintptr_t context);
FfiResult expr_std(uintptr_t handle, uintptr_t context);
FfiResult expr_var(uintptr_t handle, uintptr_t context);
FfiResult expr_median(uintptr_t handle, uintptr_t context);
FfiResult expr_first(uintptr_t handle, uintptr_t context);
FfiResult expr_last(uintptr_t handle, uintptr_t context);
FfiResult expr_nunique(uintptr_t handle, uintptr_t context);
FfiResult expr_count(uintptr_t handle, uintptr_t context);

// Null checking operations
FfiResult expr_is_null(uintptr_t handle, uintptr_t context);
FfiResult expr_is_not_null(uintptr_t handle, uintptr_t context);

// String operations
FfiResult expr_str_len(uintptr_t handle, uintptr_t context);
FfiResult expr_str_contains(uintptr_t handle, uintptr_t context);
FfiResult expr_str_starts_with(uintptr_t handle, uintptr_t context);
FfiResult expr_str_ends_with(uintptr_t handle, uintptr_t context);
FfiResult expr_str_to_lowercase(uintptr_t handle, uintptr_t context);
FfiResult expr_str_to_uppercase(uintptr_t handle, uintptr_t context);

// Expression utility operations
FfiResult expr_alias(uintptr_t handle, uintptr_t context);

// Main execution function
FfiResult execute_operations(uintptr_t handle, const Operation* operations, size_t count);

// Testing helper - adds a null row to DataFrame
FfiResult dispatch_add_null_row(uintptr_t handle, uintptr_t args);
int release_dataframe(uintptr_t handle);
void free_string(char* error_message);

// DataFrame introspection
char* dataframe_to_csv(uintptr_t handle);
char* dataframe_to_string(uintptr_t handle);

// Benchmark helper
int noop();

#endif // TURBO_POLARS_H
