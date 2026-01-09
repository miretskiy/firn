# ❄️ Firn - Go Bindings for Polars

**Firn — Go bindings for Polars with optimized FFI performance.**

Firn is a Go library providing bindings to the [Polars](https://github.com/pola-rs/polars) data manipulation library. Named after the granular snow that forms the transitional layer between fresh snow and dense glacial ice, Firn provides an efficient interface between Go applications and Polars operations.

Firn focuses on minimizing CGO overhead through operation batching and a stack-machine architecture for expression evaluation.

---

## 🎯 **Performance Philosophy**

Unlike existing Go-Polars libraries that incur high CGO costs for each method invocation, Firn employs a **batch-oriented architecture** to minimize overhead:

### 🔥 **High-Performance Architecture:**
1. **RPN Stack Machine** - Batch multiple operations into single FFI calls
2. **Deferred Execution** - Build operation graphs without CGO overhead
3. **Direct Rust Integration** - Minimal wrapper around native Polars
4. **Multi-Architecture Support** - Native ARM64 and AMD64 binaries

### 📊 **Performance Goals:**
- **Minimize CGO overhead** through operation batching and static linking
- **Leverage native Polars performance** with minimal Go wrapper cost
- **Memory-efficient** zero-copy data sharing where possible
- **Cross-platform** native performance on ARM64 and AMD64

---

## 🛠 **Architecture Overview**

```
┌─────────────────────────────────────────────────────────────────┐
│                        Go Layer (polars/)                       │
├─────────────────┬─────────────────┬─────────────────────────────┤
│   DataFrame     │   ExprNode      │      Operation Queue        │
│   Operations    │   (Lazy Iter)   │   []Operation{opcode,args}  │
│ .Filter().Sort()│ Col("x").Gt(5)  │     (Zero CGO until         │
│                 │                 │      .Collect())            │
└─────────────────┴─────────────────┴─────────────────────────────┘
                           │
                           ▼ Single CGO Call (.Collect())
┌─────────────────────────────────────────────────────────────────┐
│                     Rust Layer (rust/src/)                      │
├─────────────────┬─────────────────┬─────────────────────────────┤
│ ExecutionContext│  Expression     │    DataFrame Dispatch       │
│ {expr_stack,    │  Stack Machine  │   match opcode {            │
│  operation_args}│  Vec<Expr>      │     OpFilter => filter(),   │
│                 │                 │     OpSort => sort(), ...   │
└─────────────────┴─────────────────┴─────────────────────────────┘
                           │
                           ▼ Direct Polars API calls
┌─────────────────────────────────────────────────────────────────┐
│                    Native Polars Library                        │
│  LazyFrame::filter(expr).sort().collect() -> DataFrame          │
└─────────────────────────────────────────────────────────────────┘
```

**Key Architecture Points:**
1. **Go Layer**: Builds operation queues with zero CGO overhead
2. **Single FFI Call**: All operations batched into one `execute_operations()` call
3. **Rust Execution Engine**: Processes operation queue with expression stack machine
4. **Context Tracking**: Maintains DataFrame/LazyFrame/LazyGroupBy state across operations
5. **Native Polars**: Direct integration with Polars LazyFrame for optimal performance

### **Polars Immutability Design**

**Critical Architecture Note**: Polars follows an **immutable DataFrame design** where each operation returns a **new DataFrame instance** rather than modifying the original. This is fundamental to Polars' thread-safety and performance model.

**Evidence from Polars source code** (`rust/src/lib.rs:540`):
```rust
// Each operation creates a NEW DataFrame
match df.clone().lazy().filter(filter_expr).collect() {
    Ok(new_df) => Result::success(new_df),  // Returns NEW handle
    Err(e) => Result::error(ERROR_POLARS_OPERATION, &e.to_string()),
}
```

**Memory Management**: Our Go bindings automatically handle the lifecycle of intermediate DataFrames by releasing old handles when `Execute()` creates new ones, preventing memory leaks while maintaining Polars' immutable semantics.

**Why Not SIMBA Trampolines?**
While [SIMBA](https://github.com/miretskiy/simba) provides ultra-fast FFI for simple SIMD operations, Polars operations are complex library functions involving file I/O, parsing, and deep call stacks that exceed Go's NOSPLIT stack constraints (~2KB). Therefore, we use optimized CGO with static linking instead.

---

## 🚀 **Quick Start**

### Configure cgo linker search path
```shell
export CGO_LDFLAGS="-L/path/to/lib -Wl,-rpath,/path/to/lib"
```

### Basic DataFrame Operations
```go
package main

import (
    "fmt"
    "github.com/isesword/firn/polars"
)

func main() {
    // Create DataFrame from CSV
    df, err := polars.ReadCSV("employees.csv")
    if err != nil {
        panic(err)
    }
    
    // Chain operations efficiently
    result := df.
        Filter(polars.Col("age").Gt(25)).
        WithColumns(
            polars.Col("salary").Mul(1.1).Alias("salary_with_bonus"),
        ).
        GroupBy("department").
        Agg(
            polars.Col("salary_with_bonus").Mean().Alias("avg_salary"),
            polars.Col("age").Max().Alias("max_age"),
            polars.Col("*").Count().Alias("employee_count"),
        ).
        SortBy([]polars.SortField{polars.Desc("avg_salary")})
    
    fmt.Printf("Processed %d rows\n", result.Height())
    fmt.Println(result.String())
}
```

### Reading Data from Files
```go
// Read CSV files (with glob pattern support)
df := polars.ReadCSV("data.csv")
df := polars.ReadCSV("data_*.csv")  // Multiple files

// Read Parquet files with full optimization support
df := polars.ReadParquet("data.parquet")

// Advanced Parquet reading with options
df := polars.ReadParquetWithOptions("large_dataset.parquet", polars.ParquetOptions{
    Columns:  []string{"id", "name", "value"},  // Column pruning
    NRows:    1000,                             // Row limiting
    Parallel: true,                             // Parallel reading
    WithGlob: true,                             // Glob pattern support
})

// Parquet with glob patterns for partitioned datasets
df := polars.ReadParquet("year=2024/month=*/data_*.parquet")
```

### Creating DataFrames from Go Data
```go
// Create DataFrame from Go slices
df := polars.NewDataFrame(
    polars.NewSeries("name", []string{"Alice", "Bob", "Charlie"}),
    polars.NewSeries("age", []int{25, 30, 35}),
    polars.NewSeries("salary", []float64{50000, 60000, 70000}),
)

// Or from a map
data := map[string]interface{}{
    "product": []string{"A", "B", "C", "A", "B"},
    "price":   []float64{10.0, 15.0, 20.0, 12.0, 18.0},
    "qty":     []int{100, 200, 150, 80, 120},
}
df := polars.FromMap(data)
```

---

## 📦 **Installation**

### Prerequisites
- **Go 1.23+** (requires iterators support)
- **CGO enabled** (`CGO_ENABLED=1`)
- **Rust toolchain** (for building the Polars integration)

### Install from Source
Since this project requires compiling Rust libraries, you cannot use `go get` directly. You must build from source:

```bash
git clone https://github.com/isesword/firn
cd firn

# Build Rust library and Go bindings
bazel build //rust:all
bazel build //polars:all

# Run tests to verify installation
bazel test //polars:all
# Or run Go tests directly with linker warning suppression
CGO_LDFLAGS="-w" go test -v ./polars
```

### Using in Your Project
After building, you can import and use Firn in your Go projects:

```go
import "github.com/isesword/firn/polars"
```

### Suppressing Linker Warnings ⚠️ **macOS Users**

If you see macOS version compatibility warnings during compilation like:
```
ld: warning: object file (...) was built for newer 'macOS' version (15.5) than being linked (15.0)
```

You can suppress them using any of these methods:

#### Method 1: Environment Variable (Recommended)
```bash
# Set once in your shell profile (.zshrc, .bashrc, etc.)
export CGO_LDFLAGS="-w"

# Then run tests/builds normally
go test -v ./polars
go build
```

#### Method 2: Per-Command Basis
```bash
# For testing
CGO_LDFLAGS="-w" go test -v ./polars

# For building
CGO_LDFLAGS="-w" go build

# For specific test runs
CGO_LDFLAGS="-w" go test -v -run TestBasicOperations ./polars
```

#### Method 3: Project Integration
Add to your project's build scripts or CI configuration:
```bash
# In your build script
export CGO_LDFLAGS="-w"

# Then run your normal commands
go test -v ./polars
go build
```

**Note**: These warnings are harmless - they occur because the Rust library was compiled with a newer macOS SDK than Go's default target. The `-w` flag suppresses all linker warnings.

### Build with Bazel (Alternative)
You can also build using Bazel:

```bash
bazel build //polars:all
bazel test //polars:all
```

### Test Data Requirements

Some performance tests require large test data files that are not included in the repository due to GitHub's file size limits. These tests will be automatically skipped if the required files are not present.

**Performance Tests Requiring Large Data:**
- `TestPerformanceBenchmarks` - Tests with 10M+ row datasets
- Large weather data files (`weather_data_part_*.csv`) - ~340MB each
- 100M+ row aggregation tests

**To generate test data locally:**
```bash
# Generate large CSV test files (optional - for performance testing)
python3 scripts/generate_large_csv.py

# This creates weather_data_part_*.csv files in testdata/ and scripts/testdata/
# These files are automatically ignored by git (.gitignore)
```

**What gets skipped without large data:**
- Performance benchmarks on 10M+ row datasets
- Complex filtering tests on large datasets  
- 100M row aggregation performance tests

**All other tests work without large data:**
- Core DataFrame operations (uses small `sample.csv`)
- Expression system tests
- Join operations
- SQL query tests
- Parquet integration tests (uses `fortune1000_2024.parquet`)
- Window functions
- Error handling tests

The repository includes smaller test files that cover all functionality:
- `testdata/sample.csv` - 7 rows for basic operations
- `testdata/fortune1000_2024.parquet` - Fortune 1000 companies data
- Various small CSV files for specific test scenarios

---

## ⚡ **Performance Goals**

Our performance strategy focuses on minimizing the overhead that plagues existing Go-Polars solutions:

### 🎯 **Key Optimizations**
- **Reduced CGO calls** through operation batching
- **Static linking** with pre-compiled Rust libraries (.syso files)
- **Zero-copy data sharing** where possible
- **Multi-architecture native binaries** (ARM64/AMD64)

### 📊 **Benchmarking**
Firn includes comprehensive performance tests that demonstrate real-world DataFrame operations:

- **10M row operations**: 76-88 million rows/second
- **100M row operations**: 59-67 million rows/second  
- **Complex filtering and aggregations**: Maintains high throughput on large datasets
- **Memory efficiency**: Automatic handle cleanup prevents memory leaks

Run benchmarks with:
```bash
# Run all tests including performance benchmarks
bazel test //polars:all
# Or with Go directly
CGO_LDFLAGS="-w" go test -v ./polars

# Detailed benchmarking
cd benchmarks && CGO_LDFLAGS="-w" go test -bench=. -benchmem
```

---

## 🧩 **Advanced Features**

### 📁 **High-Performance File I/O**

#### **Parquet Support** 🚀
Firn provides comprehensive Parquet support with advanced optimization features:

```go
// Basic Parquet reading
df := polars.ReadParquet("dataset.parquet")

// Advanced Parquet with column pruning and row limiting
df := polars.ReadParquetWithOptions("large_dataset.parquet", polars.ParquetOptions{
    Columns:  []string{"id", "timestamp", "value"},  // Only read needed columns
    NRows:    100000,                                // Limit rows for sampling
    Parallel: true,                                  // Enable parallel reading
    WithGlob: true,                                  // Support glob patterns
})

// Partitioned datasets with glob patterns
df := polars.ReadParquet("year=*/month=*/data_*.parquet")

// Combine with Firn operations for optimal performance
result := polars.ReadParquetWithOptions("fortune1000.parquet", polars.ParquetOptions{
    Columns: []string{"Rank", "Company", "Revenue", "Sector"},
    NRows:   100,  // Top 100 companies
}).
Filter(polars.Col("Revenue").Gt(polars.Lit(50000))).
GroupBy("Sector").
Agg(
    polars.Col("Revenue").Mean().Alias("avg_revenue"),
    polars.Col("Company").Count().Alias("company_count"),
).
SortBy([]polars.SortField{polars.Desc("avg_revenue")}).
Collect()
```

**Parquet Performance Benefits:**
- **Column Pruning**: Only read columns you need, dramatically reducing I/O
- **Row Limiting**: Sample large datasets efficiently with `NRows` parameter
- **Parallel Reading**: Leverage multiple cores for faster file processing
- **Native Integration**: Seamless integration with Firn's RPN stack machine
- **Memory Efficient**: Polars' zero-copy architecture minimizes memory usage

#### **CSV Support**
```go
// Basic CSV reading
df := polars.ReadCSV("data.csv")

// Multiple files with glob patterns
df := polars.ReadCSV("data_part_*.csv")

// Advanced CSV options
df := polars.ReadCSVWithOptions("data.csv", hasHeader, inferSchema)
```

### 🔄 **Lazy Evaluation**
```go
// Build computation graph without executing
lazy := polars.LazyFrame().
    ReadCSV("large_file.csv").
    Filter(polars.Col("status").Eq("active")).
    GroupBy("category").
    Agg(polars.Col("value").Sum()).
    Sort("value", polars.Descending)

// Execute when ready - optimized query plan
result := lazy.Collect()
```

### 📊 **Complex Expressions**
```go
// Advanced column operations
df = df.WithColumns(
    // Mathematical operations
    polars.Col("price").Mul(polars.Col("quantity")).Alias("total"),
    
    // String operations (basic operations available)
    polars.Col("name").StrLen().Alias("name_length"),
    polars.Col("name").StrToUppercase().Alias("name_upper"),
    
    // Arithmetic and comparison
    polars.Col("salary").Add(polars.Col("bonus")).Alias("total_comp"),
    polars.Col("age").Gt(polars.Lit(30)).Alias("is_senior"),
)
```

### 🔀 **Conditional Expressions (When/Then/Otherwise)**
Firn provides SQL CASE-like conditional expressions with a fluent API for building complex conditional logic:

```go
// Basic conditional expression - equivalent to SQL CASE WHEN
df = df.WithColumns(
    polars.When(polars.Col("age").Gt(polars.Lit(65))).
        Then(polars.Lit("senior")).
        Otherwise(polars.Lit("adult")).
        Alias("age_category"),
)

// Multiple conditions with chained When statements
df = df.WithColumns(
    polars.When(polars.Col("score").Gt(polars.Lit(90))).
        Then(polars.Lit("A")).
        When(polars.Col("score").Gt(polars.Lit(80))).
        Then(polars.Lit("B")).
        When(polars.Col("score").Gt(polars.Lit(70))).
        Then(polars.Lit("C")).
        Otherwise(polars.Lit("F")).
        Alias("grade"),
)

// Complex conditional expressions with computed values
df = df.WithColumns(
    polars.When(polars.Col("department").Eq(polars.Lit("Engineering"))).
        Then(polars.Col("salary").Mul(polars.Lit(1.15))).  // 15% bonus for engineers
        When(polars.Col("department").Eq(polars.Lit("Sales"))).
        Then(polars.Col("salary").Mul(polars.Lit(1.10))).  // 10% bonus for sales
        Otherwise(polars.Col("salary").Mul(polars.Lit(1.05))). // 5% for others
        Alias("salary_with_bonus"),
)

// Use in filtering operations
filtered := df.Filter(
    polars.When(polars.Col("status").Eq(polars.Lit("active"))).
        Then(polars.Col("last_login").Gt(polars.Lit("2024-01-01"))).
        Otherwise(polars.Lit(false)),
)

// Use in aggregations
summary := df.GroupBy("department").Agg(
    polars.When(polars.Col("performance").Gt(polars.Lit(8.0))).
        Then(polars.Lit(1)).
        Otherwise(polars.Lit(0)).
        Sum().Alias("high_performers"),
    polars.Col("salary").Mean().Alias("avg_salary"),
)
```

**Conditional Expression Features:**
- **SQL CASE Equivalent**: Familiar `WHEN condition THEN value` syntax
- **Chained Conditions**: Multiple `When().Then()` pairs for complex logic
- **Type Safety**: Full type checking at compile time through Go's type system
- **Performance Optimized**: Compiled to native Polars conditional expressions
- **Flexible Usage**: Works in columns, filters, aggregations, and any expression context
- **RPN Stack Integration**: Zero CGO overhead during expression building

### 🔍 **SQL Queries**
Firn provides flexible SQL support that can be mixed seamlessly with fluent-style expressions, giving you the best of both worlds:

```go
// Execute SQL queries directly on DataFrames
// The DataFrame is automatically registered as "df" table
result := df.Query(`
    SELECT name, salary * 1.1 as new_salary 
    FROM df 
    WHERE age > 25 AND department = 'Engineering'
`).Collect()

// Complex SQL with aggregations and grouping
summary := df.Query(`
    SELECT 
        department,
        AVG(salary) as avg_salary,
        COUNT(*) as employee_count,
        MAX(age) as max_age
    FROM df 
    GROUP BY department 
    HAVING COUNT(*) > 2
    ORDER BY avg_salary DESC
`).Collect()

// Mix SQL strings with fluent expressions for maximum flexibility
result := df.
    Query("SELECT * FROM df WHERE active = true").           // SQL for complex filtering
    WithColumns(polars.Col("bonus").Mul(polars.Lit(1.1))).   // Fluent for type-safe operations
    SortBy([]polars.SortField{polars.Desc("salary")}).       // Fluent for programmatic control
    Collect()

// Use SQL for what it's best at (complex queries, familiar syntax)
// Use fluent API for what it's best at (type safety, IDE support, composition)
complex := df.
    Query(`
        SELECT *, 
               CASE WHEN age > 50 THEN 'senior' ELSE 'junior' END as category
        FROM df 
        WHERE department IN ('Engineering', 'Data Science')
    `).
    WithColumns(
        polars.Col("salary").Quantile(0.95).Over("category").Alias("p95_salary"),
        polars.Col("performance_score").Rank().Over("department").Alias("dept_rank"),
    ).
    Filter(polars.Col("dept_rank").Lt(polars.Lit(10))).
    Collect()
```

### 🔗 **Joins and Concatenation**
```go
// Basic join operations
employees, _ := polars.ReadCSV("employees.csv").Collect()
departments, _ := polars.ReadCSV("departments.csv").Collect()

// Inner join (most common)
result, _ := employees.InnerJoin(departments, "dept_id").Collect()

// Left join with all employees, even those without departments
result, _ := employees.LeftJoin(departments, "dept_id").Collect()

// Advanced join with different column names
result, _ := employees.Join(departments, 
    polars.LeftOn("department_id").RightOn("id")).Collect()

// Join with custom suffix for duplicate columns
result, _ := employees.Join(departments, 
    polars.On("dept_id").WithType(polars.JoinTypeLeft).WithSuffix("_dept")).Collect()

// Cross join (Cartesian product)
result, _ := employees.CrossJoin(departments).Collect()

// Concatenate DataFrames vertically
combined, _ := polars.Concat(df1, df2, df3).Collect()
```

### 🎯 **Window Functions**
```go
// Window operations
df = df.WithColumns(
    // Running sum
    polars.Col("sales").Sum().Over("department").Alias("dept_total"),
    
    // Rank within groups
    polars.Col("score").Rank().Over("team").Alias("team_rank"),
    
    // Moving average (using Over with partition - window functions need partitioning)
    polars.Col("price").Mean().Over("date").Alias("price_ma7"),
)
```

### 📈 **Deferred Execution (Performance Optimization)**
```go
// Operations build an execution plan without CGO calls
result, err := df.
    Filter(polars.Col("active").Eq(true)).        // No CGO - builds operation
    WithColumns(polars.Col("a").Add(polars.Col("b")).Alias("computed")). // No CGO
    SortBy([]polars.SortField{polars.Desc("timestamp")}).  // No CGO
    Execute()                                     // Single CGO call executes all
```

**Performance comparison (M4 Mac measurements):**

**Traditional go-polars approach:**
- `df.Filter(...)` → ~22ns CGO overhead + C string alloc/dealloc costs
- `df.WithColumns(...)` → ~22ns CGO overhead + C string alloc/dealloc costs  
- `df.Sort(...)` → ~22ns CGO overhead + C string alloc/dealloc costs
- **Total:** ~66ns + 3x string allocation + free overhead + actual work

**Firn's batched approach:** ~22ns + actual work

**Key architectural advantage:** Firn's Operation args function captures arguments that remain alive for the duration of the CGO call, allowing raw string passing (char * + len) to Rust without CGO allocation/deallocation. Rust copies these buffers as needed, eliminating repeated boundary costs.

## 🏗️ **Implementation Architecture**

### **🎯 RPN Stack Machine Architecture**

Firn implements a **Reverse Polish Notation (RPN) stack machine** for expression evaluation to optimize FFI performance:

#### **How the Stack Machine Works**
```go
// Go side: Build expression as operation sequence
expr := Col("salary").Mul(Lit(2)).Add(Col("bonus"))

// Generates RPN sequence:
// [push_col("salary"), push_lit(2), mul, push_col("bonus"), add]

// Single FFI call executes entire expression tree
result, err := df.WithColumns(expr.Alias("total_comp")).Execute()
```

#### **Stack Machine Benefits** ✅
1. **Single FFI Call**: Entire expression trees execute in one CGO boundary crossing
2. **Zero CGO During Construction**: Expressions build locally in Go with zero CGO overhead
3. **Native Polars Integration**: Stack operations map directly to `polars::Expr` operations
4. **Memory Efficient**: Linear operation sequence vs heap-allocated expression trees
5. **Type Safe**: All operations validated at the Rust boundary with proper error reporting

#### **Expression Execution Flow**
```rust
// Rust side: Execute RPN sequence on expression stack
let mut expr_stack: Vec<Expr> = Vec::new();

for operation in operations {
    match operation.func_ptr {
        expr_column => expr_stack.push(col(&args.name)),
        expr_literal => expr_stack.push(lit(args.value)),
        expr_mul => {
            let right = expr_stack.pop().unwrap();
            let left = expr_stack.pop().unwrap();
            expr_stack.push(left * right);
        }
        // ... other operations
    }
}
```

#### **Performance Impact**
```go
// Traditional approach (multiple CGO calls):
df.Filter(col.Gt(5))     // ~22ns CGO overhead
  .WithColumns(expr)     // ~22ns CGO overhead  
  .Sort("name")          // ~22ns CGO overhead
// Total: 66ns + actual work

// Stack machine approach (single CGO call):
df.Filter(col.Gt(5)).WithColumns(expr).Sort("name").Execute()
// Total: 22ns + actual work (3x improvement!)
```

### **Architecture Comparison**

#### **Function Pointer + RPN Stack** ✅ **Selected**
```go
type Operation struct {
    funcPtr unsafe.Pointer  // Points to Rust dispatch function
    args    unsafe.Pointer  // Operation-specific arguments
}
```

**Advantages:**
- **Uniform Interface**: All operations use `(handle, context) -> Result` signature
- **RPN Evaluation**: Natural expression tree evaluation via stack machine
- **Type Safety**: Each operation validates its specific argument types
- **Performance**: Direct function calls, no opcode dispatch overhead

#### **OpCode Dispatch** ✅ **Selected**
```rust
match operation.opcode {
    OP_FILTER => dispatch_filter(handle, args),
    OP_SELECT => dispatch_select(handle, args),
    // ... opcode-based dispatch system
}
```

**Why Selected:**
- **Uniform Interface**: Consistent opcode-based dispatch system
- **Type Safety**: Each operation validates its specific argument types
- **Extensible**: Easy to add new operations by defining new opcodes
- **Performance**: Direct opcode matching with minimal overhead

**The opcode dispatch system provides a clean, extensible architecture for operation handling.**

---

## 🛠 **Development**

### Build & Test
```bash
# Build library
bazel build //rust:all //polars:all

# Run tests
bazel test //polars:all
# Or with Go directly
CGO_LDFLAGS="-w" go test -v ./polars

# Run benchmarks
cd benchmarks && CGO_LDFLAGS="-w" go test -bench=. -benchmem

# CPU/memory profiling
CGO_LDFLAGS="-w" go test -cpuprofile=cpu.prof -memprofile=mem.prof -bench=. ./polars
```

### CGO Integration
- **Rust library** automatically built via `scripts/build_rust.sh`
- **Type-safe bindings** in `internal/cgo/` and `internal/ffi/`
- **Memory management** handled automatically


---

## 🎯 **Roadmap**

### Phase 1: Core Foundation ✅ **Completed**
- [x] Project structure and architecture design
- [x] RPN stack machine implementation with function pointers
- [x] Unified dispatch system with ExecutionContext
- [x] Core DataFrame and Series types
- [x] Basic I/O operations (CSV with glob support)
- [x] Parquet I/O operations with column pruning and row limiting
- [x] Memory management and safety (automatic handle cleanup)
- [x] Expression system with move semantics

### Phase 2: DataFrame Operations ✅ **Completed**
- [x] Column operations and expressions (Col, Lit, arithmetic, boolean)
- [x] Filtering with complex expressions
- [x] Selection and projection operations
- [x] WithColumns for computed columns (single and multiple)
- [x] Comprehensive aggregation operations (Count, Sum, Mean, Min, Max, Median, First, Last, NUnique, Std, Var)
- [x] Null-aware operations (IsNull, IsNotNull, Count vs CountWithNulls)
- [x] Statistical functions with ddof parameter support
- [x] DataFrame concatenation
- [x] Expression aliases and column naming

### Phase 3: Advanced Features ✅ **Completed**
- [x] Deferred execution API for performance (Execute pattern)
- [x] Complex expression composition (chained operations)
- [x] Multi-file operations with glob patterns
- [x] GroupBy and aggregation operations (complete implementation)
- [x] Sort operations with multi-column and nulls ordering support
- [x] String operations (Tier 1: length, contains, starts/ends with, case conversion)
- [x] Context-aware lazy evaluation (DataFrame, LazyFrame, LazyGroupBy)
- [x] SQL query support with full Polars SQL syntax

### Phase 4: Advanced Operations ✅ **Completed**
- [x] Join operations (inner, left, right, outer, cross) with comprehensive API
- [x] Window functions and rolling operations
- [x] Conditional expressions (When/Then/Otherwise) - SQL CASE-like functionality
- [x] Cast operations with comprehensive data type support
- [x] Advanced string operations (Tier 2: slice, replace, split)
- [ ] Date/time operations

### Phase 5: Extended I/O and Extensibility 🎯 **Next**
- [x] Golden test framework for output validation
- [x] Multi-architecture support (ARM64/AMD64)
- [ ] Extended I/O support (JSON, Arrow, ORC, Avro)
- [ ] Go extension framework for custom data sources
- [ ] Plugin system for user-defined functions
- [ ] Streaming I/O for large datasets
- [ ] Advanced string operations (Tier 2: slice, replace, split)
- [ ] Date/time operations

---

## 📚 **Documentation**

- [**API Reference**](docs/api.md) - Complete API documentation
- [**Performance Guide**](docs/performance.md) - Optimization techniques
- [**CGO Integration**](docs/cgo.md) - Internal architecture details
- [**Benchmarking**](docs/benchmarks.md) - Performance measurement

---

## 🤝 **Contributing**

We welcome contributions! Here's how to get started:

### Development Setup
1. Clone the repository
2. Build the project with `bazel build //rust:all //polars:all`
3. Run tests with `bazel test //polars:all` or `CGO_LDFLAGS="-w" go test -v ./polars`
4. Make your changes and add tests
5. Ensure all tests pass before submitting
6. Submit a pull request

### Guidelines
- Follow existing code style and patterns
- Add tests for new functionality
- Update documentation as needed
- Ensure all tests pass before submitting
- **AI Tools Encouraged**: Use of AI tools like Cline is not only recommended but encouraged for development

---

## 📄 **License**

Licensed under the [Apache License, Version 2.0](LICENSE).

---

## 🙏 **Acknowledgments**

- **[Polars](https://github.com/pola-rs/polars)** - The amazing DataFrame library
- **[SIMBA](https://github.com/miretskiy/simba)** - High-performance FFI inspiration
- **[go-polars](https://github.com/jordandelbar/go-polars)** - Prior art and inspiration

---

**Built for speed. Designed for scale. Optimized for Go.**
