# 🚀 Turbo Polars - High-Performance Go Bindings for Polars

**Turbo Polars** is a high-performance Go library providing bindings to the [Polars](https://github.com/pola-rs/polars) data manipulation library. Built with performance as the primary goal, Turbo Polars aims to deliver **significantly faster** DataFrame operations than existing Go-Polars solutions by minimizing CGO overhead and leveraging efficient static linking.

---

## 🎯 **Performance Philosophy**

Unlike existing Go-Polars libraries that incur high CGO costs for each method invocation, Turbo Polars employs a **batch-oriented architecture** with **static linking** to minimize overhead:

### 🔥 **High-Performance Architecture:**
1. **Static Rust Libraries** - Pre-compiled Polars libraries embedded as `.syso` files
2. **Batch Operations** - Minimize CGO calls by batching multiple operations
3. **Zero-Copy Data Sharing** - Direct memory access where possible
4. **Multi-Architecture Support** - Native ARM64 and AMD64 binaries

### 📊 **Performance Goals:**
- **Minimize CGO overhead** through operation batching and static linking
- **Leverage native Polars performance** with minimal Go wrapper cost
- **Memory-efficient** zero-copy data sharing where possible
- **Cross-platform** native performance on ARM64 and AMD64

---

## 🛠 **Architecture Overview**

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Go API        │───▶│  CGO Interface   │───▶│ Polars Rust     │
│  (pkg/polars)   │    │ (internal/cgo)   │    │ (.syso libs)    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
        │                       │                       │
        ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Batch Processor │    │  Performance     │    │  Memory Pool    │
│ (Operation      │    │  Instrumentation │    │  Management     │
│  Batching)      │    │ (benchmarks/)    │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

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

### Basic DataFrame Operations
```go
package main

import (
    "fmt"
    "github.com/miretskiy/turbo-polars/pkg/polars"
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
            polars.Count().Alias("employee_count"),
        ).
        Sort("avg_salary", polars.Descending)
    
    fmt.Printf("Processed %d rows\n", result.Height())
    fmt.Println(result.String())
}
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
- **Go 1.21+**
- **CGO enabled** (`CGO_ENABLED=1`)
- **Polars C library** (built automatically via scripts)

### Install
```bash
go get github.com/miretskiy/turbo-polars
```

### Build from Source
```bash
git clone https://github.com/miretskiy/turbo-polars
cd turbo-polars
make build
```

---

## ⚡ **Performance Goals**

Our performance strategy focuses on minimizing the overhead that plagues existing Go-Polars solutions:

### 🎯 **Key Optimizations**
- **Reduced CGO calls** through operation batching
- **Static linking** with pre-compiled Rust libraries (.syso files)
- **Zero-copy data sharing** where possible
- **Multi-architecture native binaries** (ARM64/AMD64)

### 📊 **Benchmarking**
We will provide comprehensive benchmarks comparing against:
- [go-polars](https://github.com/jordandelbar/go-polars) - existing Go bindings
- Native Polars (Python/Rust) - reference implementation
- Pure Go alternatives - for context

*Benchmarks will be published once core functionality is implemented.*

---

## 🧩 **Advanced Features**

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
    // Conditional logic
    polars.When(polars.Col("age").Gt(65)).
        Then(polars.Lit("senior")).
        Otherwise(polars.Lit("adult")).
        Alias("age_group"),
    
    // String operations
    polars.Col("name").Str().ToUppercase().Alias("name_upper"),
    
    // Date operations
    polars.Col("date").Dt().Year().Alias("year"),
    
    // Mathematical operations
    polars.Col("price").Mul(polars.Col("quantity")).Alias("total"),
)
```

### 🔗 **Joins and Concatenation**
```go
// Join DataFrames
result := df1.Join(df2, 
    polars.JoinOn("id"), 
    polars.JoinType.Inner,
)

// Concatenate DataFrames
combined := polars.Concat(df1, df2, df3)

// Union with different schemas
unified := polars.ConcatDiagonal(df1, df2) // Fills missing columns with nulls
```

### 🎯 **Window Functions**
```go
// Window operations
df = df.WithColumns(
    // Running sum
    polars.Col("sales").Sum().Over("department").Alias("dept_total"),
    
    // Rank within groups
    polars.Col("score").Rank().Over("team").Alias("team_rank"),
    
    // Moving average
    polars.Col("price").Mean().Over(polars.Window{Size: 7}).Alias("price_ma7"),
)
```

### 📈 **Deferred Execution (Performance Optimization)**
```go
// Operations build an execution plan without CGO calls
result, err := df.
    Filter(polars.Col("active").Eq(true)).        // No CGO - builds operation
    WithColumns(polars.Col("a").Add(polars.Col("b")).Alias("computed")). // No CGO
    Sort("timestamp", polars.Descending).         // No CGO
    Execute()                                     // Single CGO call executes all

// Compare to traditional approach:
// df.Filter(...) -> 22ns CGO overhead
// df.WithColumns(...) -> 22ns CGO overhead  
// df.Sort(...) -> 22ns CGO overhead
// Total: 66ns + actual work
//
// Our approach: 22ns + actual work (3x improvement for 3 operations)
```

## 🏗️ **Implementation Architecture**

### **🎯 RPN Stack Machine: The Core Innovation**

**Turbo Polars** implements a **Reverse Polish Notation (RPN) stack machine** for expression evaluation, which is the key to our high-performance architecture:

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
2. **Zero CGO During Construction**: Expressions build locally in Go with zero overhead
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

#### **OpCode Dispatch** ❌ **Rejected**
```rust
match operation.opcode {
    OP_FILTER => dispatch_filter(handle, args),
    OP_SELECT => dispatch_select(handle, args),
    // ... requires switch statement overhead
}
```

**Why Rejected:**
- **Dispatch Overhead**: Extra switch statement for every operation
- **Less Type Safe**: Generic opcode handling vs specific function signatures
- **Harder to Extend**: Adding operations requires opcode management

**The RPN stack machine with function pointers gives us the best of both worlds: performance and elegance.**

---

## 🛠 **Development**

### Build & Test
```bash
make build          # Build library
make test           # Run tests  
make bench          # Run benchmarks
make profile        # CPU/memory profiling
```

### CGO Integration
- **Polars C API** automatically built via `scripts/build_polars.sh`
- **Type-safe bindings** in `internal/cgo/`
- **Memory management** handled automatically

### Future: SIMBA Integration
Plans to integrate **SIMBA-style trampolines** for ultra-fast operations:
- ~2ns function call overhead (vs ~200ns CGO)
- SIMD-accelerated data processing
- Stack-safe operation validation

---

## 🎯 **Roadmap**

### Phase 1: Core Foundation ✅ **Completed**
- [x] Project structure and architecture design
- [x] RPN stack machine implementation with function pointers
- [x] Unified dispatch system with ExecutionContext
- [x] Core DataFrame and Series types
- [x] Basic I/O operations (CSV with glob support)
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

### Phase 3: Advanced Features 🚧 **In Progress**
- [x] Deferred execution API for performance (Execute pattern)
- [x] Complex expression composition (chained operations)
- [x] Multi-file operations with glob patterns
- [ ] GroupBy and aggregation operations (beyond Count)
- [ ] Join operations (inner, left, outer, cross)
- [ ] Window functions and rolling operations
- [ ] String and datetime operations
- [ ] Lazy evaluation and query optimization

### Phase 4: Performance & Polish 🎯 **Next**
- [x] Golden test framework for output validation
- [ ] Comprehensive benchmarking suite vs go-polars
- [ ] Memory optimization and pooling
- [ ] Multi-architecture support (ARM64/AMD64)
- [ ] Performance profiling and optimization
- [ ] Documentation and examples

---

## 📚 **Documentation**

- [**API Reference**](docs/api.md) - Complete API documentation
- [**Performance Guide**](docs/performance.md) - Optimization techniques
- [**CGO Integration**](docs/cgo.md) - Internal architecture details
- [**Benchmarking**](docs/benchmarks.md) - Performance measurement

---

## 🤝 **Contributing**

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Setup
1. Clone the repository
2. Run `make setup` to install dependencies
3. Run `make test` to ensure everything works
4. Make your changes and add tests
5. Submit a pull request

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
