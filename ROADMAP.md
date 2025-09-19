# Turbo-Polars: High-Performance Go Bindings for Polars

## Project Vision

Build high-performance, zero-copy Go bindings for Polars that leverage:
- **Function pointer dispatch** instead of opcode-based dispatch
- **Expression stack machine** for minimal CGO overhead
- **Parallel execution** of operations where possible
- **Zero-copy memory management** between Go and Rust

## Current Status ✅

### Core Infrastructure
- [x] Function pointer dispatch system
- [x] Expression stack machine (`ExprOp` with `expr_column`, `expr_literal`, `expr_gt`, `expr_lt`, `expr_eq`)
- [x] Zero-copy string handling with `RawStr`
- [x] Memory management with `gcRefs`
- [x] Basic DataFrame operations (ReadCSV, Select, Filter, Count)
- [x] DataFrame introspection (`ToCsv()`)

### Expression System
- [x] `Col("name")` - column references
- [x] `Lit(value)` - literal values (int64, float64, string, bool)
- [x] `Gt()`, `Lt()`, `Eq()` - basic comparisons
- [x] Expression chaining: `Col("age").Gt(Lit(25))`
- [x] `Filter(expr)` - expression-based filtering

## Roadmap 🚀

### Phase 1: Core Expression Operations (Priority: HIGH)
Based on [Polars expressions documentation](https://docs.pola.rs/user-guide/expressions/):

#### Arithmetic Operations
- [ ] `Add()`, `Sub()`, `Mul()`, `Div()` - basic arithmetic
- [ ] `Pow()`, `Mod()` - advanced arithmetic
- [ ] `Abs()`, `Round()`, `Floor()`, `Ceil()` - math functions

#### Logical Operations  
- [ ] `And()`, `Or()`, `Not()` - boolean logic
- [ ] `IsNull()`, `IsNotNull()` - null checking
- [ ] `IsIn(values)` - membership testing

#### String Operations
- [ ] `StrContains()`, `StrStartsWith()`, `StrEndsWith()`
- [ ] `StrLen()`, `StrToUppercase()`, `StrToLowercase()`
- [ ] `StrSlice()`, `StrReplace()`

#### Aggregation Operations
- [ ] `Sum()`, `Mean()`, `Min()`, `Max()`, `Count()`
- [ ] `First()`, `Last()`, `Median()`, `Std()`, `Var()`

### Phase 2: Parallel Column Operations (Priority: HIGH)
Based on [pandas migration examples](https://docs.pola.rs/user-guide/migration/pandas/):

#### with_columns Implementation
```go
// Target API:
df.WithColumns(
    Col("value").Mul(Lit(10)).Alias("tenXValue"),
    Col("value").Mul(Lit(100)).Alias("hundredXValue"),
)
```

- [ ] `WithColumns()` method for parallel column assignment
- [ ] `Alias()` method for column renaming
- [ ] `When().Then().Otherwise()` conditional expressions
- [ ] Expression parallelization in Rust

### Phase 3: Window Functions (Priority: MEDIUM)
Based on pandas `transform` examples:

#### Window Operations
```go
// Target API:
df.WithColumns(
    Col("type").Count().Over("c").Alias("size"),
    Col("c").Sum().Over("type").Alias("sum"),
)
```

- [ ] `Over(partition_by)` - window function partitioning
- [ ] Window aggregations (count, sum, mean, etc.)
- [ ] `Rank()`, `RowNumber()` - ranking functions

### Phase 4: I/O Operations (Priority: MEDIUM)
Based on [Polars I/O documentation](https://docs.pola.rs/user-guide/io/):

#### File Format Support
- [ ] **Parquet** - `ReadParquet()`, `WriteParquet()` (highest priority)
- [ ] **JSON** - `ReadJSON()`, `WriteJSON()`
- [ ] **Excel** - `ReadExcel()`, `WriteExcel()`
- [ ] **Multiple files** - glob pattern support

#### Advanced I/O
- [ ] **Lazy scanning** - `ScanParquet()`, `ScanCSV()` with optimization
- [ ] **Streaming** - large file processing
- [ ] **Cloud storage** - S3, GCS integration

### Phase 5: Transformations (Priority: MEDIUM)
Based on [transformations documentation](https://docs.pola.rs/user-guide/transformations/):

#### Data Reshaping
- [ ] `Join()` - inner, left, right, outer joins
- [ ] `Concat()` - vertical and horizontal concatenation
- [ ] `Pivot()`, `Unpivot()` - data reshaping
- [ ] `GroupBy()` with aggregations

#### Time Series
- [ ] Date/time parsing and manipulation
- [ ] Time-based filtering and grouping
- [ ] Resampling operations

### Phase 6: Advanced Features (Priority: LOW)
#### Folds and Complex Operations
- [ ] `Fold()` operations for complex aggregations
- [ ] `Map()` operations for custom transformations
- [ ] `Apply()` for row-wise operations

#### GPU Acceleration Investigation
Based on [GPU support documentation](https://docs.pola.rs/user-guide/gpu-support/):
- [ ] Research GPU acceleration on M4 Mac
- [ ] Identify operations that benefit from GPU
- [ ] Benchmark GPU vs CPU performance
- [ ] Implement GPU-accelerated operations if beneficial

### Phase 7: Zero-Copy Data Access (Priority: HIGH for benchmarking)
#### Arrow Integration
- [ ] Extract underlying Arrow data structures
- [ ] Provide zero-copy access to numpy-compatible arrays
- [ ] Implement C Data Interface for interoperability
- [ ] Memory-mapped file support

## Immediate Next Steps 🎯

### 1. DataFrame Display (Week 1)
- [ ] Implement `String()` method for tabular output
- [ ] Support pretty-printing like Polars native output
- [ ] Add configurable display options (max rows, columns)

### 2. Benchmark Suite (Week 1-2)
- [ ] Create comprehensive benchmark comparing with go-polars
- [ ] Test scenarios: CSV reading, filtering, aggregations, joins
- [ ] Performance metrics: throughput, memory usage, latency
- [ ] Automated benchmark runner

### 3. Core Expression Expansion (Week 2-3)
- [ ] Add arithmetic operations (`Add`, `Sub`, `Mul`, `Div`)
- [ ] Add logical operations (`And`, `Or`, `Not`)
- [ ] Add basic aggregations (`Sum`, `Mean`, `Min`, `Max`)

### 4. with_columns Implementation (Week 3-4)
- [ ] Parallel column assignment
- [ ] Conditional expressions (`When().Then().Otherwise()`)
- [ ] Column aliasing

## Success Metrics 📊

### Performance Targets
- **10x faster** than go-polars for large datasets (>1M rows)
- **50% lower memory usage** through zero-copy operations
- **Sub-millisecond** expression evaluation overhead

### API Completeness
- **80% coverage** of common Polars operations
- **100% compatibility** with Polars expression semantics
- **Zero breaking changes** in core API

### Developer Experience
- **IntelliSense support** for all operations
- **Comprehensive documentation** with examples
- **Easy migration path** from pandas/go-polars

## Architecture Decisions 🏗️

### Why Function Pointers?
- **Eliminates opcode dispatch** overhead in Rust
- **Type-safe** operation-specific argument structs
- **Extensible** - easy to add new operations

### Why Expression Stack Machine?
- **Minimal CGO calls** - build expressions in pure Go
- **Composable** - expressions can be chained and reused
- **Optimizable** - Rust can optimize expression trees

### Why Zero-Copy?
- **Performance** - avoid memory allocations and copies
- **Scalability** - handle large datasets efficiently
- **Memory efficiency** - share data between Go and Rust

## Testing Strategy 🧪

### Unit Tests
- Expression building and evaluation
- Memory management and GC safety
- Error handling and edge cases

### Integration Tests
- End-to-end workflows with real data
- Interoperability with Arrow ecosystem
- Performance regression testing

### Benchmark Tests
- Comparison with go-polars, pandas, native Polars
- Memory usage profiling
- Throughput and latency measurements

---

## Contributing 🤝

This project aims to provide the fastest, most memory-efficient Polars bindings for Go. We prioritize:

1. **Performance** over convenience
2. **Zero-copy** operations over simplicity
3. **Type safety** over flexibility
4. **Polars compatibility** over custom features

Let's build something amazing! 🚀
