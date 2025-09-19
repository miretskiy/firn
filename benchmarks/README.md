# Turbo-Polars Benchmarks

This directory contains benchmarks to measure the performance of our function pointer dispatch system vs other Go Polars implementations.

## Running Benchmarks

### Basic Usage
```bash
# Run all benchmarks
go test -run ^$ -bench .

# Run specific benchmark patterns
go test -run ^$ -bench BenchmarkTurboPolars_ReadCSV
go test -run ^$ -bench BenchmarkTurboPolars_Complex

# Run with memory allocation stats
go test -run ^$ -bench . -benchmem

# Run multiple times for statistical significance
go test -run ^$ -bench . -count=5

# Run for longer duration for more stable results
go test -run ^$ -bench . -benchtime=10s
```

### Performance Profiling
```bash
# CPU profiling
go test -run ^$ -bench BenchmarkTurboPolars_ComplexChain -cpuprofile=cpu.prof

# Memory profiling  
go test -run ^$ -bench BenchmarkTurboPolars_ComplexChain -memprofile=mem.prof

# Analyze profiles
go tool pprof cpu.prof
go tool pprof mem.prof
```

## Benchmark Categories

### Basic Operations
- `BenchmarkTurboPolars_ReadCSV_Execute` - CSV reading + execution
- `BenchmarkTurboPolars_Filter_Execute` - Single filter operation
- `BenchmarkTurboPolars_Select_Execute` - Column selection

### Complex Operations
- `BenchmarkTurboPolars_ComplexChain_Execute` - Multiple chained operations
- `BenchmarkTurboPolars_MultipleFilters_Execute` - Multiple filter conditions

### Expression Construction
- `BenchmarkTurboPolars_ExpressionConstruction` - Pure expression building overhead

### Memory Analysis
- `BenchmarkTurboPolars_*_WithAllocs` - Same operations with allocation tracking

### Large Dataset Tests
- `BenchmarkTurboPolars_LargeDataset_*` - Tests with 1000+ rows

## What We're Measuring

Each benchmark measures complete "ops/sec" including:
1. **DataFrame creation** - ReadCSV() call
2. **Query building** - Filter(), Select() with expressions  
3. **Execution** - Execute() call
4. **Cleanup** - Release() calls

This tests the **CGO overhead** and our **function pointer dispatch system**, not the underlying Polars performance.

## Comparison with go-polars

To compare with go-polars:
1. Install: `go get github.com/pola-rs/polars/go`
2. Create equivalent benchmarks using go-polars API
3. Run both benchmark suites
4. Compare ops/sec and memory allocations

## Expected Results

Our goals:
- **10x faster** than go-polars for large datasets
- **50% lower memory** usage through zero-copy operations  
- **Sub-millisecond** expression evaluation overhead
