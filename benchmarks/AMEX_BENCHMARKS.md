# AMEX Dataset Benchmarks for Firn

This directory contains comprehensive benchmarks for testing Firn's performance with the AMEX credit default prediction dataset - a large-scale real-world dataset with **16.9 million rows** and **193 columns**.

## Dataset Overview

- **Source**: AMEX "All Data" dataset from Kaggle
- **Size**: ~11GB uncompressed Parquet file
- **Rows**: 16,895,213 records
- **Columns**: 193 features
- **Domain**: Credit default prediction
- **Key Columns**:
  - `customer_ID`: Unique customer identifier
  - `date`: Transaction/record date (datetime)
  - `target`: Binary target (0=no default, 1=default)
  - `test`: Test set indicator
  - `P_2`, `D_144`, `D_145`, etc.: Feature columns (float32)

## Benchmark Categories

### 1. Basic Read Performance (`BasicReadPerformance`)
Tests Parquet reading performance with different row limits:
- **Read1K**: 1,000 rows
- **Read10K**: 10,000 rows  
- **Read100K**: 100,000 rows
- **Read1M**: 1,000,000 rows

**Purpose**: Measure raw I/O performance and scaling characteristics.

### 2. Column Selection Performance (`ColumnSelectionPerformance`)
Tests performance impact of column projection:
- **KeyColumns**: Essential columns only (customer_ID, date, target, test)
- **FeatureSubset**: Key columns + sample features (6 columns total)
- **AllColumns**: All 193 columns

**Purpose**: Evaluate Parquet column pruning efficiency.

### 3. Filtering Benchmarks (`FilteringBenchmarks`)
Tests various filtering strategies:
- **TargetFilter**: `target = 1` (credit defaults only)
- **DateRangeFilter**: `date >= '2018-03-01' AND date <= '2018-03-31'`
- **FeatureThreshold**: `P_2 > 0.5` (feature-based filtering)
- **ComplexFilter**: Multi-condition filter combining target, feature, and date

**Purpose**: Measure predicate pushdown and filtering performance.

### 4. Aggregation Benchmarks (`AggregationBenchmarks`)
Tests aggregation operations:
- **BasicStats**: Statistical aggregations (mean, min, max, sum)
- **GroupByTarget**: Group by target with customer counts and averages
- **GroupByMonth**: Time-based grouping with default rate calculation

**Purpose**: Evaluate GroupBy and aggregation performance.

### 5. Complex Analytics Benchmarks (`ComplexAnalyticsBenchmarks`)
Tests advanced analytical queries:
- **CustomerRiskProfile**: Customer-level risk analysis with multiple aggregations
- **TimeSeriesAnalysis**: Monthly trend analysis with multiple dimensions
- **FeatureCorrelationSample**: Feature correlation preparation (null filtering)

**Purpose**: Test complex multi-operation query performance.

### 6. Full Dataset Stress Test (`FullDatasetStressTest`)
Tests performance on the complete 16.9M row dataset:
- **Skipped by default** - uncomment to run
- Filters all 16.9M rows for credit defaults
- Ultimate stress test for memory and performance

**Purpose**: Validate scalability to full dataset size.

## Data Quality Tests (`TestAMEXDataQuality`)

### Schema Validation
- Verifies presence of key columns
- Validates data types and structure

### Data Range Validation  
- Checks target variable distribution (~26.45% default rate)
- Validates test set indicators
- Ensures data integrity

## Running the Benchmarks

### Quick Validation
```bash
# Test data quality and schema
cd benchmarks
CGO_LDFLAGS="-w" go test -v -run TestAMEXDataQuality

# Test basic read performance
CGO_LDFLAGS="-w" go test -v -run TestAMEXBenchmarks/BasicReadPerformance
```

### Comprehensive Benchmarking
```bash
# Run all benchmarks (excluding full dataset stress test)
CGO_LDFLAGS="-w" go test -v -run TestAMEXBenchmarks

# Run specific benchmark categories
CGO_LDFLAGS="-w" go test -v -run TestAMEXBenchmarks/FilteringBenchmarks
CGO_LDFLAGS="-w" go test -v -run TestAMEXBenchmarks/AggregationBenchmarks
```

### Full Dataset Stress Test (Use with Caution)
```bash
# Uncomment the t.Skip() line in FullDatasetStressTest first
CGO_LDFLAGS="-w" go test -v -run TestAMEXBenchmarks/FullDatasetStressTest
```

## Performance Expectations

Based on initial testing, Firn demonstrates:

- **I/O Performance**: Efficient Parquet reading with parallel processing
- **Column Pruning**: Significant performance gains with column selection
- **Predicate Pushdown**: Fast filtering operations
- **Aggregation Speed**: Competitive GroupBy and statistical operations
- **Memory Efficiency**: Handles large datasets without excessive memory usage

## Benchmark Dimensions Tested

### 🔍 **I/O Operations**
- Parquet file reading
- Column selection (projection)
- Row limiting (top-N)
- Parallel processing

### 🎯 **Query Operations**  
- Filtering (WHERE clauses)
- Aggregations (SUM, AVG, COUNT, etc.)
- GroupBy operations
- Sorting and limiting

### 📊 **Data Types**
- String columns (customer_ID)
- Datetime columns (date)
- Float32 features (P_2, D_144, etc.)
- Integer targets (target, test)
- Binary data (line_ID)

### 🏗️ **Query Complexity**
- Simple single-table operations
- Multi-condition filters
- Complex analytical queries
- Time-series analysis
- Customer profiling

## Integration with Firn Features

These benchmarks exercise key Firn capabilities:

- **RPN Stack Machine**: Complex expression evaluation
- **Lazy Evaluation**: Deferred execution optimization
- **FFI Efficiency**: Minimal CGO overhead
- **Memory Management**: Automatic handle cleanup
- **SQL Integration**: Mixed SQL and fluent API usage
- **Parquet Optimization**: ScanArgsParquet usage

## Usage as Performance Regression Tests

This benchmark suite serves as:

1. **Performance Baseline**: Establish current performance characteristics
2. **Regression Detection**: Identify performance degradations
3. **Optimization Validation**: Measure improvement impact
4. **Scalability Testing**: Validate large dataset handling
5. **Real-world Validation**: Test with actual production-scale data

## Dataset Setup

The benchmarks expect the AMEX dataset at `../testdata/data.parquet`. To set up:

1. Download the AMEX dataset (data.parquet.zip)
2. Extract to `testdata/data.parquet`
3. Run the schema inspector: `cd cmd/schema_inspector && go run main.go ../../testdata/data.parquet`
4. Execute benchmarks as shown above

## Contributing

When adding new benchmarks:

1. Follow the existing test structure and naming conventions
2. Include performance logging with rows/second metrics
3. Add appropriate test descriptions and comments
4. Consider memory usage and test duration
5. Validate results with smaller datasets first

This benchmark suite provides comprehensive coverage of Firn's capabilities with real-world data at scale.
