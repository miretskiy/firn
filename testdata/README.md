# Test Data Directory

This directory contains various datasets used for testing and benchmarking Firn's performance.

## Available Datasets

### Small Test Datasets (Included in Repository)
- `sample.csv` - Small sample dataset for basic testing
- `fortune1000_2024.parquet` - Fortune 1000 companies dataset
- `weather_data_part_*.csv` - Weather data partitions for performance testing
- Various other CSV files for specific test cases

### Large Benchmark Dataset (Not in Repository)
- `data.parquet` - **16.9M row AMEX credit default prediction dataset (11GB)**

## Getting the AMEX Dataset

The AMEX dataset (`data.parquet`) is excluded from the repository due to its large size (11GB). To reproduce the benchmarks:

### Option 1: Download from Kaggle
1. Visit the [AMEX dataset on Kaggle](https://www.kaggle.com/datasets/jtbontinck/amex-parquet-file)
2. Download `data.parquet.zip`
3. Extract to `testdata/data.parquet`

### Option 2: Use the Schema Inspector
If you have the dataset, you can analyze it using:
```bash
cd cmd/schema_inspector
go run main.go ../../testdata/data.parquet
```

## Dataset Characteristics

The AMEX dataset contains:
- **Rows**: 16,895,213 records
- **Columns**: 193 features
- **Size**: ~11GB uncompressed Parquet
- **Domain**: Credit default prediction
- **Key Columns**: `customer_ID`, `date`, `target`, `test`, feature columns (`P_2`, `D_144`, etc.)

## Running Benchmarks

With the dataset in place, you can run the comprehensive benchmark suite:
```bash
cd benchmarks
CGO_LDFLAGS="-w" go test -v -run TestAMEXBenchmarks
```

See `benchmarks/AMEX_BENCHMARKS.md` for detailed benchmark documentation and `benchmarks/PERFORMANCE_RESULTS.md` for actual performance results.
