#!/usr/bin/env python3
"""
Generate large CSV files for testing turbo-polars performance.
Based on the script from: https://medium.com/@tubelwj/explore-the-speed-of-duckdb-with-hundred-million-row-csv-files-9fd64d4e4105

Usage:
  python3 generate_large_csv.py           # Generate 100M rows (default)
  python3 generate_large_csv.py 1000000   # Generate 1M rows
  python3 generate_large_csv.py 10000000  # Generate 10M rows
"""

import pandas as pd
from faker import Faker
import numpy as np
import random
import time
import os
import sys
import argparse

# Initialize Faker
fake = Faker()

def generate_data(n, unique_cities_list):
    """Generate n rows of synthetic weather data."""
    print(f"Generating {n:,} rows of data...")
    start_time = time.time()
    
    data = {
        'city': [fake.random_element(elements=unique_cities_list) for _ in range(n)],
        'low_temp': [fake.random_int(min=-50, max=50) for _ in range(n)],  # Temperature range: -50 to 50°C
        'high_temp': [fake.random_int(min=-50, max=50) for _ in range(n)],
        'precipitation': [round(random.uniform(0, 100), 2) for _ in range(n)],  # Precipitation range: 0 to 100 mm
        'humidity': [round(random.uniform(0, 100), 2) for _ in range(n)],  # Humidity range: 0% to 100%
        'pressure': [fake.random_int(min=950, max=1050) for _ in range(n)]  # Pressure range: 950 to 1050 hPa
    }
    
    elapsed = time.time() - start_time
    print(f"Generated {n:,} rows in {elapsed:.2f} seconds ({n/elapsed:,.0f} rows/sec)")
    
    return pd.DataFrame(data)

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Generate large CSV files for testing turbo-polars performance",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 generate_large_csv.py                    # Generate 100M rows (default)
  python3 generate_large_csv.py -n 1000000         # Generate 1M rows
  python3 generate_large_csv.py --rows 10000000    # Generate 10M rows
  python3 generate_large_csv.py -n 1000000 -o data # Generate 1M rows in 'data' directory
        """
    )
    
    parser.add_argument(
        'rows', 
        nargs='?', 
        type=int, 
        default=100_000_000,
        help='Number of rows to generate (default: 100,000,000)'
    )
    parser.add_argument(
        '-n', '--rows', 
        dest='rows_flag',
        type=int,
        help='Number of rows to generate (alternative to positional argument)'
    )
    parser.add_argument(
        '-o', '--output-dir',
        default='testdata',
        help='Output directory for CSV files (default: testdata)'
    )
    parser.add_argument(
        '-c', '--chunk-size',
        type=int,
        help='Chunk size for splitting large datasets (auto-calculated if not specified)'
    )
    
    args = parser.parse_args()
    
    # Use flag value if provided, otherwise use positional argument
    total_rows = args.rows_flag if args.rows_flag is not None else args.rows
    output_dir = args.output_dir
    
    # Auto-adjust chunk size based on total rows or use specified chunk size
    if args.chunk_size:
        chunk_size = args.chunk_size
    elif total_rows <= 1_000_000:
        chunk_size = total_rows  # Single file for small datasets
    else:
        chunk_size = min(10_000_000, total_rows // 10)  # Max 10 chunks
    
    print(f"🌦️  Generating {total_rows:,} row weather dataset for turbo-polars testing")
    print("=" * 70)
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate unique cities list once (reuse across chunks for consistency)
    print("Generating unique city names...")
    unique_cities_list = [fake.city() for _ in range(1000)]
    print(f"Generated {len(unique_cities_list)} unique cities")
    
    # Calculate chunks
    num_chunks = total_rows // chunk_size
    print(f"Will generate {num_chunks} chunks of {chunk_size:,} rows each")
    print()
    
    total_start_time = time.time()
    
    # Generate and save chunks
    for i in range(num_chunks):
        chunk_start_time = time.time()
        print(f"📊 Processing chunk {i+1}/{num_chunks}")
        
        # Generate chunk data
        chunk_df = generate_data(chunk_size, unique_cities_list)
        
        # Save to CSV
        filename = f"{output_dir}/weather_data_part_{i:02d}.csv"
        print(f"Writing to {filename}...")
        chunk_df.to_csv(filename, index=False)
        
        # Calculate file size
        file_size_mb = os.path.getsize(filename) / (1024 * 1024)
        chunk_elapsed = time.time() - chunk_start_time
        
        print(f"✅ Chunk {i+1} complete: {file_size_mb:.1f} MB in {chunk_elapsed:.2f} seconds")
        print()
    
    total_elapsed = time.time() - total_start_time
    
    print("🎉 Generation complete!")
    print(f"📈 Total rows: {total_rows:,}")
    print(f"⏱️  Total time: {total_elapsed:.2f} seconds ({total_rows/total_elapsed:,.0f} rows/sec)")
    print()
    print("📁 Files generated:")
    total_size_mb = 0
    for i in range(num_chunks):
        filename = f"{output_dir}/weather_data_part_{i:02d}.csv"
        if os.path.exists(filename):
            size_mb = os.path.getsize(filename) / (1024 * 1024)
            total_size_mb += size_mb
            print(f"   - {filename} ({size_mb:.1f} MB)")
    
    print(f"📊 Total dataset size: {total_size_mb/1024:.2f} GB")
    print("🚀 Ready for turbo-polars testing!")

if __name__ == "__main__":
    main()
